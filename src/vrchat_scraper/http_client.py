"""HTTP-based implementations of VRChat API client and image downloader."""

import base64
import hashlib
import logging
from pathlib import Path
from typing import Dict, List, Tuple, Any

import httpx

from vrchat_scraper.protocols import VRChatAPIClient

from .models import WorldSummary

logger = logging.getLogger(__name__)

USER_AGENT = "vrcwscrape/0.1.0 (hiina@hiina.space)"


class AuthenticationError(Exception):
    """Raised when VRChat authentication fails."""

    pass


class HTTPVRChatAPIClient(VRChatAPIClient):
    """HTTP implementation of VRChat API client."""

    def __init__(self, auth_cookie: str, timeout: float = 30.0):
        """Initialize with VRChat auth cookie."""
        self.client = httpx.AsyncClient(
            timeout=timeout,
            limits=httpx.Limits(max_connections=10),
            headers={"Cookie": f"auth={auth_cookie}", "User-Agent": USER_AGENT},
        )

    async def get_recent_worlds(self) -> List[WorldSummary]:
        """Fetch recently updated worlds from VRChat API."""
        response = await self.client.get(
            "https://api.vrchat.cloud/api/1/worlds",
            params={"sort": "updated", "n": 100},
        )

        if response.status_code == 401:
            raise AuthenticationError("VRChat authentication failed")

        response.raise_for_status()
        return [WorldSummary(**w) for w in response.json()]

    async def get_world_details(self, world_id: str) -> Dict[str, Any]:
        """Fetch complete metadata for a single world as raw JSON."""
        response = await self.client.get(
            f"https://api.vrchat.cloud/api/1/worlds/{world_id}"
        )

        if response.status_code == 401:
            raise AuthenticationError("VRChat authentication failed")

        response.raise_for_status()
        return response.json()

    async def get_file_metadata(self, file_id: str) -> Dict[str, Any]:
        """Fetch metadata for a VRChat file by ID as raw JSON."""
        response = await self.client.get(
            f"https://api.vrchat.cloud/api/1/file/{file_id}"
        )

        if response.status_code == 401:
            raise AuthenticationError("VRChat authentication failed")

        response.raise_for_status()
        return response.json()

    async def close(self):
        """Clean up HTTP client resources."""
        await self.client.aclose()


class FileImageDownloader:
    """Content-addressed image downloader with dual hash verification."""

    def __init__(self, auth_cookie: str, storage_path: str, timeout: float = 30.0):
        """Initialize with image storage directory."""
        self.storage_path = Path(storage_path)
        # the initial request still requires vrchat auth, even if the download is direct from s3.
        self.client = httpx.AsyncClient(
            timeout=timeout,
            limits=httpx.Limits(max_connections=10),
            headers={"Cookie": f"auth={auth_cookie}", "User-Agent": USER_AGENT},
        )

    async def download_image(
        self, file_id: str, version: int, download_url: str, expected_md5: str, expected_size: int
    ) -> Tuple[bool, str, str]:
        """Download and verify an image file using content-addressed storage.

        Args:
            file_id: VRChat file ID for tracking purposes
            version: File version number
            download_url: Direct download URL from VRChat CDN
            expected_md5: MD5 hash from VRChat for verification
            expected_size: Expected file size in bytes

        Returns:
            Tuple of (success, sha256_hash, error_message)
        """
        try:
            # vrchat file links redirect to s3 signed urls.
            response = await self.client.get(download_url, follow_redirects=True)

            if response.status_code == 404:
                # File no longer exists on VRChat CDN
                return False, "", "Image not found on VRChat CDN (404)"

            response.raise_for_status()

            # Get downloaded content
            content = response.content
            actual_size = len(content)

            # Verify size
            if actual_size != expected_size:
                return (
                    False,
                    "",
                    f"Size mismatch: expected {expected_size}, got {actual_size}",
                )

            # Verify MD5 hash (for VRChat API compatibility)
            if not self._verify_md5(content, expected_md5):
                return (
                    False,
                    "",
                    "MD5 hash verification failed",
                )

            # Compute SHA256 for content addressing
            sha256_hash = hashlib.sha256(content).hexdigest()
            image_path = self._get_content_addressed_path(sha256_hash)

            # Check if we already have this content
            if image_path.exists():
                # Content already exists, verify it matches
                existing_content = image_path.read_bytes()
                existing_sha256 = hashlib.sha256(existing_content).hexdigest()
                if existing_sha256 == sha256_hash:
                    logger.debug(
                        f"Image content already exists for {file_id} v{version} (SHA256: {sha256_hash})"
                    )
                    return True, sha256_hash, ""
                else:
                    logger.warning(
                        f"SHA256 collision detected for {file_id} v{version}: existing file has different content"
                    )
                    return False, "", "SHA256 collision detected"

            # Create directory and write content
            image_path.parent.mkdir(parents=True, exist_ok=True)
            image_path.write_bytes(content)
            
            logger.debug(
                f"Successfully downloaded and verified image {file_id} v{version} ({actual_size} bytes, SHA256: {sha256_hash})"
            )

            return True, sha256_hash, ""

        except (httpx.TimeoutException, httpx.ConnectTimeout, httpx.ReadTimeout) as e:
            error_msg = f"Timeout downloading image {file_id} v{version}: {e}"
            logger.warning(error_msg)
            return False, "", error_msg

        except httpx.HTTPStatusError as e:
            error_msg = f"HTTP error downloading image {file_id} v{version}: {e}"
            logger.warning(error_msg)
            return False, "", error_msg

        except Exception as e:
            error_msg = f"Unexpected error downloading image {file_id} v{version}: {e}"
            logger.error(error_msg)
            return False, "", error_msg

    def _get_content_addressed_path(self, sha256_hash: str) -> Path:
        """Generate hierarchical path for content-addressed storage using SHA256."""
        return self.storage_path / sha256_hash[0:2] / sha256_hash[2:4] / f"{sha256_hash}.png"

    def _verify_md5(self, content: bytes, expected_md5_b64: str) -> bool:
        """Verify content against VRChat's base64-encoded MD5 hash."""
        try:
            # Calculate MD5 of downloaded content
            actual_md5_hex = hashlib.md5(content).hexdigest()

            # Decode VRChat's base64 MD5 to hex
            expected_md5_bytes = base64.b64decode(expected_md5_b64)
            expected_md5_hex = expected_md5_bytes.hex()

            return actual_md5_hex == expected_md5_hex

        except Exception as e:
            logger.warning(f"MD5 verification failed due to encoding error: {e}")
            return False

    def image_exists(self, sha256_hash: str) -> bool:
        """Check if image already exists for a SHA256 hash."""
        return self._get_content_addressed_path(sha256_hash).exists()

    async def close(self):
        """Clean up HTTP client resources."""
        await self.client.aclose()
