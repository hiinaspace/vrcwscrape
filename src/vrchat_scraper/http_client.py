"""HTTP-based implementations of VRChat API client and image downloader."""

import base64
import hashlib
import logging
from pathlib import Path
from typing import List, Tuple

import httpx

from .models import WorldDetail, WorldSummary, FileMetadata

logger = logging.getLogger(__name__)


class AuthenticationError(Exception):
    """Raised when VRChat authentication fails."""

    pass


class HTTPVRChatAPIClient:
    """HTTP implementation of VRChat API client."""

    def __init__(self, auth_cookie: str, timeout: float = 30.0):
        """Initialize with VRChat auth cookie."""
        self.client = httpx.AsyncClient(
            timeout=timeout,
            limits=httpx.Limits(max_connections=10),
            headers={"Cookie": f"auth={auth_cookie}"},
        )

    async def get_recent_worlds(self) -> List[WorldSummary]:
        """Fetch recently updated worlds from VRChat API."""
        response = await self.client.get(
            "https://api.vrchat.cloud/api/1/worlds",
            params={"sort": "updated", "n": 1000},
        )

        if response.status_code == 401:
            raise AuthenticationError("VRChat authentication failed")

        response.raise_for_status()
        return [WorldSummary(**w) for w in response.json()]

    async def get_world_details(self, world_id: str) -> WorldDetail:
        """Fetch complete metadata for a single world."""
        response = await self.client.get(
            f"https://api.vrchat.cloud/api/1/worlds/{world_id}"
        )

        if response.status_code == 401:
            raise AuthenticationError("VRChat authentication failed")

        response.raise_for_status()
        return WorldDetail(**response.json())

    async def get_file_metadata(self, file_id: str) -> FileMetadata:
        """Fetch metadata for a VRChat file by ID."""
        response = await self.client.get(
            f"https://api.vrchat.cloud/api/1/file/{file_id}"
        )

        if response.status_code == 401:
            raise AuthenticationError("VRChat authentication failed")

        response.raise_for_status()
        return FileMetadata(**response.json())

    async def close(self):
        """Clean up HTTP client resources."""
        await self.client.aclose()


class FileImageDownloader:
    """File system-based image downloader with MD5 verification."""

    def __init__(self, storage_path: str):
        """Initialize with image storage directory."""
        self.storage_path = Path(storage_path)
        self.client = httpx.AsyncClient(timeout=30.0)

    async def download_image(self, file_id: str, download_url: str, expected_md5: str) -> Tuple[bool, str, int, str]:
        """Download and verify an image file.

        Args:
            file_id: VRChat file ID for storage path generation
            download_url: Direct download URL from VRChat CDN
            expected_md5: Base64-encoded MD5 hash from VRChat for verification

        Returns:
            Tuple of (success, local_file_path, actual_size_bytes, error_message)
        """
        image_path = self._get_image_path(file_id)
        image_path.parent.mkdir(parents=True, exist_ok=True)

        try:
            response = await self.client.get(download_url)

            if response.status_code == 404:
                # File no longer exists on VRChat CDN
                return False, str(image_path), 0, "Image not found on VRChat CDN (404)"

            response.raise_for_status()
            
            # Get downloaded content
            content = response.content
            actual_size = len(content)
            
            # Verify MD5 hash
            if not self._verify_md5(content, expected_md5):
                return False, str(image_path), actual_size, "MD5 hash verification failed"
            
            # Write verified content to disk
            image_path.write_bytes(content)
            logger.debug(f"Successfully downloaded and verified image {file_id} ({actual_size} bytes)")
            
            return True, str(image_path), actual_size, ""

        except (httpx.TimeoutException, httpx.ConnectTimeout, httpx.ReadTimeout) as e:
            error_msg = f"Timeout downloading image {file_id}: {e}"
            logger.warning(error_msg)
            return False, str(image_path), 0, error_msg
            
        except httpx.HTTPStatusError as e:
            error_msg = f"HTTP error downloading image {file_id}: {e}"
            logger.warning(error_msg)
            return False, str(image_path), 0, error_msg
            
        except Exception as e:
            error_msg = f"Unexpected error downloading image {file_id}: {e}"
            logger.error(error_msg)
            return False, str(image_path), 0, error_msg

    def _get_image_path(self, file_id: str) -> Path:
        """Generate hierarchical path for image storage using file ID."""
        # file_id format: file_xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
        uuid_part = file_id[5:]  # Remove 'file_' prefix
        return self.storage_path / uuid_part[0:2] / uuid_part[2:4] / f"{file_id}.png"

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

    def image_exists(self, file_id: str) -> bool:
        """Check if image already exists for a file ID."""
        return self._get_image_path(file_id).exists()

    async def close(self):
        """Clean up HTTP client resources."""
        await self.client.aclose()
