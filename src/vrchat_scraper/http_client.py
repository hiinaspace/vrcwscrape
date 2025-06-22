"""HTTP-based implementations of VRChat API client and image downloader."""

import logging
from pathlib import Path
from typing import List

import httpx

from .models import WorldDetail, WorldSummary

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

    async def close(self):
        """Clean up HTTP client resources."""
        await self.client.aclose()


class FileImageDownloader:
    """File system-based image downloader."""

    def __init__(self, storage_path: str):
        """Initialize with image storage directory."""
        self.storage_path = Path(storage_path)
        self.client = httpx.AsyncClient(timeout=30.0)

    async def download_image(self, url: str, world_id: str) -> bool:
        """Download image for a world.

        Returns True if download was successful, False otherwise.
        Creates a 0-byte file for 404 responses to indicate intentional missing image.
        """
        image_path = self._get_image_path(world_id)
        image_path.parent.mkdir(parents=True, exist_ok=True)

        try:
            response = await self.client.get(url)

            if response.status_code == 404:
                # Create 0-byte file to indicate intentional missing image
                image_path.touch()
                return True

            response.raise_for_status()
            image_path.write_bytes(response.content)
            return True

        except (httpx.TimeoutException, httpx.ConnectTimeout, httpx.ReadTimeout) as e:
            logger.warning(f"Timeout downloading image for {world_id}: {e}")
            return False
        except Exception as e:
            logger.warning(f"Failed to download image for {world_id}: {e}")
            return False

    def _get_image_path(self, world_id: str) -> Path:
        """Generate hierarchical path for image storage."""
        # world_id format: wrld_xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
        uuid_part = world_id[5:]  # Remove 'wrld_' prefix
        return self.storage_path / uuid_part[0:2] / uuid_part[2:4] / f"{world_id}.png"

    def image_exists(self, world_id: str) -> bool:
        """Check if image already exists for a world."""
        return self._get_image_path(world_id).exists()

    async def close(self):
        """Clean up HTTP client resources."""
        await self.client.aclose()
