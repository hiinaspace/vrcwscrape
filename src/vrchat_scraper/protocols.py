"""Protocol definitions for VRChat scraper abstractions."""

from typing import List, Protocol

from .models import WorldDetail, WorldSummary


class VRChatAPIClient(Protocol):
    """Protocol for VRChat API client implementations."""

    async def get_recent_worlds(self) -> List[WorldSummary]:
        """Fetch recently updated worlds from VRChat API."""
        ...

    async def get_world_details(self, world_id: str) -> WorldDetail:
        """Fetch complete metadata for a single world."""
        ...


class ImageDownloader(Protocol):
    """Protocol for image downloader implementations."""

    async def download_image(self, url: str, world_id: str) -> bool:
        """Download image for a world.

        Returns True if download was successful, False otherwise.
        """
        ...
