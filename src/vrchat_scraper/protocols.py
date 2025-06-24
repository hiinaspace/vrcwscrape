"""Protocol definitions for VRChat scraper abstractions."""

from typing import Dict, List, Protocol, Tuple, Any

from .models import WorldSummary, ImageDownloadResult


class VRChatAPIClient(Protocol):
    """Protocol for VRChat API client implementations."""

    async def get_recent_worlds(self) -> List[WorldSummary]:
        """Fetch recently updated worlds from VRChat API."""
        ...

    async def get_world_details(self, world_id: str) -> Dict[str, Any]:
        """Fetch complete metadata for a single world as raw JSON."""
        ...

    async def get_file_metadata(self, file_id: str) -> Dict[str, Any]:
        """Fetch metadata for a VRChat file by ID as raw JSON."""
        ...


class ImageDownloader(Protocol):
    """Protocol for image downloader implementations."""

    async def download_image(
        self, file_id: str, version: int, download_url: str, expected_md5: str, expected_size: int
    ) -> ImageDownloadResult:
        """Download and verify an image file using content-addressed storage.

        Args:
            file_id: VRChat file ID for tracking purposes
            version: File version number
            download_url: Direct download URL from VRChat CDN
            expected_md5: MD5 hash from VRChat for verification
            expected_size: Expected file size in bytes

        Returns:
            ImageDownloadResult with success flag, SHA256 hash, and error message
        """
        ...
