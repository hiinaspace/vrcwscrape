"""Protocol definitions for VRChat scraper abstractions."""

from typing import Dict, List, Protocol, Tuple, Any

from .models import WorldSummary, FileMetadata


class VRChatAPIClient(Protocol):
    """Protocol for VRChat API client implementations."""

    async def get_recent_worlds(self) -> List[WorldSummary]:
        """Fetch recently updated worlds from VRChat API."""
        ...

    async def get_world_details(self, world_id: str) -> Dict[str, Any]:
        """Fetch complete metadata for a single world as raw JSON."""
        ...

    async def get_file_metadata(self, file_id: str) -> FileMetadata:
        """Fetch metadata for a VRChat file by ID."""
        ...


class ImageDownloader(Protocol):
    """Protocol for image downloader implementations."""

    async def download_image(
        self, file_id: str, download_url: str, expected_md5: str
    ) -> Tuple[bool, str, int, str]:
        """Download and verify an image file.

        Args:
            file_id: VRChat file ID for storage path generation
            download_url: Direct download URL from VRChat CDN
            expected_md5: Base64-encoded MD5 hash from VRChat for verification

        Returns:
            Tuple of (success, local_file_path, actual_size_bytes, error_message)
        """
        ...
