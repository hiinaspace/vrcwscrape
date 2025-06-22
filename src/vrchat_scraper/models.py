"""Pydantic models for VRChat API responses and internal data structures."""

import re
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, computed_field


class FileType(str, Enum):
    """Types of files discovered from world metadata."""

    IMAGE = "IMAGE"
    UNITY_PACKAGE = "UNITY_PACKAGE"


class FileReference(BaseModel):
    """A file reference discovered from world metadata URLs."""

    file_id: str
    file_type: FileType
    version_number: int
    original_url: str


class UnityPackageBasic(BaseModel):
    """Basic unity package info from recent worlds endpoint."""

    platform: str
    unity_version: str = Field(alias="unityVersion")


class UnityPackageDetailed(BaseModel):
    """Detailed unity package info from world detail endpoint."""

    id: str
    platform: str
    unity_version: str = Field(alias="unityVersion")
    asset_url: str = Field(alias="assetUrl")
    asset_version: int = Field(alias="assetVersion")
    created_at: datetime


class WorldSummary(BaseModel):
    """World summary from recent worlds endpoint."""

    id: str
    name: str
    author_id: str = Field(alias="authorId")
    author_name: str = Field(alias="authorName")
    image_url: str = Field(alias="imageUrl")
    thumbnail_url: str = Field(alias="thumbnailImageUrl")
    updated_at: datetime
    unity_packages: List[UnityPackageBasic] = Field(
        alias="unityPackages", default_factory=list
    )

    @computed_field
    @property
    def discovered_files(self) -> List[FileReference]:
        """Extract file references from URLs in this world summary."""
        files = []

        # Parse image URLs
        for url_field, file_type in [
            (self.image_url, FileType.IMAGE),
            (self.thumbnail_url, FileType.IMAGE),
        ]:
            file_ref = _parse_file_url(url_field, file_type)
            if file_ref:
                files.append(file_ref)

        return files


class WorldDetail(BaseModel):
    """Complete world metadata from individual world endpoint."""

    id: str
    name: str
    description: Optional[str] = None
    author_id: str = Field(alias="authorId")
    image_url: str = Field(alias="imageUrl")
    thumbnail_url: str = Field(alias="thumbnailImageUrl")
    capacity: int
    favorites: int
    heat: int
    popularity: int
    occupants: int
    private_occupants: int = Field(alias="privateOccupants", default=0)
    public_occupants: int = Field(alias="publicOccupants", default=0)
    visits: int
    created_at: datetime
    updated_at: datetime
    tags: List[str]
    unity_packages: List[UnityPackageDetailed] = Field(
        alias="unityPackages", default_factory=list
    )

    @computed_field
    @property
    def discovered_files(self) -> List[FileReference]:
        """Extract file references from URLs in this world detail."""
        files = []

        # Parse image URLs
        for url_field, file_type in [
            (self.image_url, FileType.IMAGE),
            (self.thumbnail_url, FileType.IMAGE),
        ]:
            file_ref = _parse_file_url(url_field, file_type)
            if file_ref:
                files.append(file_ref)

        # Parse unity package URLs
        for unity_package in self.unity_packages:
            file_ref = _parse_file_url(unity_package.asset_url, FileType.UNITY_PACKAGE)
            if file_ref:
                files.append(file_ref)

        return files

    def extract_metrics(self) -> Dict[str, int]:
        """Extract ephemeral metrics for separate storage."""
        return {
            "favorites": self.favorites,
            "heat": self.heat,
            "popularity": self.popularity,
            "occupants": self.occupants,
            "private_occupants": self.private_occupants,
            "public_occupants": self.public_occupants,
            "visits": self.visits,
        }

    def stable_metadata(self) -> Dict[str, Any]:
        """Return metadata without ephemeral fields."""
        data = self.model_dump(mode="json")  # Serialize datetimes as ISO strings
        ephemeral_fields = [
            "favorites",
            "heat",
            "popularity",
            "occupants",
            "private_occupants",
            "public_occupants",
            "visits",
        ]
        for field in ephemeral_fields:
            data.pop(field, None)
        return data


class FileMetadataVersion(BaseModel):
    """A single version of a file from VRChat file metadata API."""

    version: int
    status: str
    created_at: datetime
    deleted: Optional[bool] = None
    file: Optional["FileInfo"] = None


class FileInfo(BaseModel):
    """File download information from VRChat file metadata API."""

    md5: str  # Base64 encoded MD5 hash
    size_in_bytes: int = Field(alias="sizeInBytes")
    url: str
    file_name: str = Field(alias="fileName")


class FileMetadata(BaseModel):
    """Complete file metadata response from VRChat file API."""

    id: str
    name: str
    extension: str
    mime_type: str = Field(alias="mimeType")
    owner_id: str = Field(alias="ownerId")
    versions: List[FileMetadataVersion]

    def get_latest_version(self) -> Optional[FileMetadataVersion]:
        """Get the latest non-deleted version of this file."""
        # Sort by version descending and find first non-deleted version with file info
        for version in sorted(self.versions, key=lambda v: v.version, reverse=True):
            if not getattr(version, "deleted", False) and version.file:
                return version
        return None


def _parse_file_url(url: str, file_type: FileType) -> Optional[FileReference]:
    """Parse a VRChat file URL to extract file ID and version.

    Examples:
    - https://api.vrchat.cloud/api/1/file/file_447b6078-e5fb-488c-bff5-432d4631f6cf/2/file
    - https://api.vrchat.cloud/api/1/image/file_447b6078-e5fb-488c-bff5-432d4631f6cf/2/256
    """
    # Pattern to match VRChat file URLs with file ID and version
    pattern = r"/file_([\w-]+)/(\d+)/"
    match = re.search(pattern, url)

    if not match:
        return None

    file_id = f"file_{match.group(1)}"
    version_number = int(match.group(2))

    return FileReference(
        file_id=file_id,
        file_type=file_type,
        version_number=version_number,
        original_url=url,
    )
