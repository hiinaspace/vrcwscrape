"""Pydantic models for VRChat API responses."""

from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class WorldSummary(BaseModel):
    """World summary from recent worlds endpoint."""

    id: str
    name: str
    author_id: str = Field(alias="authorId")
    author_name: str = Field(alias="authorName")
    image_url: str = Field(alias="imageUrl")
    thumbnail_url: str = Field(alias="thumbnailImageUrl")
    updated_at: datetime


class WorldDetail(BaseModel):
    """Complete world metadata from individual world endpoint."""

    id: str
    name: str
    description: Optional[str] = None
    author_id: str = Field(alias="authorId")
    image_url: str = Field(alias="imageUrl")
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
