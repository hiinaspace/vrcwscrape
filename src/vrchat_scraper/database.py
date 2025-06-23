"""Database connection and operations for VRChat scraper."""

from datetime import datetime
from enum import Enum
from typing import Dict, List, Optional, Tuple

import sqlalchemy
from sqlalchemy import (
    DateTime,
    Integer,
    String,
    func,
    select,
    JSON,
    Text,
    ForeignKey,
    delete,
)
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
from sqlalchemy.sql import and_, or_

from .models import FileReference, FileType


class Base(DeclarativeBase):
    """Base class for SQLAlchemy models."""

    pass


class ScrapeStatus(str, Enum):
    """Scrape status enumeration."""

    PENDING = "PENDING"
    SUCCESS = "SUCCESS"
    DELETED = "DELETED"
    ERROR = "ERROR"


class DownloadStatus(str, Enum):
    """Download status enumeration."""

    PENDING = "PENDING"
    SUCCESS = "SUCCESS"
    NOT_FOUND = "NOT_FOUND"
    ERROR = "ERROR"


class World(Base):
    """World metadata table."""

    __tablename__ = "worlds"

    world_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    world_metadata: Mapped[dict] = mapped_column(JSON)
    publish_date: Mapped[datetime | None] = mapped_column(DateTime)
    update_date: Mapped[datetime | None] = mapped_column(DateTime)
    last_scrape_time: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    scrape_status: Mapped[ScrapeStatus] = mapped_column(String(20), nullable=False)

    # Relationship to file metadata (optional, for convenience)
    file_metadata: Mapped[List["FileMetadata"]] = relationship(
        back_populates="world", cascade="all, delete-orphan"
    )


class WorldMetrics(Base):
    """World metrics time-series table."""

    __tablename__ = "world_metrics"

    world_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    scrape_time: Mapped[datetime] = mapped_column(DateTime, primary_key=True)
    favorites: Mapped[int] = mapped_column(Integer)
    heat: Mapped[int] = mapped_column(Integer)
    popularity: Mapped[int] = mapped_column(Integer)
    occupants: Mapped[int] = mapped_column(Integer)
    private_occupants: Mapped[int] = mapped_column(Integer)
    public_occupants: Mapped[int] = mapped_column(Integer)
    visits: Mapped[int] = mapped_column(Integer)


class FileMetadata(Base):
    """File metadata table."""

    __tablename__ = "file_metadata"

    file_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    world_id: Mapped[str] = mapped_column(
        String(64), ForeignKey("worlds.world_id"), nullable=False
    )
    file_type: Mapped[str] = mapped_column(String(20), nullable=False)  # FileType enum
    version_number: Mapped[int] = mapped_column(Integer, nullable=False)
    scrape_status: Mapped[str] = mapped_column(
        String(20), nullable=False
    )  # ScrapeStatus enum
    file_metadata: Mapped[Optional[dict]] = mapped_column(JSON)
    last_scrape_time: Mapped[Optional[datetime]] = mapped_column(DateTime)
    error_message: Mapped[Optional[str]] = mapped_column(Text)

    # Relationship to world (optional, for convenience)
    world: Mapped["World"] = relationship(back_populates="file_metadata")


class WorldImage(Base):
    """World images download tracking table."""

    __tablename__ = "world_images"

    file_id: Mapped[str] = mapped_column(
        String(64), ForeignKey("file_metadata.file_id"), primary_key=True
    )
    download_status: Mapped[str] = mapped_column(
        String(20), nullable=False
    )  # DownloadStatus enum
    downloaded_md5: Mapped[Optional[str]] = mapped_column(String(32))
    downloaded_size_bytes: Mapped[Optional[int]] = mapped_column(Integer)
    local_file_path: Mapped[Optional[str]] = mapped_column(Text)
    last_attempt_time: Mapped[Optional[datetime]] = mapped_column(DateTime)
    success_time: Mapped[Optional[datetime]] = mapped_column(DateTime)
    error_message: Mapped[Optional[str]] = mapped_column(Text)

    # Relationship to file metadata (optional, for convenience)
    file_metadata: Mapped["FileMetadata"] = relationship()


class Database:
    """Database layer for VRChat world metadata storage."""

    def __init__(self, connection_string: str):
        """Initialize database connection."""
        self.connection_string = connection_string
        converted_connection_string = self._convert_connection_string(connection_string)

        # SQLite doesn't support pool_size parameter
        if connection_string.startswith("sqlite"):
            self.engine = create_async_engine(converted_connection_string)
        else:
            self.engine = create_async_engine(converted_connection_string, pool_size=10)

        self.async_session = async_sessionmaker(self.engine, expire_on_commit=False)

    def _convert_connection_string(self, connection_string: str) -> str:
        """Convert MySQL connection string to async SQLAlchemy format."""
        if connection_string.startswith("mysql://"):
            return connection_string.replace("mysql://", "mysql+aiomysql://", 1)
        elif connection_string.startswith("sqlite:///"):
            return connection_string.replace("sqlite:///", "sqlite+aiosqlite:///", 1)
        elif connection_string.startswith("sqlite://"):
            return connection_string.replace("sqlite://", "sqlite+aiosqlite://", 1)
        return connection_string

    async def init_schema(self):
        """Create tables if they don't exist."""
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

    async def upsert_world(
        self, world_id: str, metadata: dict, status: str = "SUCCESS"
    ):
        """Insert or update world metadata."""
        async with self.async_session() as session:
            # Try to get existing world
            stmt = select(World).where(World.world_id == world_id)
            result = await session.execute(stmt)
            existing_world = result.scalar_one_or_none()

            if existing_world:
                # Determine if this is a discovery operation vs full scrape
                # Discovery operations have minimal metadata (like just discovered_at)
                # Full scrapes have rich metadata (name, description, etc.)
                is_discovery = self._is_discovery_metadata(metadata)

                if is_discovery and existing_world.world_metadata:
                    # Merge discovery metadata with existing detailed metadata
                    merged_metadata = existing_world.world_metadata.copy()
                    merged_metadata.update(metadata)
                    existing_world.world_metadata = merged_metadata
                else:
                    # Full scrape or no existing metadata - replace completely
                    existing_world.world_metadata = metadata

                existing_world.last_scrape_time = datetime.utcnow()
                existing_world.scrape_status = ScrapeStatus(status)
            else:
                # Create new world
                new_world = World(
                    world_id=world_id,
                    world_metadata=metadata,
                    last_scrape_time=datetime.utcnow(),
                    scrape_status=ScrapeStatus(status),
                )
                session.add(new_world)

            await session.commit()

    async def batch_upsert_worlds(self, worlds_data: List[Tuple[str, dict, str]]):
        """Batch upsert multiple worlds in a single transaction.

        Args:
            worlds_data: List of tuples (world_id, metadata, status)
        """
        async with self.async_session() as session:
            for world_id, metadata, status in worlds_data:
                # Try to get existing world
                stmt = select(World).where(World.world_id == world_id)
                result = await session.execute(stmt)
                existing_world = result.scalar_one_or_none()

                if existing_world:
                    # Same logic as upsert_world
                    is_discovery = self._is_discovery_metadata(metadata)

                    if is_discovery and existing_world.world_metadata:
                        # Merge discovery metadata with existing detailed metadata
                        merged_metadata = existing_world.world_metadata.copy()
                        merged_metadata.update(metadata)
                        existing_world.world_metadata = merged_metadata
                    else:
                        # Full scrape or no existing metadata - replace completely
                        existing_world.world_metadata = metadata

                    existing_world.last_scrape_time = datetime.utcnow()
                    existing_world.scrape_status = ScrapeStatus(status)
                else:
                    # Create new world
                    new_world = World(
                        world_id=world_id,
                        world_metadata=metadata,
                        last_scrape_time=datetime.utcnow(),
                        scrape_status=ScrapeStatus(status),
                    )
                    session.add(new_world)

            # Single commit for all worlds
            await session.commit()

    def _is_discovery_metadata(self, metadata: dict) -> bool:
        """Determine if metadata represents a discovery operation vs full scrape.

        Discovery metadata typically contains minimal info like timestamps,
        while full scrape metadata contains rich world details.
        """
        # Common discovery-only keys
        discovery_keys = {"discovered_at", "last_seen_at", "found_in_recent"}

        # Rich metadata keys that indicate a full scrape
        rich_keys = {
            "name",
            "description",
            "capacity",
            "favorites",
            "visits",
            "heat",
            "popularity",
            "tags",
            "created_at",
            "updated_at",
        }

        metadata_keys = set(metadata.keys())

        # If it only has discovery keys and no rich keys, it's a discovery
        if metadata_keys <= discovery_keys:
            return True

        # If it has any rich keys, it's a full scrape
        if metadata_keys & rich_keys:
            return False

        # If it's empty or has unknown keys, default to full scrape behavior
        return False

    async def insert_metrics(
        self, world_id: str, metrics: Dict[str, int], scrape_time: datetime
    ):
        """Insert world metrics for a specific time."""
        async with self.async_session() as session:
            new_metrics = WorldMetrics(
                world_id=world_id,
                scrape_time=scrape_time,
                favorites=metrics["favorites"],
                heat=metrics["heat"],
                popularity=metrics["popularity"],
                occupants=metrics["occupants"],
                private_occupants=metrics["private_occupants"],
                public_occupants=metrics["public_occupants"],
                visits=metrics["visits"],
            )
            session.add(new_metrics)
            await session.commit()

    def _get_random_func(self):
        """Get the appropriate random function for the database."""
        if "mysql" in self.connection_string.lower():
            return func.rand()
        else:  # SQLite
            return func.random()

    def _get_time_diff_funcs(self):
        """Get the appropriate time difference functions for the database."""
        if "mysql" in self.connection_string.lower():
            # MySQL uses TIMESTAMPDIFF
            def days_diff(date1, date2):
                return func.timestampdiff(sqlalchemy.text("DAY"), date2, date1)

            def hours_diff(date1, date2):
                return func.timestampdiff(sqlalchemy.text("HOUR"), date2, date1)

            return days_diff, hours_diff
        else:  # SQLite
            # SQLite uses julianday
            def days_diff(date1, date2):
                return func.julianday(date1) - func.julianday(date2)

            def hours_diff(date1, date2):
                return (func.julianday(date1) - func.julianday(date2)) * 24

            return days_diff, hours_diff

    async def get_worlds_to_scrape(self, limit: int = 100) -> List[str]:
        """Get world IDs that need scraping based on strategy."""
        async with self.async_session() as session:
            random_func = self._get_random_func()
            days_diff, hours_diff = self._get_time_diff_funcs()

            # First get PENDING worlds
            pending_stmt = (
                select(World.world_id)
                .where(World.scrape_status == ScrapeStatus.PENDING)
                .order_by(random_func)
                .limit(limit)
            )
            result = await session.execute(pending_stmt)
            world_ids = [row[0] for row in result.fetchall()]

            if len(world_ids) < limit:
                # Get worlds that need rescraping based on age
                remaining = limit - len(world_ids)
                now = datetime.utcnow()

                # Calculate time differences and build rescrape conditions
                age_days_expr = days_diff(now, World.publish_date)
                hours_since_scrape_expr = hours_diff(now, World.last_scrape_time)

                rescrape_conditions = or_(
                    and_(age_days_expr < 7, hours_since_scrape_expr >= 24),
                    and_(age_days_expr < 30, hours_since_scrape_expr >= 168),
                    and_(age_days_expr < 365, hours_since_scrape_expr >= 720),
                    hours_since_scrape_expr >= 8760,
                )

                rescrape_stmt = (
                    select(World.world_id)
                    .where(
                        and_(
                            World.scrape_status == ScrapeStatus.SUCCESS,
                            rescrape_conditions,
                        )
                    )
                    .order_by(random_func)
                    .limit(remaining)
                )

                result = await session.execute(rescrape_stmt)
                world_ids.extend([row[0] for row in result.fetchall()])

            return world_ids

    async def upsert_world_with_files(
        self,
        world_id: str,
        metadata: dict,
        metrics: Dict[str, int],
        discovered_files: List[FileReference],
        status: str = "SUCCESS",
    ):
        """Update world metadata and file references in a single transaction."""
        async with self.async_session() as session:
            async with session.begin():
                # Update or insert world
                world_result = await session.execute(
                    select(World).where(World.world_id == world_id)
                )
                existing_world = world_result.scalar_one_or_none()

                now = datetime.utcnow()

                if existing_world:
                    existing_world.world_metadata = metadata
                    existing_world.last_scrape_time = now
                    existing_world.scrape_status = ScrapeStatus(status)
                else:
                    new_world = World(
                        world_id=world_id,
                        world_metadata=metadata,
                        last_scrape_time=now,
                        scrape_status=ScrapeStatus(status),
                    )
                    session.add(new_world)

                # Insert metrics
                new_metrics = WorldMetrics(
                    world_id=world_id, scrape_time=now, **metrics
                )
                session.add(new_metrics)

                # Delete file_metadata rows no longer referenced
                current_file_ids = {file_ref.file_id for file_ref in discovered_files}
                if current_file_ids:
                    await session.execute(
                        delete(FileMetadata).where(
                            and_(
                                FileMetadata.world_id == world_id,
                                FileMetadata.file_id.not_in(current_file_ids),
                            )
                        )
                    )
                else:
                    # No files discovered, delete all file metadata for this world
                    await session.execute(
                        delete(FileMetadata).where(FileMetadata.world_id == world_id)
                    )

                # Upsert file_metadata rows
                for file_ref in discovered_files:
                    file_result = await session.execute(
                        select(FileMetadata).where(
                            FileMetadata.file_id == file_ref.file_id
                        )
                    )
                    existing_file = file_result.scalar_one_or_none()

                    if existing_file:
                        # Update existing file metadata if version changed
                        if existing_file.version_number != file_ref.version_number:
                            existing_file.version_number = file_ref.version_number
                            existing_file.scrape_status = ScrapeStatus.PENDING
                            existing_file.last_scrape_time = None
                            existing_file.file_metadata = None
                            existing_file.error_message = None
                    else:
                        # Create new file metadata
                        new_file = FileMetadata(
                            file_id=file_ref.file_id,
                            world_id=world_id,
                            file_type=file_ref.file_type.value,
                            version_number=file_ref.version_number,
                            scrape_status=ScrapeStatus.PENDING,
                        )
                        session.add(new_file)

                        # If it's an image file, also create world_images entry
                        if file_ref.file_type == FileType.IMAGE:
                            # Check if world_images entry already exists
                            image_result = await session.execute(
                                select(WorldImage).where(
                                    WorldImage.file_id == file_ref.file_id
                                )
                            )
                            existing_image = image_result.scalar_one_or_none()

                            if not existing_image:
                                new_image = WorldImage(
                                    file_id=file_ref.file_id,
                                    download_status=DownloadStatus.PENDING,
                                )
                                session.add(new_image)

    async def get_pending_file_metadata(
        self, limit: int = 100
    ) -> List[Tuple[str, str]]:
        """Get file IDs that need metadata scraping.

        Returns:
            List of (file_id, file_type) tuples
        """
        async with self.async_session() as session:
            stmt = (
                select(FileMetadata.file_id, FileMetadata.file_type)
                .where(FileMetadata.scrape_status == ScrapeStatus.PENDING)
                .limit(limit)
            )
            result = await session.execute(stmt)
            return [(row[0], row[1]) for row in result.fetchall()]

    async def update_file_metadata(
        self,
        file_id: str,
        metadata: dict,
        status: str = "SUCCESS",
        error_message: Optional[str] = None,
    ):
        """Update file metadata after scraping."""
        async with self.async_session() as session:
            result = await session.execute(
                select(FileMetadata).where(FileMetadata.file_id == file_id)
            )
            file_metadata = result.scalar_one_or_none()

            if file_metadata:
                file_metadata.file_metadata = metadata if status == "SUCCESS" else None
                file_metadata.scrape_status = ScrapeStatus(status)
                file_metadata.last_scrape_time = datetime.utcnow()
                file_metadata.error_message = error_message

                await session.commit()

    async def get_pending_image_downloads(
        self, limit: int = 100
    ) -> List[Tuple[str, dict, str]]:
        """Get images that need downloading.

        Returns:
            List of (file_id, file_metadata_json, file_type) tuples where file_metadata has SUCCESS status
        """
        async with self.async_session() as session:
            stmt = (
                select(
                    WorldImage.file_id,
                    FileMetadata.file_metadata,
                    FileMetadata.file_type,
                )
                .join(FileMetadata, WorldImage.file_id == FileMetadata.file_id)
                .where(
                    and_(
                        WorldImage.download_status == DownloadStatus.PENDING,
                        FileMetadata.scrape_status == ScrapeStatus.SUCCESS,
                    )
                )
                .limit(limit)
            )
            result = await session.execute(stmt)
            return [(row[0], row[1], row[2]) for row in result.fetchall()]

    async def update_image_download(
        self,
        file_id: str,
        status: str,
        local_file_path: Optional[str] = None,
        downloaded_md5: Optional[str] = None,
        downloaded_size_bytes: Optional[int] = None,
        error_message: Optional[str] = None,
    ):
        """Update image download status."""
        async with self.async_session() as session:
            result = await session.execute(
                select(WorldImage).where(WorldImage.file_id == file_id)
            )
            world_image = result.scalar_one_or_none()

            if world_image:
                world_image.download_status = DownloadStatus(status)
                world_image.last_attempt_time = datetime.utcnow()
                world_image.local_file_path = local_file_path
                world_image.downloaded_md5 = downloaded_md5
                world_image.downloaded_size_bytes = downloaded_size_bytes
                world_image.error_message = error_message

                if status == "SUCCESS":
                    world_image.success_time = datetime.utcnow()

                await session.commit()

    async def get_queue_depths(self) -> Dict[str, int]:
        """Get count of pending work for observability metrics.

        Returns:
            Dictionary with counts of pending worlds, files, and images
        """
        async with self.async_session() as session:
            # Count pending worlds
            pending_worlds_stmt = select(func.count(World.world_id)).where(
                World.scrape_status == ScrapeStatus.PENDING
            )
            pending_worlds_result = await session.execute(pending_worlds_stmt)
            pending_worlds = pending_worlds_result.scalar() or 0

            # Count pending file metadata
            pending_files_stmt = select(func.count(FileMetadata.file_id)).where(
                FileMetadata.scrape_status == ScrapeStatus.PENDING
            )
            pending_files_result = await session.execute(pending_files_stmt)
            pending_files = pending_files_result.scalar() or 0

            # Count pending image downloads
            pending_images_stmt = select(func.count(WorldImage.file_id)).where(
                and_(
                    WorldImage.download_status == DownloadStatus.PENDING,
                    # Only count images where file metadata is ready
                    WorldImage.file_id.in_(
                        select(FileMetadata.file_id).where(
                            FileMetadata.scrape_status == ScrapeStatus.SUCCESS
                        )
                    ),
                )
            )
            pending_images_result = await session.execute(pending_images_stmt)
            pending_images = pending_images_result.scalar() or 0

            return {
                "pending_worlds": pending_worlds,
                "pending_file_metadata": pending_files,
                "pending_image_downloads": pending_images,
            }
