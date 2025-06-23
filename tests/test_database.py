"""Tests for database operations."""

import pytest
import pytest_asyncio
from datetime import datetime, timedelta
from sqlalchemy import select

from vrchat_scraper.database import (
    Database,
    World,
    WorldMetrics,
    FileMetadata,
    WorldImage,
    ScrapeStatus,
    DownloadStatus,
)
from vrchat_scraper.models import FileReference, FileType


@pytest_asyncio.fixture
async def test_db():
    """Create test database using in-memory SQLite."""
    db = Database("sqlite:///:memory:")
    await db.init_schema()
    return db


@pytest.mark.asyncio
async def test_init_schema(test_db):
    """Test schema initialization creates tables."""
    # Schema already initialized in fixture
    async with test_db.async_session() as session:
        # Test we can query empty tables without error
        result = await session.execute(select(World))
        assert result.fetchall() == []

        result = await session.execute(select(WorldMetrics))
        assert result.fetchall() == []


@pytest.mark.asyncio
async def test_upsert_world_insert(test_db):
    """Test inserting a new world."""
    world_id = "wrld_test_123"
    metadata = {"name": "Test World", "description": "A test world"}

    await test_db.upsert_world(world_id, metadata, "PENDING")

    async with test_db.async_session() as session:
        result = await session.execute(select(World).where(World.world_id == world_id))
        world = result.scalar_one()

        assert world.world_id == world_id
        assert world.world_metadata == metadata
        assert world.scrape_status == ScrapeStatus.PENDING
        assert world.last_scrape_time is not None


@pytest.mark.asyncio
async def test_upsert_world_update(test_db):
    """Test updating an existing world."""
    world_id = "wrld_test_123"
    initial_metadata = {"name": "Test World", "description": "A test world"}
    updated_metadata = {"name": "Updated World", "description": "An updated test world"}

    # Insert initial world
    await test_db.upsert_world(world_id, initial_metadata, "PENDING")

    # Update the world
    await test_db.upsert_world(world_id, updated_metadata, "SUCCESS")

    async with test_db.async_session() as session:
        result = await session.execute(select(World).where(World.world_id == world_id))
        world = result.scalar_one()

        assert world.world_id == world_id
        assert world.world_metadata == updated_metadata
        assert world.scrape_status == ScrapeStatus.SUCCESS


@pytest.mark.asyncio
async def test_world_state_transitions(test_db):
    """Test world moves through states correctly."""
    world_id = "wrld_test_123"
    metadata = {"name": "Test World"}

    # Start as PENDING
    await test_db.upsert_world(world_id, metadata, "PENDING")

    async with test_db.async_session() as session:
        result = await session.execute(select(World).where(World.world_id == world_id))
        world = result.scalar_one()
        assert world.scrape_status == ScrapeStatus.PENDING

    # Move to SUCCESS
    await test_db.upsert_world(world_id, metadata, "SUCCESS")

    async with test_db.async_session() as session:
        result = await session.execute(select(World).where(World.world_id == world_id))
        world = result.scalar_one()
        assert world.scrape_status == ScrapeStatus.SUCCESS

    # Move to DELETED
    await test_db.upsert_world(world_id, metadata, "DELETED")

    async with test_db.async_session() as session:
        result = await session.execute(select(World).where(World.world_id == world_id))
        world = result.scalar_one()
        assert world.scrape_status == ScrapeStatus.DELETED


@pytest.mark.asyncio
async def test_insert_metrics(test_db):
    """Test inserting world metrics."""
    world_id = "wrld_test_123"
    scrape_time = datetime.utcnow()
    metrics = {
        "favorites": 100,
        "heat": 5,
        "popularity": 8,
        "occupants": 12,
        "private_occupants": 3,
        "public_occupants": 9,
        "visits": 1500,
    }

    await test_db.insert_metrics(world_id, metrics, scrape_time)

    async with test_db.async_session() as session:
        result = await session.execute(
            select(WorldMetrics).where(
                WorldMetrics.world_id == world_id,
                WorldMetrics.scrape_time == scrape_time,
            )
        )
        saved_metrics = result.scalar_one()

        assert saved_metrics.world_id == world_id
        assert saved_metrics.scrape_time == scrape_time
        assert saved_metrics.favorites == 100
        assert saved_metrics.heat == 5
        assert saved_metrics.popularity == 8
        assert saved_metrics.occupants == 12
        assert saved_metrics.private_occupants == 3
        assert saved_metrics.public_occupants == 9
        assert saved_metrics.visits == 1500


@pytest.mark.asyncio
async def test_metrics_append_only(test_db):
    """Test metrics are appended, not updated."""
    world_id = "wrld_test_123"
    time1 = datetime.utcnow()
    time2 = time1 + timedelta(hours=1)

    metrics1 = {
        "favorites": 100,
        "heat": 5,
        "popularity": 8,
        "occupants": 12,
        "private_occupants": 3,
        "public_occupants": 9,
        "visits": 1500,
    }
    metrics2 = {
        "favorites": 120,
        "heat": 6,
        "popularity": 9,
        "occupants": 15,
        "private_occupants": 4,
        "public_occupants": 11,
        "visits": 1600,
    }

    await test_db.insert_metrics(world_id, metrics1, time1)
    await test_db.insert_metrics(world_id, metrics2, time2)

    async with test_db.async_session() as session:
        result = await session.execute(
            select(WorldMetrics).where(WorldMetrics.world_id == world_id)
        )
        all_metrics = result.fetchall()

        assert len(all_metrics) == 2

        # Verify both records exist with different values
        metrics_by_time = {m[0].scrape_time: m[0] for m in all_metrics}
        assert metrics_by_time[time1].favorites == 100
        assert metrics_by_time[time2].favorites == 120


@pytest.mark.asyncio
async def test_get_worlds_to_scrape_pending(test_db):
    """Test getting pending worlds for scraping."""
    # Insert worlds in different states
    await test_db.upsert_world("wrld_pending_1", {"name": "Pending 1"}, "PENDING")
    await test_db.upsert_world("wrld_pending_2", {"name": "Pending 2"}, "PENDING")
    await test_db.upsert_world("wrld_success_1", {"name": "Success 1"}, "SUCCESS")

    world_ids = await test_db.get_worlds_to_scrape(limit=10)

    # Should return only pending worlds
    assert len(world_ids) == 2
    assert "wrld_pending_1" in world_ids
    assert "wrld_pending_2" in world_ids
    assert "wrld_success_1" not in world_ids


@pytest.mark.asyncio
async def test_get_worlds_to_scrape_rescrape_logic(test_db):
    """Test rescrape scheduling logic."""
    now = datetime.utcnow()
    old_time = now - timedelta(
        hours=25
    )  # Should trigger rescrape (>24h for new worlds)

    # Create a world that was scraped 25 hours ago (needs rescrape)
    world_id = "wrld_old_success"
    await test_db.upsert_world(world_id, {"name": "Old Success"}, "SUCCESS")

    # Manually update the last_scrape_time to be old
    async with test_db.async_session() as session:
        result = await session.execute(select(World).where(World.world_id == world_id))
        world = result.scalar_one()
        world.last_scrape_time = old_time
        world.publish_date = now - timedelta(days=1)  # New world (< 7 days old)
        await session.commit()

    world_ids = await test_db.get_worlds_to_scrape(limit=10)

    # Should include the old world for rescraping
    assert world_id in world_ids


@pytest.mark.asyncio
async def test_get_worlds_to_scrape_limit(test_db):
    """Test scrape limit is respected."""
    # Insert more worlds than the limit
    for i in range(5):
        await test_db.upsert_world(
            f"wrld_pending_{i}", {"name": f"Pending {i}"}, "PENDING"
        )

    world_ids = await test_db.get_worlds_to_scrape(limit=3)

    assert len(world_ids) == 3


@pytest.mark.asyncio
async def test_upsert_world_with_files(test_db):
    """Test upserting world with file metadata in a single transaction."""
    world_id = "wrld_test_123"
    metadata = {"name": "Test World", "description": "A test world"}
    metrics = {
        "favorites": 100,
        "heat": 5,
        "popularity": 8,
        "occupants": 12,
        "private_occupants": 3,
        "public_occupants": 9,
        "visits": 1500,
    }

    # Create some file references
    discovered_files = [
        FileReference(
            file_id="file_image_123",
            file_type=FileType.IMAGE,
            version_number=1,
            original_url="https://api.vrchat.cloud/api/1/file/file_image_123/1/file",
        ),
        FileReference(
            file_id="file_unity_456",
            file_type=FileType.UNITY_PACKAGE,
            version_number=5,
            original_url="https://api.vrchat.cloud/api/1/file/file_unity_456/5/file",
        ),
    ]

    await test_db.upsert_world_with_files(world_id, metadata, metrics, discovered_files)

    # Verify world was created
    async with test_db.async_session() as session:
        world_result = await session.execute(
            select(World).where(World.world_id == world_id)
        )
        world = world_result.scalar_one()
        assert world.world_metadata == metadata
        assert world.scrape_status == ScrapeStatus.SUCCESS

        # Verify metrics were created
        metrics_result = await session.execute(
            select(WorldMetrics).where(WorldMetrics.world_id == world_id)
        )
        world_metrics = metrics_result.scalar_one()
        assert world_metrics.favorites == 100
        assert world_metrics.visits == 1500

        # Verify file metadata was created
        file_result = await session.execute(
            select(FileMetadata).where(FileMetadata.world_id == world_id)
        )
        files = file_result.fetchall()
        assert len(files) == 2

        # Check image file
        image_file = next(f[0] for f in files if f[0].file_type == FileType.IMAGE.value)
        assert image_file.file_id == "file_image_123"
        assert image_file.version_number == 1
        assert image_file.scrape_status == ScrapeStatus.PENDING

        # Check unity package file
        unity_file = next(
            f[0] for f in files if f[0].file_type == FileType.UNITY_PACKAGE.value
        )
        assert unity_file.file_id == "file_unity_456"
        assert unity_file.version_number == 5

        # Verify world_images entry was created for image file
        image_result = await session.execute(
            select(WorldImage).where(WorldImage.file_id == "file_image_123")
        )
        world_image = image_result.scalar_one()
        assert world_image.download_status == DownloadStatus.PENDING


@pytest.mark.asyncio
async def test_upsert_world_with_files_update_removes_old_files(test_db):
    """Test that updating world with files removes old file references."""
    world_id = "wrld_test_123"
    metadata = {"name": "Test World"}
    metrics = {
        "favorites": 100,
        "heat": 5,
        "popularity": 8,
        "occupants": 12,
        "private_occupants": 3,
        "public_occupants": 9,
        "visits": 1500,
    }

    # First insert with 2 files
    initial_files = [
        FileReference(
            file_id="file_old_1",
            file_type=FileType.IMAGE,
            version_number=1,
            original_url="url1",
        ),
        FileReference(
            file_id="file_old_2",
            file_type=FileType.UNITY_PACKAGE,
            version_number=1,
            original_url="url2",
        ),
    ]
    await test_db.upsert_world_with_files(world_id, metadata, metrics, initial_files)

    # Update with only 1 file (different file)
    updated_files = [
        FileReference(
            file_id="file_new_1",
            file_type=FileType.IMAGE,
            version_number=2,
            original_url="url3",
        ),
    ]
    await test_db.upsert_world_with_files(world_id, metadata, metrics, updated_files)

    # Verify old files were removed and new file was added
    async with test_db.async_session() as session:
        file_result = await session.execute(
            select(FileMetadata).where(FileMetadata.world_id == world_id)
        )
        files = file_result.fetchall()
        assert len(files) == 1
        assert files[0][0].file_id == "file_new_1"


@pytest.mark.asyncio
async def test_get_pending_file_metadata(test_db):
    """Test getting pending file metadata."""
    world_id = "wrld_test_123"
    metadata = {"name": "Test World"}
    metrics = {
        "favorites": 100,
        "heat": 5,
        "popularity": 8,
        "occupants": 12,
        "private_occupants": 3,
        "public_occupants": 9,
        "visits": 1500,
    }

    discovered_files = [
        FileReference(
            file_id="file_pending_1",
            file_type=FileType.IMAGE,
            version_number=1,
            original_url="url1",
        ),
        FileReference(
            file_id="file_pending_2",
            file_type=FileType.UNITY_PACKAGE,
            version_number=1,
            original_url="url2",
        ),
    ]

    await test_db.upsert_world_with_files(world_id, metadata, metrics, discovered_files)

    pending_files = await test_db.get_pending_file_metadata(limit=10)
    assert len(pending_files) == 2

    file_ids = [f[0] for f in pending_files]
    file_types = [f[1] for f in pending_files]

    assert "file_pending_1" in file_ids
    assert "file_pending_2" in file_ids
    assert FileType.IMAGE.value in file_types
    assert FileType.UNITY_PACKAGE.value in file_types


@pytest.mark.asyncio
async def test_update_file_metadata_success(test_db):
    """Test updating file metadata after successful scrape."""
    world_id = "wrld_test_123"
    metadata = {"name": "Test World"}
    metrics = {
        "favorites": 100,
        "heat": 5,
        "popularity": 8,
        "occupants": 12,
        "private_occupants": 3,
        "public_occupants": 9,
        "visits": 1500,
    }

    discovered_files = [
        FileReference(
            file_id="file_test_1",
            file_type=FileType.IMAGE,
            version_number=1,
            original_url="url1",
        ),
    ]

    await test_db.upsert_world_with_files(world_id, metadata, metrics, discovered_files)

    # Update file metadata
    file_metadata = {"size": 12345, "md5": "abcdef", "versions": []}
    await test_db.update_file_metadata("file_test_1", file_metadata, "SUCCESS")

    # Verify update
    async with test_db.async_session() as session:
        result = await session.execute(
            select(FileMetadata).where(FileMetadata.file_id == "file_test_1")
        )
        file_meta = result.scalar_one()
        assert file_meta.file_metadata == file_metadata
        assert file_meta.scrape_status == ScrapeStatus.SUCCESS
        assert file_meta.last_scrape_time is not None
        assert file_meta.error_message is None


@pytest.mark.asyncio
async def test_update_file_metadata_error(test_db):
    """Test updating file metadata after failed scrape."""
    world_id = "wrld_test_123"
    metadata = {"name": "Test World"}
    metrics = {
        "favorites": 100,
        "heat": 5,
        "popularity": 8,
        "occupants": 12,
        "private_occupants": 3,
        "public_occupants": 9,
        "visits": 1500,
    }

    discovered_files = [
        FileReference(
            file_id="file_test_1",
            file_type=FileType.IMAGE,
            version_number=1,
            original_url="url1",
        ),
    ]

    await test_db.upsert_world_with_files(world_id, metadata, metrics, discovered_files)

    # Update with error
    await test_db.update_file_metadata("file_test_1", {}, "ERROR", "404 Not Found")

    # Verify update
    async with test_db.async_session() as session:
        result = await session.execute(
            select(FileMetadata).where(FileMetadata.file_id == "file_test_1")
        )
        file_meta = result.scalar_one()
        assert file_meta.file_metadata is None
        assert file_meta.scrape_status == ScrapeStatus.ERROR
        assert file_meta.error_message == "404 Not Found"


@pytest.mark.asyncio
async def test_get_pending_image_downloads(test_db):
    """Test getting pending image downloads."""
    world_id = "wrld_test_123"
    metadata = {"name": "Test World"}
    metrics = {
        "favorites": 100,
        "heat": 5,
        "popularity": 8,
        "occupants": 12,
        "private_occupants": 3,
        "public_occupants": 9,
        "visits": 1500,
    }

    discovered_files = [
        FileReference(
            file_id="file_image_1",
            file_type=FileType.IMAGE,
            version_number=1,
            original_url="url1",
        ),
        FileReference(
            file_id="file_unity_1",
            file_type=FileType.UNITY_PACKAGE,
            version_number=1,
            original_url="url2",
        ),
    ]

    await test_db.upsert_world_with_files(world_id, metadata, metrics, discovered_files)

    # Update file metadata to SUCCESS (required for image downloads)
    file_metadata = {"versions": [{"file": {"url": "download_url", "md5": "abc123"}}]}
    await test_db.update_file_metadata("file_image_1", file_metadata, "SUCCESS")

    # Get pending downloads
    pending_downloads = await test_db.get_pending_image_downloads(limit=10)

    # Should only return image files with SUCCESS file metadata
    assert len(pending_downloads) == 1
    assert pending_downloads[0][0] == "file_image_1"
    assert pending_downloads[0][1] == file_metadata
    assert pending_downloads[0][2] == FileType.IMAGE.value


@pytest.mark.asyncio
async def test_update_image_download_success(test_db):
    """Test updating image download status after successful download."""
    world_id = "wrld_test_123"
    metadata = {"name": "Test World"}
    metrics = {
        "favorites": 100,
        "heat": 5,
        "popularity": 8,
        "occupants": 12,
        "private_occupants": 3,
        "public_occupants": 9,
        "visits": 1500,
    }

    discovered_files = [
        FileReference(
            file_id="file_image_1",
            file_type=FileType.IMAGE,
            version_number=1,
            original_url="url1",
        ),
    ]

    await test_db.upsert_world_with_files(world_id, metadata, metrics, discovered_files)

    # Update download status
    await test_db.update_image_download(
        "file_image_1",
        "SUCCESS",
        local_file_path="/images/abc/def/file_image_1.png",
        downloaded_md5="abc123",
        downloaded_size_bytes=12345,
    )

    # Verify update
    async with test_db.async_session() as session:
        result = await session.execute(
            select(WorldImage).where(WorldImage.file_id == "file_image_1")
        )
        world_image = result.scalar_one()
        assert world_image.download_status == DownloadStatus.SUCCESS
        assert world_image.local_file_path == "/images/abc/def/file_image_1.png"
        assert world_image.downloaded_md5 == "abc123"
        assert world_image.downloaded_size_bytes == 12345
        assert world_image.success_time is not None
        assert world_image.last_attempt_time is not None


@pytest.mark.asyncio
async def test_update_image_download_error(test_db):
    """Test updating image download status after failed download."""
    world_id = "wrld_test_123"
    metadata = {"name": "Test World"}
    metrics = {
        "favorites": 100,
        "heat": 5,
        "popularity": 8,
        "occupants": 12,
        "private_occupants": 3,
        "public_occupants": 9,
        "visits": 1500,
    }

    discovered_files = [
        FileReference(
            file_id="file_image_1",
            file_type=FileType.IMAGE,
            version_number=1,
            original_url="url1",
        ),
    ]

    await test_db.upsert_world_with_files(world_id, metadata, metrics, discovered_files)

    # Update with error
    await test_db.update_image_download(
        "file_image_1", "ERROR", error_message="Connection timeout"
    )

    # Verify update
    async with test_db.async_session() as session:
        result = await session.execute(
            select(WorldImage).where(WorldImage.file_id == "file_image_1")
        )
        world_image = result.scalar_one()
        assert world_image.download_status == DownloadStatus.ERROR
        assert world_image.error_message == "Connection timeout"
        assert world_image.success_time is None
        assert world_image.last_attempt_time is not None


@pytest.mark.asyncio
async def test_upsert_world_with_files_empty_files_deletes_all(test_db):
    """Test that upserting world with empty files list deletes all existing files."""
    world_id = "wrld_test_123"
    metadata = {"name": "Test World"}
    metrics = {
        "favorites": 100,
        "heat": 5,
        "popularity": 8,
        "occupants": 12,
        "private_occupants": 3,
        "public_occupants": 9,
        "visits": 1500,
    }

    # First insert with files
    initial_files = [
        FileReference(
            file_id="file_to_delete_1",
            file_type=FileType.IMAGE,
            version_number=1,
            original_url="url1",
        ),
        FileReference(
            file_id="file_to_delete_2",
            file_type=FileType.UNITY_PACKAGE,
            version_number=1,
            original_url="url2",
        ),
    ]
    await test_db.upsert_world_with_files(world_id, metadata, metrics, initial_files)

    # Verify files were created
    async with test_db.async_session() as session:
        file_result = await session.execute(
            select(FileMetadata).where(FileMetadata.world_id == world_id)
        )
        files = file_result.fetchall()
        assert len(files) == 2

    # Update with empty files list
    await test_db.upsert_world_with_files(world_id, metadata, metrics, [])

    # Verify all files were deleted
    async with test_db.async_session() as session:
        file_result = await session.execute(
            select(FileMetadata).where(FileMetadata.world_id == world_id)
        )
        files = file_result.fetchall()
        assert len(files) == 0


@pytest.mark.asyncio
async def test_upsert_world_with_files_version_change_resets_status(test_db):
    """Test that changing file version resets scrape status to PENDING."""
    world_id = "wrld_test_123"
    metadata = {"name": "Test World"}
    metrics = {
        "favorites": 100,
        "heat": 5,
        "popularity": 8,
        "occupants": 12,
        "private_occupants": 3,
        "public_occupants": 9,
        "visits": 1500,
    }

    # Insert file with version 1
    initial_files = [
        FileReference(
            file_id="file_version_test",
            file_type=FileType.IMAGE,
            version_number=1,
            original_url="url1",
        ),
    ]
    await test_db.upsert_world_with_files(world_id, metadata, metrics, initial_files)

    # Update file metadata to SUCCESS with some data
    file_metadata = {"size": 12345, "md5": "abcdef"}
    await test_db.update_file_metadata("file_version_test", file_metadata, "SUCCESS")

    # Verify file is marked as SUCCESS
    async with test_db.async_session() as session:
        result = await session.execute(
            select(FileMetadata).where(FileMetadata.file_id == "file_version_test")
        )
        file_meta = result.scalar_one()
        assert file_meta.scrape_status == ScrapeStatus.SUCCESS
        assert file_meta.file_metadata == file_metadata
        assert file_meta.last_scrape_time is not None

    # Update world with same file but different version
    updated_files = [
        FileReference(
            file_id="file_version_test",
            file_type=FileType.IMAGE,
            version_number=2,  # Version changed
            original_url="url1",
        ),
    ]
    await test_db.upsert_world_with_files(world_id, metadata, metrics, updated_files)

    # Verify file status was reset to PENDING
    async with test_db.async_session() as session:
        result = await session.execute(
            select(FileMetadata).where(FileMetadata.file_id == "file_version_test")
        )
        file_meta = result.scalar_one()
        assert file_meta.version_number == 2  # Version updated
        assert file_meta.scrape_status == ScrapeStatus.PENDING  # Status reset
        assert file_meta.file_metadata is None  # Metadata cleared
        assert file_meta.last_scrape_time is None  # Timestamp cleared
        assert file_meta.error_message is None  # Error cleared


@pytest.mark.asyncio
async def test_upsert_world_with_files_same_version_no_change(test_db):
    """Test that upserting same file version doesn't change existing metadata."""
    world_id = "wrld_test_123"
    metadata = {"name": "Test World"}
    metrics = {
        "favorites": 100,
        "heat": 5,
        "popularity": 8,
        "occupants": 12,
        "private_occupants": 3,
        "public_occupants": 9,
        "visits": 1500,
    }

    # Insert file with version 1
    initial_files = [
        FileReference(
            file_id="file_same_version",
            file_type=FileType.IMAGE,
            version_number=1,
            original_url="url1",
        ),
    ]
    await test_db.upsert_world_with_files(world_id, metadata, metrics, initial_files)

    # Update file metadata to SUCCESS
    file_metadata = {"size": 12345, "md5": "abcdef"}
    await test_db.update_file_metadata("file_same_version", file_metadata, "SUCCESS")

    # Get the current state for comparison
    async with test_db.async_session() as session:
        result = await session.execute(
            select(FileMetadata).where(FileMetadata.file_id == "file_same_version")
        )
        original_file = result.scalar_one()
        original_scrape_time = original_file.last_scrape_time

    # Update world with same file and same version
    same_files = [
        FileReference(
            file_id="file_same_version",
            file_type=FileType.IMAGE,
            version_number=1,  # Same version
            original_url="url1",
        ),
    ]
    await test_db.upsert_world_with_files(world_id, metadata, metrics, same_files)

    # Verify file metadata was not changed
    async with test_db.async_session() as session:
        result = await session.execute(
            select(FileMetadata).where(FileMetadata.file_id == "file_same_version")
        )
        file_meta = result.scalar_one()
        assert file_meta.version_number == 1
        assert file_meta.scrape_status == ScrapeStatus.SUCCESS  # Unchanged
        assert file_meta.file_metadata == file_metadata  # Unchanged
        assert file_meta.last_scrape_time == original_scrape_time  # Unchanged


@pytest.mark.asyncio
async def test_upsert_world_with_files_unity_package_no_world_images(test_db):
    """Test that UNITY_PACKAGE files don't create world_images entries."""
    world_id = "wrld_test_123"
    metadata = {"name": "Test World"}
    metrics = {
        "favorites": 100,
        "heat": 5,
        "popularity": 8,
        "occupants": 12,
        "private_occupants": 3,
        "public_occupants": 9,
        "visits": 1500,
    }

    # Insert only unity package file
    unity_files = [
        FileReference(
            file_id="file_unity_only",
            file_type=FileType.UNITY_PACKAGE,
            version_number=1,
            original_url="url1",
        ),
    ]
    await test_db.upsert_world_with_files(world_id, metadata, metrics, unity_files)

    # Verify file_metadata was created
    async with test_db.async_session() as session:
        file_result = await session.execute(
            select(FileMetadata).where(FileMetadata.file_id == "file_unity_only")
        )
        file_meta = file_result.scalar_one()
        assert file_meta.file_type == FileType.UNITY_PACKAGE.value

        # Verify no world_images entry was created
        image_result = await session.execute(
            select(WorldImage).where(WorldImage.file_id == "file_unity_only")
        )
        world_image = image_result.scalar_one_or_none()
        assert world_image is None


@pytest.mark.asyncio
async def test_upsert_world_with_files_existing_world_images_not_duplicated(test_db):
    """Test that existing world_images entries are not duplicated."""
    world_id = "wrld_test_123"
    metadata = {"name": "Test World"}
    metrics = {
        "favorites": 100,
        "heat": 5,
        "popularity": 8,
        "occupants": 12,
        "private_occupants": 3,
        "public_occupants": 9,
        "visits": 1500,
    }

    # Insert image file
    image_files = [
        FileReference(
            file_id="file_image_existing",
            file_type=FileType.IMAGE,
            version_number=1,
            original_url="url1",
        ),
    ]
    await test_db.upsert_world_with_files(world_id, metadata, metrics, image_files)

    # Verify world_images entry was created
    async with test_db.async_session() as session:
        image_result = await session.execute(
            select(WorldImage).where(WorldImage.file_id == "file_image_existing")
        )
        world_images = image_result.fetchall()
        assert len(world_images) == 1

    # Update world again with same image file (but different world metadata)
    updated_metadata = {"name": "Updated Test World"}
    await test_db.upsert_world_with_files(
        world_id, updated_metadata, metrics, image_files
    )

    # Verify still only one world_images entry exists
    async with test_db.async_session() as session:
        image_result = await session.execute(
            select(WorldImage).where(WorldImage.file_id == "file_image_existing")
        )
        world_images = image_result.fetchall()
        assert len(world_images) == 1


@pytest.mark.asyncio
async def test_get_pending_file_metadata_limit_respected(test_db):
    """Test that the limit parameter is respected in get_pending_file_metadata."""
    world_id = "wrld_test_123"
    metadata = {"name": "Test World"}
    metrics = {
        "favorites": 100,
        "heat": 5,
        "popularity": 8,
        "occupants": 12,
        "private_occupants": 3,
        "public_occupants": 9,
        "visits": 1500,
    }

    # Create 5 pending files
    pending_files = [
        FileReference(
            file_id=f"file_pending_{i}",
            file_type=FileType.IMAGE,
            version_number=1,
            original_url=f"url{i}",
        )
        for i in range(5)
    ]
    await test_db.upsert_world_with_files(world_id, metadata, metrics, pending_files)

    # Get only 3 files with limit
    result = await test_db.get_pending_file_metadata(limit=3)
    assert len(result) == 3

    # Get all files with higher limit
    result = await test_db.get_pending_file_metadata(limit=10)
    assert len(result) == 5


@pytest.mark.asyncio
async def test_get_pending_image_downloads_only_success_files(test_db):
    """Test that get_pending_image_downloads only returns files with SUCCESS file_metadata."""
    world_id = "wrld_test_123"
    metadata = {"name": "Test World"}
    metrics = {
        "favorites": 100,
        "heat": 5,
        "popularity": 8,
        "occupants": 12,
        "private_occupants": 3,
        "public_occupants": 9,
        "visits": 1500,
    }

    # Create image files
    image_files = [
        FileReference(
            file_id="file_image_success",
            file_type=FileType.IMAGE,
            version_number=1,
            original_url="url1",
        ),
        FileReference(
            file_id="file_image_pending",
            file_type=FileType.IMAGE,
            version_number=1,
            original_url="url2",
        ),
        FileReference(
            file_id="file_image_error",
            file_type=FileType.IMAGE,
            version_number=1,
            original_url="url3",
        ),
    ]
    await test_db.upsert_world_with_files(world_id, metadata, metrics, image_files)

    # Update one file to SUCCESS, one to ERROR, leave one PENDING
    await test_db.update_file_metadata(
        "file_image_success", {"test": "data"}, "SUCCESS"
    )
    await test_db.update_file_metadata("file_image_error", {}, "ERROR", "Test error")
    # file_image_pending stays PENDING

    # Get pending downloads - should only return the SUCCESS file
    pending_downloads = await test_db.get_pending_image_downloads()
    assert len(pending_downloads) == 1
    assert pending_downloads[0][0] == "file_image_success"
    assert pending_downloads[0][1] == {"test": "data"}


@pytest.mark.asyncio
async def test_update_file_metadata_nonexistent_file(test_db):
    """Test that updating nonexistent file metadata handles gracefully."""
    # Try to update a file that doesn't exist
    await test_db.update_file_metadata("nonexistent_file", {"test": "data"}, "SUCCESS")

    # Should not raise an error, just do nothing
    # Verify no file was created
    async with test_db.async_session() as session:
        result = await session.execute(
            select(FileMetadata).where(FileMetadata.file_id == "nonexistent_file")
        )
        file_meta = result.scalar_one_or_none()
        assert file_meta is None


@pytest.mark.asyncio
async def test_update_image_download_nonexistent_image(test_db):
    """Test that updating nonexistent image download handles gracefully."""
    # Try to update an image that doesn't exist
    await test_db.update_image_download(
        "nonexistent_image",
        "SUCCESS",
        local_file_path="/test/path.png",
        downloaded_md5="abc123",
        downloaded_size_bytes=12345,
    )

    # Should not raise an error, just do nothing
    # Verify no image entry was created
    async with test_db.async_session() as session:
        result = await session.execute(
            select(WorldImage).where(WorldImage.file_id == "nonexistent_image")
        )
        world_image = result.scalar_one_or_none()
        assert world_image is None


@pytest.mark.asyncio
async def test_upsert_world_preserves_existing_metadata_on_discovery(test_db):
    """Test that upserting world from recent worlds discovery preserves existing detailed metadata."""
    world_id = "wrld_test_123"

    # First, simulate detailed world scraping with complete metadata
    detailed_metadata = {
        "name": "Test World",
        "description": "A detailed test world",
        "capacity": 32,
        "favorites": 100,
        "visits": 1500,
        "heat": 5,
        "popularity": 8,
        "tags": ["system_approved"],
        "created_at": "2024-01-01T00:00:00.000Z",
        "updated_at": "2024-01-02T00:00:00.000Z",
    }
    await test_db.upsert_world(world_id, detailed_metadata, "SUCCESS")

    # Verify detailed metadata was saved
    async with test_db.async_session() as session:
        result = await session.execute(select(World).where(World.world_id == world_id))
        world = result.scalar_one()
        assert world.world_metadata == detailed_metadata
        assert world.scrape_status == ScrapeStatus.SUCCESS

    # Now simulate recent worlds discovery with minimal metadata (what happens currently)
    discovery_metadata = {"discovered_at": "2024-01-03T00:00:00.000Z"}
    await test_db.upsert_world(world_id, discovery_metadata, "PENDING")

    # Verify that detailed metadata is preserved (this test will fail with current implementation)
    async with test_db.async_session() as session:
        result = await session.execute(select(World).where(World.world_id == world_id))
        world = result.scalar_one()

        # The detailed metadata should still be there
        assert "name" in world.world_metadata
        assert "description" in world.world_metadata
        assert "capacity" in world.world_metadata
        assert world.world_metadata["name"] == "Test World"
        assert world.world_metadata["description"] == "A detailed test world"

        # But the discovery timestamp should be added/updated
        assert "discovered_at" in world.world_metadata
        assert world.world_metadata["discovered_at"] == "2024-01-03T00:00:00.000Z"

        # Status should be updated to PENDING for re-scraping
        assert world.scrape_status == ScrapeStatus.PENDING


@pytest.mark.asyncio
async def test_batch_upsert_worlds_new_worlds(test_db):
    """Test batch upserting multiple new worlds."""
    worlds_data = [
        ("wrld_batch_1", {"name": "Batch World 1"}, "SUCCESS"),
        ("wrld_batch_2", {"name": "Batch World 2"}, "PENDING"),
        ("wrld_batch_3", {"discovered_at": "2024-01-01T00:00:00.000Z"}, "PENDING"),
    ]

    await test_db.batch_upsert_worlds(worlds_data)

    # Verify all worlds were created
    async with test_db.async_session() as session:
        for world_id, expected_metadata, expected_status in worlds_data:
            result = await session.execute(
                select(World).where(World.world_id == world_id)
            )
            world = result.scalar_one()
            assert world.world_metadata == expected_metadata
            assert world.scrape_status == ScrapeStatus(expected_status)


@pytest.mark.asyncio
async def test_batch_upsert_worlds_mixed_new_and_existing(test_db):
    """Test batch upserting with mix of new and existing worlds."""
    # First create an existing world with detailed metadata
    existing_world_id = "wrld_existing"
    detailed_metadata = {
        "name": "Existing World",
        "description": "An existing world with details",
        "capacity": 16,
    }
    await test_db.upsert_world(existing_world_id, detailed_metadata, "SUCCESS")

    # Now batch upsert with discovery data for existing world + new worlds
    worlds_data = [
        (existing_world_id, {"discovered_at": "2024-01-01T12:00:00.000Z"}, "PENDING"),
        ("wrld_new_1", {"name": "New World 1"}, "SUCCESS"),
        ("wrld_new_2", {"discovered_at": "2024-01-01T12:00:00.000Z"}, "PENDING"),
    ]

    await test_db.batch_upsert_worlds(worlds_data)

    # Verify existing world metadata was preserved and discovery timestamp added
    async with test_db.async_session() as session:
        result = await session.execute(
            select(World).where(World.world_id == existing_world_id)
        )
        existing_world = result.scalar_one()

        # Should have both original detailed metadata and discovery timestamp
        assert existing_world.world_metadata["name"] == "Existing World"
        assert (
            existing_world.world_metadata["description"]
            == "An existing world with details"
        )
        assert existing_world.world_metadata["capacity"] == 16
        assert (
            existing_world.world_metadata["discovered_at"] == "2024-01-01T12:00:00.000Z"
        )
        assert existing_world.scrape_status == ScrapeStatus.PENDING

        # Verify new worlds were created correctly
        result = await session.execute(
            select(World).where(World.world_id == "wrld_new_1")
        )
        new_world1 = result.scalar_one()
        assert new_world1.world_metadata == {"name": "New World 1"}
        assert new_world1.scrape_status == ScrapeStatus.SUCCESS

        result = await session.execute(
            select(World).where(World.world_id == "wrld_new_2")
        )
        new_world2 = result.scalar_one()
        assert new_world2.world_metadata == {
            "discovered_at": "2024-01-01T12:00:00.000Z"
        }
        assert new_world2.scrape_status == ScrapeStatus.PENDING


@pytest.mark.asyncio
async def test_batch_upsert_worlds_empty_list(test_db):
    """Test batch upserting with empty list does nothing."""
    await test_db.batch_upsert_worlds([])

    # Verify no worlds were created
    async with test_db.async_session() as session:
        result = await session.execute(select(World))
        worlds = result.fetchall()
        assert len(worlds) == 0
