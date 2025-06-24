"""Unit tests for raw file metadata API response storage strategy."""

import pytest
import pytest_asyncio
from unittest.mock import patch

from src.vrchat_scraper.database import Database
from src.vrchat_scraper.scraper import VRChatScraper
from src.vrchat_scraper.models import FileMetadata
from tests.fakes import (
    FakeVRChatAPIClient,
    FakeImageDownloader,
    MockTime,
    MockAsyncSleep,
)


class StubRateLimiter:
    """Stub rate limiter that always allows requests through immediately."""

    def get_delay_until_next_request(self, now: float) -> float:
        return 0.0  # Always allow requests immediately

    def on_request_sent(self, request_id: str, timestamp: float):
        pass  # No-op

    def on_success(self, request_id: str, timestamp: float):
        pass  # No-op

    def on_error(self, request_id: str, timestamp: float):
        pass  # No-op


class StubCircuitBreaker:
    """Stub circuit breaker that never blocks requests."""

    def get_delay_until_proceed(self, now: float) -> float:
        return 0.0  # Always allow requests immediately

    def on_success(self):
        pass  # No-op

    def on_error(self, timestamp: float):
        pass  # No-op


@pytest_asyncio.fixture
async def test_database():
    """Create an in-memory SQLite database for testing."""
    db = Database("sqlite:///:memory:")
    await db.init_schema()
    return db


@pytest.fixture
def mock_time():
    """Create a mock time source."""
    return MockTime(start_time=1000.0)


@pytest.fixture
def mock_sleep(mock_time):
    """Create a mock async sleep function."""
    return MockAsyncSleep(mock_time)


@pytest.fixture
def fake_api_client(mock_time):
    """Create a fake VRChat API client."""
    return FakeVRChatAPIClient(mock_time.now)


@pytest.fixture
def fake_image_downloader(mock_time):
    """Create a fake image downloader."""
    return FakeImageDownloader(mock_time.now)


@pytest.fixture
def stub_scraper(
    test_database,
    fake_api_client,
    fake_image_downloader,
    mock_time,
    mock_sleep,
):
    """Create a VRChat scraper with stub rate limiters and circuit breakers."""
    return VRChatScraper(
        database=test_database,
        api_client=fake_api_client,
        image_downloader=fake_image_downloader,
        api_rate_limiter=StubRateLimiter(),
        image_rate_limiter=StubRateLimiter(),
        api_circuit_breaker=StubCircuitBreaker(),
        image_circuit_breaker=StubCircuitBreaker(),
        time_source=mock_time.now,
        sleep_func=mock_sleep.sleep,
    )


@pytest.mark.asyncio
async def test_file_metadata_scraping_stores_raw_api_response(
    stub_scraper, fake_api_client, test_database
):
    """Test that file metadata scraping stores the raw API response instead of pydantic model dump."""
    file_id = "file_test_image_123"

    # Create a raw API response (what VRChat file API actually returns)
    raw_api_response = {
        "id": file_id,
        "name": "Test Image File",
        "extension": ".png",
        "mimeType": "image/png",
        "ownerId": "usr_test_owner",
        "tags": ["user_file"],
        "versions": [
            {
                "version": 0,
                "status": "complete",
                "created_at": "2024-01-01T10:00:00.000Z",
            },
            {
                "version": 1,
                "status": "complete",
                "created_at": "2024-01-01T12:00:00.000Z",
                "file": {
                    "md5": "Kr+54w1dA19dG24hsMusPg==",
                    "sizeInBytes": 311598,
                    "url": "https://api.vrchat.cloud/api/1/file/file_test_image_123/1/file",
                    "fileName": "test_image.png",
                    "status": "complete",
                    "category": "simple",
                },
                "signature": {
                    "md5": "aATXZOPLbeXrLOIFx7cCkQ==",
                    "sizeInBytes": 5520,
                    "url": "https://api.vrchat.cloud/api/1/file/file_test_image_123/1/signature",
                    "fileName": "test_image.png.signature",
                    "status": "complete",
                    "category": "simple",
                },
            },
        ],
        # Extra API fields that should be preserved
        "uploadedBy": "usr_uploader",
        "visibility": "public",
        "metadata": {"compression": "png", "originalSize": 400000},
    }

    # Set up file in database as PENDING
    await test_database.upsert_world("wrld_test", {}, "SUCCESS")
    from src.vrchat_scraper.models import FileReference, FileType

    file_ref = FileReference(
        file_id=file_id,
        file_type=FileType.IMAGE,
        version_number=1,
        original_url="https://example.com/test",
    )
    # Need to provide metrics when creating world with files
    dummy_metrics = {
        "favorites": 0,
        "heat": 0,
        "popularity": 0,
        "occupants": 0,
        "private_occupants": 0,
        "public_occupants": 0,
        "visits": 0,
    }
    await test_database.upsert_world_with_files(
        "wrld_test", {}, dummy_metrics, [file_ref], "SUCCESS"
    )

    # Mock the API client to return raw JSON instead of pydantic model
    with patch.object(fake_api_client, "get_file_metadata") as mock_get_file_metadata:
        # The API client should return raw JSON, not a parsed FileMetadata object
        mock_get_file_metadata.return_value = raw_api_response

        # Act: Scrape the file metadata
        await stub_scraper._scrape_file_metadata_task(file_id)

        # Assert: Check that the stored metadata is the raw API response
        async with test_database.async_session() as session:
            from src.vrchat_scraper.database import FileMetadata as FileMetadataDB
            from sqlalchemy import select

            result = await session.execute(
                select(FileMetadataDB).where(FileMetadataDB.file_id == file_id)
            )
            file_metadata_record = result.scalar_one()

            stored_metadata = file_metadata_record.file_metadata

            # The stored metadata should be exactly the raw API response
            assert stored_metadata == raw_api_response

            # Verify that extra API fields are preserved (these would be lost in current implementation)
            assert stored_metadata["uploadedBy"] == "usr_uploader"
            assert stored_metadata["visibility"] == "public"
            assert stored_metadata["metadata"]["compression"] == "png"
            assert stored_metadata["metadata"]["originalSize"] == 400000

            # Verify that standard fields are preserved
            assert stored_metadata["id"] == file_id
            assert stored_metadata["name"] == "Test Image File"
            assert stored_metadata["extension"] == ".png"
            assert stored_metadata["mimeType"] == "image/png"
            assert len(stored_metadata["versions"]) == 2


@pytest.mark.asyncio
async def test_file_metadata_still_enables_image_downloads(
    stub_scraper, fake_api_client, test_database
):
    """Test that image downloads still work with raw file metadata storage."""
    file_id = "file_download_test"

    raw_api_response = {
        "id": file_id,
        "name": "Download Test Image",
        "extension": ".png",
        "mimeType": "image/png",
        "ownerId": "usr_test",
        "versions": [
            {
                "version": 1,
                "status": "complete",
                "created_at": "2024-01-01T12:00:00.000Z",
                "file": {
                    "md5": "testMD5hash1234567890==",
                    "sizeInBytes": 150000,
                    "url": "https://api.vrchat.cloud/api/1/file/file_download_test/1/file",
                    "fileName": "download_test.png",
                },
            }
        ],
    }

    # Set up file metadata in database
    await test_database.upsert_world("wrld_test", {}, "SUCCESS")
    from src.vrchat_scraper.models import FileReference, FileType

    file_ref = FileReference(
        file_id=file_id,
        file_type=FileType.IMAGE,
        version_number=1,
        original_url="https://example.com/test",
    )
    # Need to provide metrics when creating world with files
    dummy_metrics = {
        "favorites": 0,
        "heat": 0,
        "popularity": 0,
        "occupants": 0,
        "private_occupants": 0,
        "public_occupants": 0,
        "visits": 0,
    }
    await test_database.upsert_world_with_files(
        "wrld_test", {}, dummy_metrics, [file_ref], "SUCCESS"
    )

    with patch.object(fake_api_client, "get_file_metadata") as mock_get_file_metadata:
        mock_get_file_metadata.return_value = raw_api_response

        # Scrape file metadata first
        await stub_scraper._scrape_file_metadata_task(file_id)

        # Verify the file metadata was stored successfully
        async with test_database.async_session() as session:
            from src.vrchat_scraper.database import FileMetadata as FileMetadataDB
            from sqlalchemy import select

            result = await session.execute(
                select(FileMetadataDB).where(FileMetadataDB.file_id == file_id)
            )
            file_metadata_record = result.scalar_one()
            assert file_metadata_record.scrape_status == "SUCCESS"
            assert file_metadata_record.file_metadata == raw_api_response

        # Now test that image download can process the raw metadata
        # Get the actual fake_image_downloader from the scraper
        actual_image_downloader = stub_scraper.image_downloader
        actual_image_downloader.set_download_result(
            file_id, True, f"/fake/path/{file_id}.png", 150000, ""
        )

        # Get pending image downloads - should include our file
        pending_images = await test_database.get_pending_image_downloads(limit=10)
        assert len(pending_images) == 1

        download = pending_images[0]
        assert download.file_id == file_id
        assert download.version == 1
        assert download.filename == "download_test.png"

        # Execute image download task
        await stub_scraper._download_image_content_task(
            download.file_id, 
            download.version, 
            download.filename, 
            download.md5, 
            download.size_bytes, 
            download.download_url
        )

        # Verify image download succeeded
        async with test_database.async_session() as session:
            from src.vrchat_scraper.database import ImageContent
            from sqlalchemy import select

            result = await session.execute(
                select(ImageContent).where(ImageContent.file_id == file_id)
            )
            image_record = result.scalar_one()
            assert image_record.state == "CONFIRMED"
            assert image_record.sha256 is not None


@pytest.mark.asyncio
async def test_raw_file_metadata_preserves_unknown_fields(
    stub_scraper, fake_api_client, test_database
):
    """Test that unknown/future file metadata API fields are preserved in raw storage."""
    file_id = "file_future_test"

    # Simulate API response with new fields that don't exist in our pydantic model
    raw_api_response = {
        "id": file_id,
        "name": "Future File",
        "extension": ".png",
        "mimeType": "image/png",
        "ownerId": "usr_test",
        "versions": [
            {
                "version": 1,
                "status": "complete",
                "file": {
                    "md5": "futureMD5hash==",
                    "sizeInBytes": 200000,
                    "url": "https://api.vrchat.cloud/api/1/file/file_future_test/1/file",
                    "fileName": "future.png",
                },
            }
        ],
        # Future fields that don't exist in our current FileMetadata pydantic model
        "aiGenerated": True,
        "contentRating": "safe",
        "compressionAlgorithm": "webp",
        "qualityMetrics": {
            "resolution": "1920x1080",
            "colorDepth": 24,
            "compressionRatio": 0.85,
        },
        "accessibility": {
            "altText": "A future image with AI assistance",
            "hasTransparency": False,
        },
    }

    # Set up file in database
    await test_database.upsert_world("wrld_test", {}, "SUCCESS")
    from src.vrchat_scraper.models import FileReference, FileType

    file_ref = FileReference(
        file_id=file_id,
        file_type=FileType.IMAGE,
        version_number=1,
        original_url="https://example.com/test",
    )
    # Need to provide metrics when creating world with files
    dummy_metrics = {
        "favorites": 0,
        "heat": 0,
        "popularity": 0,
        "occupants": 0,
        "private_occupants": 0,
        "public_occupants": 0,
        "visits": 0,
    }
    await test_database.upsert_world_with_files(
        "wrld_test", {}, dummy_metrics, [file_ref], "SUCCESS"
    )

    with patch.object(fake_api_client, "get_file_metadata") as mock_get_file_metadata:
        mock_get_file_metadata.return_value = raw_api_response

        # Act: Scrape the file metadata
        await stub_scraper._scrape_file_metadata_task(file_id)

        # Assert: Verify that future/unknown fields are preserved
        async with test_database.async_session() as session:
            from src.vrchat_scraper.database import FileMetadata as FileMetadataDB
            from sqlalchemy import select

            result = await session.execute(
                select(FileMetadataDB).where(FileMetadataDB.file_id == file_id)
            )
            file_metadata_record = result.scalar_one()

            stored_metadata = file_metadata_record.file_metadata

            # Verify future fields are preserved exactly
            assert stored_metadata["aiGenerated"] is True
            assert stored_metadata["contentRating"] == "safe"
            assert stored_metadata["compressionAlgorithm"] == "webp"
            assert stored_metadata["qualityMetrics"]["resolution"] == "1920x1080"
            assert stored_metadata["qualityMetrics"]["colorDepth"] == 24
            assert stored_metadata["qualityMetrics"]["compressionRatio"] == 0.85
            assert (
                stored_metadata["accessibility"]["altText"]
                == "A future image with AI assistance"
            )
            assert stored_metadata["accessibility"]["hasTransparency"] is False

            # Verify that the entire response structure is preserved
            assert stored_metadata == raw_api_response


@pytest.mark.asyncio
async def test_current_file_metadata_implementation_processes_pydantic(test_database):
    """Test that demonstrates the current implementation behavior with pydantic processing."""
    file_id = "file_current_behavior"

    # Simulate what the current implementation does
    # This test demonstrates current behavior - processing through pydantic model

    # Create raw API response
    raw_api_response = {
        "id": file_id,
        "name": "Current Behavior Test",
        "extension": ".png",
        "mimeType": "image/png",
        "ownerId": "usr_test",
        "versions": [
            {
                "version": 1,
                "status": "complete",
                "created_at": "2024-01-01T12:00:00.000Z",
                "file": {
                    "md5": "currentMD5==",
                    "sizeInBytes": 100000,
                    "url": "https://api.vrchat.cloud/api/1/file/file_current_behavior/1/file",
                    "fileName": "current.png",
                },
            }
        ],
        # Extra field that would be lost in current pydantic processing
        "extraField": "this gets lost",
    }

    # Current implementation: parse to pydantic then dump back to JSON
    file_metadata = FileMetadata(**raw_api_response)
    processed_metadata = file_metadata.model_dump(mode="json", by_alias=True)

    # Store using current approach (processes through pydantic)
    await test_database.upsert_world("wrld_test", {}, "SUCCESS")
    from src.vrchat_scraper.models import FileReference, FileType

    file_ref = FileReference(
        file_id=file_id,
        file_type=FileType.IMAGE,
        version_number=1,
        original_url="https://example.com/test",
    )
    # Need to provide metrics when creating world with files
    dummy_metrics = {
        "favorites": 0,
        "heat": 0,
        "popularity": 0,
        "occupants": 0,
        "private_occupants": 0,
        "public_occupants": 0,
        "visits": 0,
    }
    await test_database.upsert_world_with_files(
        "wrld_test", {}, dummy_metrics, [file_ref], "SUCCESS"
    )

    await test_database.update_file_metadata(file_id, processed_metadata, "SUCCESS")

    # Verify the processed metadata was stored (current behavior)
    async with test_database.async_session() as session:
        from src.vrchat_scraper.database import FileMetadata as FileMetadataDB
        from sqlalchemy import select

        result = await session.execute(
            select(FileMetadataDB).where(FileMetadataDB.file_id == file_id)
        )
        file_metadata_record = result.scalar_one()

        stored_metadata = file_metadata_record.file_metadata

        # Current implementation loses extra fields through pydantic processing
        assert "extraField" not in stored_metadata

        # But preserves standard fields
        assert stored_metadata["id"] == file_id
        assert stored_metadata["name"] == "Current Behavior Test"
        assert stored_metadata["extension"] == ".png"

        # Demonstrate that processed != raw (this is what we want to fix)
        assert stored_metadata != raw_api_response
