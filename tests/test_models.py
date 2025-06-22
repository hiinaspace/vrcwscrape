"""Tests for Pydantic models."""

import json
from pathlib import Path

from src.vrchat_scraper.models import (
    WorldSummary,
    WorldDetail,
    FileMetadata,
    FileType,
    _parse_file_url,
)


def load_fixture(filename: str):
    """Load test fixture data."""
    fixtures_path = Path(__file__).parent / "fixtures"
    with open(fixtures_path / filename) as f:
        return json.load(f)


def test_recent_worlds_parsing():
    """Test parsing of recent worlds fixture data."""
    recent_worlds_data = load_fixture("recent_worlds.json")

    # Parse first world from fixture
    world_data = recent_worlds_data[0]
    world = WorldSummary(**world_data)

    assert world.id == "wrld_08b698a3-8d43-42fb-ab3d-0b2ce3375c1d"
    assert world.name == "TaoLandia"
    assert world.author_name == "Marino․"
    assert (
        world.image_url
        == "https://api.vrchat.cloud/api/1/file/file_75572866-4841-4704-b148-01fb5a3490b8/12/file"
    )
    assert (
        world.thumbnail_url
        == "https://api.vrchat.cloud/api/1/image/file_75572866-4841-4704-b148-01fb5a3490b8/12/256"
    )
    assert len(world.unity_packages) == 2
    assert world.unity_packages[0].platform == "android"


def test_world_detail_parsing():
    """Test parsing of world detail fixture data."""
    world_data = load_fixture("world_detail.json")
    world = WorldDetail(**world_data)

    assert world.id == "wrld_7d8aa2f4-7b3b-4fec-a446-0929cbf0b61b"
    assert world.name == "Sandy Beach"
    assert world.description == "Sandy Beach"
    assert (
        world.image_url
        == "https://api.vrchat.cloud/api/1/file/file_447b6078-e5fb-488c-bff5-432d4631f6cf/2/file"
    )
    assert world.visits == 11302
    assert len(world.unity_packages) == 1
    assert (
        world.unity_packages[0].asset_url
        == "https://api.vrchat.cloud/api/1/file/file_6b4497d9-0403-4977-b53e-5eff11b6f12e/49/file"
    )


def test_file_metadata_parsing():
    """Test parsing of file metadata fixture data."""
    # Test image file metadata
    image_data = load_fixture("image_file_metadata.json")
    image_metadata = FileMetadata(**image_data)

    assert image_metadata.id == "file_447b6078-e5fb-488c-bff5-432d4631f6cf"
    assert image_metadata.extension == ".png"
    assert image_metadata.mime_type == "image/png"
    assert len(image_metadata.versions) == 3

    latest_version = image_metadata.get_latest_version()
    assert latest_version is not None
    assert latest_version.version == 2
    assert latest_version.file.md5 == "Kr+54w1dA19dG24hsMusPg=="
    assert latest_version.file.size_in_bytes == 311598

    # Test unity package file metadata
    unity_data = load_fixture("unity_package_file_metadata.json")
    unity_metadata = FileMetadata(**unity_data)

    assert unity_metadata.id == "file_6b4497d9-0403-4977-b53e-5eff11b6f12e"
    assert unity_metadata.extension == ".vrcw"
    assert len(unity_metadata.versions) == 50  # 0-49

    latest_version = unity_metadata.get_latest_version()
    assert latest_version is not None
    assert latest_version.version == 49
    assert latest_version.file.size_in_bytes == 20632284


def test_file_url_parsing():
    """Test parsing of file URLs to extract ID and version."""
    # Test image URL parsing
    image_url = "https://api.vrchat.cloud/api/1/file/file_447b6078-e5fb-488c-bff5-432d4631f6cf/2/file"
    file_ref = _parse_file_url(image_url, FileType.IMAGE)

    assert file_ref is not None
    assert file_ref.file_id == "file_447b6078-e5fb-488c-bff5-432d4631f6cf"
    assert file_ref.version_number == 2
    assert file_ref.file_type == FileType.IMAGE
    assert file_ref.original_url == image_url

    # Test thumbnail URL parsing
    thumbnail_url = "https://api.vrchat.cloud/api/1/image/file_447b6078-e5fb-488c-bff5-432d4631f6cf/2/256"
    file_ref = _parse_file_url(thumbnail_url, FileType.IMAGE)

    assert file_ref is not None
    assert file_ref.file_id == "file_447b6078-e5fb-488c-bff5-432d4631f6cf"
    assert file_ref.version_number == 2

    # Test unity package URL parsing
    unity_url = "https://api.vrchat.cloud/api/1/file/file_6b4497d9-0403-4977-b53e-5eff11b6f12e/49/file"
    file_ref = _parse_file_url(unity_url, FileType.UNITY_PACKAGE)

    assert file_ref is not None
    assert file_ref.file_id == "file_6b4497d9-0403-4977-b53e-5eff11b6f12e"
    assert file_ref.version_number == 49
    assert file_ref.file_type == FileType.UNITY_PACKAGE

    # Test invalid URL
    invalid_url = "https://example.com/not/a/vrchat/url"
    file_ref = _parse_file_url(invalid_url, FileType.IMAGE)
    assert file_ref is None


def test_discovered_files_from_world_summary():
    """Test file discovery from world summary."""
    recent_worlds_data = load_fixture("recent_worlds.json")
    world = WorldSummary(**recent_worlds_data[0])

    files = world.discovered_files
    assert len(files) == 1  # imageUrl only (thumbnailImageUrl is same file with dynamic resizing)

    # Check image file
    image_file = next(
        f
        for f in files
        if f.file_type == FileType.IMAGE and "file_75572866" in f.file_id
    )
    assert image_file.file_id == "file_75572866-4841-4704-b148-01fb5a3490b8"
    assert image_file.version_number == 12


def test_discovered_files_from_world_detail():
    """Test file discovery from world detail."""
    world_data = load_fixture("world_detail.json")
    world = WorldDetail(**world_data)

    files = world.discovered_files
    assert len(files) == 2  # imageUrl only (no thumbnailImageUrl) and 1 unity package

    # Check image files
    image_files = [f for f in files if f.file_type == FileType.IMAGE]
    assert len(image_files) == 1

    # Check unity package file
    unity_files = [f for f in files if f.file_type == FileType.UNITY_PACKAGE]
    assert len(unity_files) == 1
    unity_file = unity_files[0]
    assert unity_file.file_id == "file_6b4497d9-0403-4977-b53e-5eff11b6f12e"
    assert unity_file.version_number == 49


def test_extract_metrics():
    """Test extraction of ephemeral metrics."""
    world_data = load_fixture("world_detail.json")
    world = WorldDetail(**world_data)

    metrics = world.extract_metrics()
    expected_keys = [
        "favorites",
        "heat",
        "popularity",
        "occupants",
        "private_occupants",
        "public_occupants",
        "visits",
    ]

    assert set(metrics.keys()) == set(expected_keys)
    assert metrics["favorites"] == 742
    assert metrics["visits"] == 11302
    assert metrics["occupants"] == 0


def test_stable_metadata():
    """Test stable metadata extraction."""
    world_data = load_fixture("world_detail.json")
    world = WorldDetail(**world_data)

    stable_data = world.stable_metadata()

    # Should not contain ephemeral fields
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
        assert field not in stable_data

    # Should contain stable fields
    assert stable_data["id"] == "wrld_7d8aa2f4-7b3b-4fec-a446-0929cbf0b61b"
    assert stable_data["name"] == "Sandy Beach"
    assert stable_data["author_id"] == "usr_6c126be6-bada-464f-ab99-9b39b525f8bb"
