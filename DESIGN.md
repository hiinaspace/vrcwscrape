# VRChat World Metadata Scraper Design

## Overview

This project maintains a copy of VRChat's world metadata database by periodically scraping their HTTP API. The data is stored in DoltDB (a Git-like MySQL-compatible database) to enable versioned data access and collaborative scraping.

Primary goals:
1. Enable better search interfaces than VRChat's native search capabilities
2. Provide archival preservation of world metadata in case VRChat shuts down
3. Allow researchers and developers to analyze trends in virtual world creation

## System Architecture

### Core Components

1. **Scraper Service** - Python asyncio processes that coordinate via database:
   - Discovers new worlds via the "recently updated" API endpoint
   - Fetches complete metadata for individual worlds with transactional file tracking
   - Scrapes VRChat file metadata for images and unity packages
   - Downloads and verifies world thumbnail images
   - Manages independent rate limiting for each API endpoint
   - Maintains transactional consistency across world and file metadata

2. **Database** - DoltDB instance coordinating distributed scraping:
   - World metadata (mostly raw JSON responses)
   - Time-series metrics (ephemeral fields like visitor counts)
   - File metadata with VRChat sizes, MD5 hashes, and versions
   - Multi-phase work queues (world scraping, file metadata, image downloads)
   - Download status and content verification results

3. **File Storage** - Local filesystem storing:
   - World thumbnail images (800x600 PNGs)
   - Organized in subdirectories to avoid filesystem limitations

### Data Flow

```
VRChat APIs → Rate Limiters → Multi-Phase Scraper → DoltDB
                                                      ↕
   ┌─────────────────────────────────────────────────┘
   │
   ├─ World Discovery Queue (PENDING worlds)
   ├─ File Metadata Queue (PENDING file_metadata)
   ├─ Image Download Queue (PENDING world_images)
   │
   └─ Coordination via Database State
                ↓
          Image Storage
     (MD5 verified, organized by file_uuid)
```

## API Integration

### Endpoints Used

- **Recent Worlds**: `GET /api/1/worlds?sort=updated`
  - Returns array of recently updated worlds (up to 1000)
  - Used periodically to discover new/updated worlds (default ~10 minutes)

- **World Details**: `GET /api/1/worlds/{world_id}`
  - Returns complete metadata for a single world
  - Includes all fields except file metadata

- **File Metadata**: `GET /api/1/file/{file_id}`
  - Returns version history, download sizes, and MD5 hashes
  - Covers both world images and unity packages
  - Multiple versions per file possible
  - Example: `https://vrchat.com/api/1/file/file_447b6078-e5fb-488c-bff5-432d4631f6cf`

- **Images**: Direct URLs from `imageUrl` and `thumbnailImageUrl` fields
  - Typically 800x600 PNG files
  - No authentication required for public world images

### Authentication

- Uses browser auth cookie (`VRCHAT_AUTH_COOKIE` environment variable)
- Cookie expires after a year it seems, so manual refresh is easiest.
- System monitors 401 responses and circuit breaks on auth failures

## Database Schema

### Tables

1. **worlds**
   ```sql
   CREATE TABLE worlds (
     world_id VARCHAR(64) PRIMARY KEY,
     metadata JSON NOT NULL,
     publish_date DATETIME,
     update_date DATETIME,
     last_scrape_time DATETIME NOT NULL,
     scrape_status ENUM('PENDING', 'SUCCESS', 'DELETED')
   );
   ```

2. **world_metrics**
   ```sql
   CREATE TABLE world_metrics (
     world_id VARCHAR(64),
     scrape_time DATETIME,
     favorites INT,
     heat INT,
     popularity INT,
     occupants INT,
     private_occupants INT,
     public_occupants INT,
     visits INT,
     PRIMARY KEY (world_id, scrape_time)
   );
   ```

3. **file_metadata**
   ```sql
   CREATE TABLE file_metadata (
     file_id VARCHAR(64) PRIMARY KEY,  -- VRChat file UUID
     world_id VARCHAR(64) NOT NULL,
     file_type ENUM('IMAGE', 'UNITY_PACKAGE') NOT NULL,
     version_number INT NOT NULL,  -- Version parsed from world metadata URL
     scrape_status ENUM('PENDING', 'SUCCESS', 'ERROR') NOT NULL,
     metadata JSON,  -- Raw response from VRChat file API
     last_scrape_time DATETIME,
     error_message TEXT,
     FOREIGN KEY (world_id) REFERENCES worlds(world_id)
   );
   ```

4. **image_content**
   ```sql
   CREATE TABLE image_content (
     file_id VARCHAR(64) NOT NULL,  -- References file_metadata.file_id
     version INT NOT NULL,  -- Version from file_metadata
     filename VARCHAR(255) NOT NULL,  -- Original filename from VRChat
     md5 VARCHAR(32) NOT NULL,  -- MD5 from VRChat file metadata
     size_bytes INT NOT NULL,  -- Size from VRChat file metadata
     sha256 VARCHAR(64),  -- SHA256 of downloaded content (content addressing)
     state ENUM('PENDING', 'CONFIRMED', 'ERROR') NOT NULL,
     last_attempt_time DATETIME,
     success_time DATETIME,
     error_message TEXT,
     PRIMARY KEY (file_id, version),
     FOREIGN KEY (file_id) REFERENCES file_metadata(file_id)
   );
   ```

### Design Decisions

- **JSON Storage**: Store raw API responses in JSON columns and separately extract metrics
- **Ephemeral Fields**: Extract time-varying fields (favorites, heat, popularity, occupants, visits) into separate metrics table
- **Two-Phase File Handling**: File metadata scraped separately from world metadata, enabling distributed processing
- **Transactional Consistency**: World metadata and associated file_metadata rows updated atomically
- **VRChat File System**: Leverage VRChat's existing file UUIDs, MD5 hashes, and size metadata
- **Content-Addressed Storage**: Use SHA256 hashes for filesystem storage, enabling deduplication and portability
- **Dual Hash Verification**: VRChat's MD5 for integrity verification, SHA256 for content addressing
- **Selective Downloads**: Download images for viewing, collect metadata for unity packages without downloading
- **Database Coordination**: Multiple scraper instances coordinate through shared database state
- **Version Optimization**: Track file versions to detect unchanged files and skip redundant scrapes
- **Simple Schema**: Primary keys only, full table scans sufficient at current scale
- **Append-Only Metrics**: Every scrape appends to metrics table, even if values unchanged

## Rate Limiting Strategy

### Separated Components

The scraper uses two separate components for request control:

1. **BBRRateLimiter**: Discovers and maintains optimal request rate
   - Tracks delivery rate and request latency in sliding windows
   - Implements probe cycles to discover increased capacity
   - Uses pacing gains (cruising: 1.0, probing_up: 1.25, probing_down: 0.9)
   - Operates at 1-10 RPS scale with 10-second windows

2. **CircuitBreaker**: Handles catastrophic failure scenarios
   - Monitors error rates and activates on high failure rates
   - Exponential backoff on repeated failures
   - Independent of rate limiting - focuses on service availability

3. **Key Differences from TCP BBR**:
   - No app_limited detection (unnecessary at our 1-10 RPS scale)
   - Probe cycles provide sufficient recovery from temporary rate degradation
   - Simpler than full BBR due to limited operational scale

## Scraping Strategy

### World Discovery

1. **Periodic Recent Scrape**: Query `/api/1/worlds?sort=updated` to find new/updated worlds
2. **Bootstrap Data**: Import ~237k existing world IDs from external dataset
3. **Future Extensions**: Could use random endpoint or search API if needed

### Multi-Phase Scraping

**Phase 1: World Metadata**
1. Scrape world metadata from `/api/1/worlds/{world_id}`
2. Parse file UUIDs from `imageUrl`, `thumbnailImageUrl`, and unity package URLs
3. In single transaction:
   - Update world metadata and metrics
   - Delete file_metadata rows no longer referenced by current world
   - Upsert file_metadata rows for all discovered files (status: PENDING)

**Phase 2: File Metadata Scraping**
1. Query for PENDING file_metadata rows
2. Scrape VRChat file metadata from `/api/1/file/{file_id}`
3. Update file_metadata with sizes, MD5 hashes, and version info
4. For IMAGE files: create/update image_content rows with PENDING state
5. Mark file_metadata as SUCCESS or ERROR with appropriate timestamps

**Phase 3: Image Downloads**
1. Query for PENDING image_content rows where file_metadata is SUCCESS
2. Download actual image files from VRChat's CDN
3. Verify downloaded content against VRChat's MD5 hash and compute SHA256
4. Store in content-addressed filesystem using SHA256 hash
5. Update image_content with CONFIRMED state, SHA256, and timestamps

### Rescrape Scheduling

Heuristic based on most recent metrics snapshot:

- **Active Worlds** (high heat/popularity or high occupants): Daily scrapes
- **Warm Worlds** (moderate heat/popularity): Weekly scrapes
- **Cold Worlds** (low but nonzero heat/popularity): Monthly scrapes
- **Dormant Worlds** (near-zero heat/popularity): Every ~6 months
- **Minimum Interval**: 24 hours between scrapes of same world

Scheduling uses the latest row in `world_metrics` (per world) to determine cadence and avoids high-frequency writes to the `worlds` table. Worlds without metrics history are eligible for immediate scrape.

### File and Image Handling

**File Discovery and Metadata:**
- Extract file UUIDs and versions from world metadata URLs using URL parsing
- Two file types: `IMAGE` (for viewing) and `UNITY_PACKAGE` (for size tracking)
- Version tracking enables skipping unchanged files when world metadata updates
- File metadata scraped separately from world metadata for parallelization
- VRChat provides authoritative MD5 hashes and sizes per file version

**Image Download Pipeline:**
1. File metadata scraped first to get VRChat's MD5 and size
2. Image content entries created for IMAGE type files with PENDING state
3. Image download triggered only for PENDING image_content rows
4. Downloaded content verified against VRChat's MD5 hash and SHA256 computed
5. Content-addressed storage: `/images/{sha256[0:2]}/{sha256[2:4]}/{sha256}.png`

**Status Tracking:**
- **file_metadata.scrape_status**:
  - `PENDING`: File discovered from world, metadata not scraped
  - `SUCCESS`: VRChat file metadata successfully retrieved
  - `ERROR`: File metadata scrape failed (retry eligible)
- **image_content.state**:
  - `PENDING`: File metadata available, image not downloaded
  - `CONFIRMED`: Image downloaded, MD5 verified, SHA256 computed and stored
  - `ERROR`: Download failed due to network/verification error (retry eligible)

**Coordination Benefits:**
- Multiple scraper instances work on different phases simultaneously
- Transactional consistency ensures world and file metadata stay synchronized
- Failed downloads don't block world metadata updates
- Unity package metadata collected without expensive downloads

## Error Handling

### World Metadata API Errors

- **429 Rate Limit**: Exponential backoff, reduce request rate
- **500 Server Error**: Treat similar to 429, retry later
- **401 Unauthorized**: Circuit break, alert for manual cookie refresh
- **404 Not Found**: Mark world as DELETED in database
- **Validation Errors**: Log warnings, store partial data, mark for retry

### File Metadata API Errors

- **404 Not Found**: File may be deleted, mark file_metadata as ERROR
- **Rate Limiting**: Apply same rate limiting as world metadata
- **Network Errors**: Mark file_metadata as ERROR with retry timestamps
- **Parse Errors**: Invalid file UUID in world URL, log and skip

### Image Download Errors

- **404 Not Found**: Mark download_status as NOT_FOUND (file deleted from CDN)
- **MD5 Mismatch**: Mark as ERROR, file may be corrupted or changed
- **Network Timeouts**: Mark as ERROR with exponential backoff for retry
- **Disk Full**: Circuit break image downloads, alert for disk space
- **Permission Errors**: Log error, mark as ERROR (don't retry)

### Transaction Consistency

- **World + File Metadata**: If world metadata succeeds but file_metadata transaction fails, retry entire world
- **Partial File Updates**: Failed individual file metadata scrapes don't rollback world metadata
- **Download Independence**: Image download failures don't affect metadata consistency

### Recovery Strategies

- **Retry Logic**: Exponential backoff based on error type and frequency
- **Circuit Breaking**: Per-endpoint circuit breakers for world, file metadata, and image APIs
- **Graceful Degradation**: Continue world metadata even if file handling fails
- **Monitoring**: Track error rates by phase (world, file metadata, downloads)

## Observability

### Metrics (via OpenTelemetry/Logfire)

**World Metadata Metrics:**
- World scrape rate and latencies
- Worlds scraped per hour/day
- World scrape queue depth (PENDING worlds)

**File Metadata Metrics:**
- File metadata scrape rate and latencies
- Files discovered per world (images + unity packages)
- File metadata queue depth (PENDING file_metadata rows)

**Image Download Metrics:**
- Image download rate and throughput (MB/s)
- Download success/failure rates by error type
- Image download queue depth (PENDING image_content rows)
- Disk usage for image storage

**Rate Limiting Metrics:**
- Rate limiter state per endpoint (world, file, image APIs)
- Circuit breaker activations by endpoint
- Request pacing gains and effective rates

**Data Quality Metrics:**
- MD5 verification success/failure rates
- File size distribution (images vs unity packages)
- World update frequency and patterns

### Logging

**Structured Context:**
- World IDs, file IDs, and file types in all log messages
- Request tracing across world → file metadata → image download phases
- Transaction boundaries and consistency events

**Key Events:**
- World metadata updates and file discovery
- File metadata scraping results and MD5 verification
- Image download attempts and verification failures
- Rate limit adjustments per endpoint
- Circuit breaker activations by API type
- Transaction rollbacks and retry scheduling

## Configuration

### Environment Variables

- `VRCHAT_AUTH_COOKIE`: Authentication cookie from browser
- `DATABASE_URL`: MySQL connection string for DoltDB
- `IMAGE_STORAGE_PATH`: Directory for storing images
- `LOG_LEVEL`: Logging verbosity

### Tuneable Parameters

- Rate limit probe frequency
- Circuit breaker thresholds
- Rescrape intervals by world category
- Request timeout values

## Testing Strategy

### Overview

Comprehensive test coverage validates rate limiter behavior, error handling, and integration flows without requiring actual VRChat API access. The test suite is organized into fast unit tests and slow integration tests.

### Test Organization

**Fast Unit Tests** (`tests/test_*.py`):
- Use stub rate limiters and circuit breakers for immediate responses
- In-memory SQLite database with SQLAlchemy async support
- Mock time sources and fake API clients
- Complete in under 1 second, suitable for TDD and CI

**Slow Integration Tests** (`tests/test_*_slow.py`):
- Real BBRRateLimiter and CircuitBreaker with fast parameters
- Delayed API responses via coroutines (50-500ms delays)
- Tests actual rate limiting behavior and probe cycles
- Marked with `@pytest.mark.slow`, can be excluded with `-m "not slow"`

### Rate Limiter Testing

**Implemented Test Coverage**:
- **Basic pacing**: Verifies request spacing based on delivery rate
- **Delivery rate sampling**: Tests BBR's multi-request sampling logic
- **Error handling**: Circuit breaker activation and rate reduction
- **State machine**: Probe cycle transitions (CRUISING → PROBING_UP → PROBING_DOWN)
- **Window filters**: Sliding window max/min tracking with sample expiration
- **Probe recovery**: Recovery from degraded rates through bandwidth probing

**Key Test Scenarios**:
```python
# Probe recovery test shows rate limiter discovering higher capacity
Initial max_rate: 1.00 req/s  # Degraded state
Final max_rate: 34.04 req/s   # Discovered through probing
Observed rate: 6.81 req/s     # Actual achieved throughput
```

### Database Testing

**SQLAlchemy with In-Memory SQLite**:
- Async session management with proper cleanup
- Transaction isolation between tests
- Schema identical to production DoltDB
- Direct SQL table inspection for state verification

**Test Coverage**:
- World state transitions (PENDING → SUCCESS → DELETED)
- Metrics time-series storage (append-only behavior)
- Upsert operations and conflict resolution
- Database schema initialization

### Scraper Integration Testing

**Comprehensive Error Handling**:
- Authentication failures (401) → shutdown behavior
- Rate limit responses (429) → circuit breaker activation
- Server errors (500, 502, 503) → retry logic
- Network timeouts → backoff behavior
- World deletion (404) → status marking
- Image download failures → graceful degradation

**Integration Flow Testing**:
- Recent worlds discovery with rate limiting
- Batch processing with concurrent world scraping
- Configurable timing parameters for testing
- Real rate limiting delays (2-4 seconds per test)

### Testing Tools

**Current Stack**:
- **pytest-asyncio**: Async test fixtures and execution
- **Custom fakes**: FakeVRChatAPIClient, FakeImageDownloader with future-based delays
- **MockTime/MockAsyncSleep**: Deterministic time control for unit tests
- **Real asyncio.sleep**: Actual delays for integration tests
- **Coverage tracking**: 76% coverage on scraper core logic

### Performance Validation

**Slow Integration Tests**:
- Verify rate limiting actually delays requests (2+ seconds for rate-limited batches)
- Test probe cycle discovery with 3+ batches over multiple probe cycles
- Validate concurrent processing with realistic API latency (50-500ms per request)
- Confirm idle behavior when no pending worlds available

## Dependencies

### Core Libraries

- **httpx**: Async HTTP client with connection pooling
- **pydantic**: Data validation and settings management
- **sqlalchemy[asyncio]**: Async ORM with MySQL support via aiomysql
- **aiosqlite**: SQLite support for testing
- **pytest**: Testing framework with asyncio support
- **logfire**: OpenTelemetry-based observability

### Why These Choices

- **SQLAlchemy**: Provides async support and enables in-memory SQLite testing
- **No OpenAPI Client**: Overhead not worth it for 4 endpoints
- **Asyncio**: Natural fit for I/O-bound scraping workload
- **Separated Rate Limiting**: Independent components for rate discovery vs failure handling

## Future Considerations

### Scalability

- Current design handles expected load (250 new worlds/day)
- Multiple scraper instances coordinate seamlessly via database state
- Image downloads can be distributed across clients without conflicts
- Could partition world ID space for multiple scrapers if needed
- DoltDB merge capabilities enable distributed scraping

### Data Export

- DoltDB automatic commits preserve history
- Separate process can push database dumps to cloud storage
- Git-like model enables efficient delta synchronization

### API Changes

- JSON storage provides flexibility for schema evolution
- Validation warnings help detect API changes early
- Ephemeral field list may need updates over time

### Image and File Optimization

**Content-Addressed Storage Benefits:**
- SHA256-based filesystem layout enables automatic deduplication of identical images
- Portable across different scraper instances and deployments
- Immutable storage - files never change once written
- Database state is mergeable between scrapers using DoltDB

**Dual Hash Strategy:**
- VRChat MD5 hashes used for integrity verification during download
- SHA256 computed locally for content addressing and deduplication
- File metadata provides authoritative sizes for storage planning
- Hash verification ensures data integrity and detects corruption

**Processing Optimization:**
- Separate file metadata and download phases enable selective processing
- Unity package metadata collected without expensive downloads
- CONFIRMED state indicates local filesystem contains verified content
- Version tracking enables intelligent re-download when files update
