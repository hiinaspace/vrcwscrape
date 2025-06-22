# VRChat World Metadata Scraper Design

## Overview

This project maintains a copy of VRChat's world metadata database by periodically scraping their HTTP API. The data is stored in DoltDB (a Git-like MySQL-compatible database) to enable versioned data access and collaborative scraping. 

Primary goals:
1. Enable better search interfaces than VRChat's native search capabilities
2. Provide archival preservation of world metadata in case VRChat shuts down
3. Allow researchers and developers to analyze trends in virtual world creation

## System Architecture

### Core Components

1. **Scraper Service** - Single Python asyncio process that:
   - Discovers new worlds via the "recently updated" API endpoint
   - Fetches complete metadata for individual worlds
   - Downloads world thumbnail images
   - Manages rate limiting to avoid API bans
   - Stores data in DoltDB

2. **Database** - DoltDB instance storing:
   - World metadata (mostly raw JSON responses)
   - Time-series metrics (ephemeral fields like visitor counts)
   - Scrape state and scheduling metadata

3. **File Storage** - Local filesystem storing:
   - World thumbnail images (800x600 PNGs)
   - Organized in subdirectories to avoid filesystem limitations

### Data Flow

```
VRChat API → Rate Limiter → Scraper → DoltDB
                              ↓
                         Image Storage
```

## API Integration

### Endpoints Used

- **Recent Worlds**: `GET /api/1/worlds?sort=updated`
  - Returns array of recently updated worlds (up to 1000)
  - Used hourly to discover new/updated worlds
  
- **World Details**: `GET /api/1/worlds/{world_id}`
  - Returns complete metadata for a single world
  - Includes all fields except file metadata
  
- **File Metadata**: `GET /api/1/file/{file_id}`
  - Returns version history and download sizes
  - Multiple versions per world possible
  
- **Images**: Direct URLs from `imageUrl` and `thumbnailImageUrl` fields
  - Typically 800x600 PNG files
  - No authentication required for public world images

### Authentication

- Uses browser auth cookie (`VRCHAT_AUTH_COOKIE` environment variable)
- Cookie expires after weeks/months
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
     file_id VARCHAR(64) PRIMARY KEY,
     world_id VARCHAR(64),
     metadata JSON NOT NULL,
     last_scrape_time DATETIME NOT NULL
   );
   ```

### Design Decisions

- **JSON Storage**: Store mostly-raw API responses in JSON columns, removing only ephemeral fields
- **Ephemeral Fields**: Extract time-varying fields (favorites, heat, popularity, occupants, visits) into separate metrics table
- **No Complex Indices**: Only primary keys needed; dataset small enough to load all world IDs into memory
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

1. **Hourly Recent Scrape**: Query `/api/1/worlds?sort=updated` to find new/updated worlds
2. **Bootstrap Data**: Import ~237k existing world IDs from external dataset
3. **Future Extensions**: Could use random endpoint or search API if needed

### Rescrape Scheduling

Heuristic based on world age and activity:

- **New Worlds** (< 1 week old): Daily scrapes
- **Recent Worlds** (1-4 weeks): Weekly scrapes  
- **Active Worlds** (< 1 year, regular visitors): Monthly scrapes
- **Inactive Worlds** (> 1 year, few visitors): Yearly scrapes
- **Minimum Interval**: 24 hours between scrapes of same world

### Image Handling

- **Decoupled from Metadata**: Separate process detects missing images
- **404 Handling**: Write 0-byte file to indicate intentional missing image
- **Storage Layout**: `/images/{uuid[0:2]}/{uuid[2:4]}/{world_id}.png`

## Error Handling

### API Errors

- **429 Rate Limit**: Exponential backoff, reduce request rate
- **500 Server Error**: Treat similar to 429
- **401 Unauthorized**: Circuit break, alert for manual cookie refresh
- **404 Not Found**: Mark world as DELETED in database

### Network Errors

- **Timeouts**: Count toward error rate, retry with backoff
- **Connection Errors**: Similar to timeouts

### Data Validation

- **Pydantic Models**: Validate API responses
- **Unknown Fields**: Log warnings but still store in JSON
- **Missing Required Fields**: Mark as ERROR, retry later

## Observability

### Metrics (via OpenTelemetry/Logfire)

- Request rates and latencies
- Error rates by type
- Scrape queue depth
- Worlds scraped per hour/day
- Rate limiter state (pacing gain, circuit breaker status)

### Logging

- Structured logging with world IDs
- Rate limit adjustments
- Circuit breaker activations
- Validation errors

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
