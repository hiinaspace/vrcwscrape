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

### BBR-Inspired Algorithm

The scraper implements a congestion control algorithm inspired by TCP BBR:

1. **Tracking**:
   - Moving window of successful request rate (past few minutes)
   - Error rates for 429s, 500s, and timeouts
   - Current pacing gain (multiplier for request rate)

2. **Decision Logic**:
   ```
   if error_rate > threshold:
     circuit_break_with_exponential_backoff()
   elif error_rate > 0:
     decrease_pacing_gain()
   else:
     if stable_for_duration:
       probe_bandwidth()  # temporarily increase pacing_gain to 1.2
   ```

3. **Implementation Details**:
   - Base rate limit estimate: ~10 requests/second
   - Max concurrent requests: 10 (via httpx limits)
   - Normal pacing gain: 1.0
   - Probe pacing gain: 1.2
   - Circuit breaker threshold: configurable error rate

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

Comprehensive testing is critical for validating the rate limiter behavior and ensuring robust operation. The test suite should exercise all code paths without requiring actual VRChat API access.

### Rate Limiter Testing

**Mock VRChat API**: Implement a fake API server that simulates VRChat's rate limiting behavior:

```python
class MockVRChatAPI:
    """Simulates a leaky bucket rate limiter with variable limits"""
    def __init__(self, initial_limit=10.0, bucket_size=100):
        self.rate_limit = initial_limit
        self.bucket = bucket_size
        self.last_request = time.time()
        
    async def handle_request(self):
        # Refill bucket based on time elapsed
        # Return 429 if bucket empty
        # Randomly return 500s to test error handling
        
    def change_rate_limit(self, new_limit):
        """Simulate dynamic rate limit changes"""
        self.rate_limit = new_limit
```

Test scenarios:
- Stable rate limit: Verify scraper finds and maintains optimal rate
- Decreasing limit: Verify scraper backs off appropriately
- Increasing limit: Verify bandwidth probing discovers new capacity
- Error bursts: Verify circuit breaker activates and recovers
- Mixed errors: Verify different error types handled correctly

### Database Testing

**In-Memory Testing**: Use transaction rollbacks or an in-memory SQLite database for fast tests:

```python
@pytest.fixture
async def test_db():
    # Option 1: Use SQLite with similar schema
    # Option 2: Use real MySQL/DoltDB with rollback
    # Option 3: Create simple mock at Database class level
```

Test scenarios:
- World state transitions (PENDING → SUCCESS → DELETED)
- Metrics append-only behavior
- Rescrape scheduling logic with various world ages
- Concurrent access patterns

### Scraper Integration Tests

Test the full scraping flow with mock HTTP responses:

```python
@pytest.fixture
def mock_vrchat_responses():
    return {
        "/api/1/worlds?sort=updated": load_fixture("recent_worlds.json"),
        "/api/1/worlds/wrld_123": load_fixture("world_detail.json"),
        # Add 404, 429, 500 responses
    }
```

### Async Testing Tools

- **pytest-asyncio**: Provides async test support and fixtures
- **aioresponses**: Mock aiohttp/httpx requests
- **freezegun** or **time-machine**: Control time in tests for rate limiting logic
- **asyncio.sleep** mocking: Speed up tests by patching sleep calls

Example test structure:

```python
@pytest.mark.asyncio
async def test_rate_limiter_finds_optimal_rate(mock_time):
    api = MockVRChatAPI(initial_limit=5.0)
    limiter = RateLimiter()
    
    # Simulate successful requests
    for _ in range(100):
        await limiter.acquire()
        api.handle_request()
        limiter.record_result(success=True)
        mock_time.advance(0.1)  # Simulate time passing
    
    # Verify converged to ~5 req/s
    assert 4.5 <= limiter.effective_rate <= 5.5
```

### Performance Testing

While not part of regular CI, performance tests ensure the scraper can handle expected load:

- Verify 250 worlds/day scraping capacity
- Measure memory usage with 237k world IDs in queue
- Test database query performance at scale

## Dependencies

### Core Libraries

- **httpx**: Async HTTP client with connection pooling
- **pydantic**: Data validation and settings management
- **mysql-connector-python**: Database connectivity
- **pytest**: Testing framework
- **logfire**: OpenTelemetry-based observability

### Why These Choices

- **No ORM**: Direct SQL for simplicity with small schema
- **No OpenAPI Client**: Overhead not worth it for 4 endpoints
- **Asyncio**: Natural fit for I/O-bound scraping workload

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
