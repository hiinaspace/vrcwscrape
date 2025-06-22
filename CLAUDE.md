# Claude Code Guide for VRChat Scraper

This guide helps Claude Code work effectively with the VRChat world metadata scraper project.

## Environment Setup

**Always use `uv` for Python commands:**
```bash
# Run any Python command
uv run python -m vrchat_scraper.main

# Run tests
uv run coverage run -m pytest
uv run coverage run -m pytest -v tests/test_rate_limiter.py  # Run specific test file
uv run coverage run -m pytest -k "test_circuit_breaker"       # Run tests matching pattern

# get coverage report from unit test run
uv run coverage report -m

# Add new dependencies
uv add <package>
uv add --dev <dev package>
```

## Code Quality

**Before committing, always run:**
```bash
# Format code
uv run ruff format src/ tests/

# Fix linting issues
uv run ruff check --fix src/ tests/

# Run tests
uv run pytest
```

## Project Context

### Authentication
- VRChat auth cookie format: `authcookie_xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`
- Obtained from browser DevTools Console after logging into VRChat website
- Set as `VRCHAT_AUTH_COOKIE` environment variable
- Cookie expires after weeks/months - monitor for 401 errors

### Database
- Using DoltDB (MySQL-compatible with Git-like versioning)
- Connection via standard MySQL protocol on port 13306 (mapped from container's 3306)
- SQLAlchemy async ORM with aiomysql for production, aiosqlite for testing
- DoltDB automatically commits periodically (configure on server)

### Data Scale
- ~237,000 existing worlds to import initially
- ~250 new worlds created daily
- World IDs format: `wrld_xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`
- Images are 800x600 PNGs, roughly 100-500KB each

### Rate Limiting
- VRChat's actual limit is ~10 requests/second (estimated)
- Be conservative to avoid bans - start at 10 req/s, back off on errors
- Use BBR-inspired algorithm to discover safe rate dynamically
- Circuit breaker with exponential backoff on high error rates

### Design Philosophy
- Single Python process with asyncio
- Simple and maintainable over complex
- Let transient errors self-heal through rescrape logic
- Observability via OpenTelemetry/Logfire
- Graceful shutdown on SIGTERM/SIGINT

## Common Development Tasks

### Running Locally

1. **Start DoltDB:**
   ```bash
   docker-compose up -d
   ```

2. **Set up environment:**
   ```bash
   cp .env.example .env
   # Edit .env with your auth cookie and database URL
   ```

3. **Run the scraper:**
   ```bash
   uv run python -m vrchat_scraper.main
   ```

### Testing Changes

1. **Test rate limiter behavior:**
   ```bash
   uv run pytest tests/test_rate_limiter.py -v
   ```
   The mock API in tests simulates VRChat's leaky bucket behavior.

2. **Test database operations:**
   ```bash
   uv run pytest tests/test_database.py -v
   ```
   Uses SQLite in-memory or MySQL rollback for isolation.

3. **Test full integration:**
   ```bash
   uv run pytest tests/test_integration.py -v
   ```

### Debugging

- Check Logfire dashboard for traces and metrics
- Database state: `SELECT * FROM worlds WHERE scrape_status = 'PENDING' LIMIT 10;`
- Rate limiter state is logged at INFO level
- Circuit breaker activations logged at WARNING level

### Importing Existing Data

One-time bootstrap script (create separately):
```python
# import_worlds.py
import asyncio
from vrchat_scraper.database import Database
from vrchat_scraper.config import Config

async def import_worlds():
    config = Config()
    db = Database(config.database_url)

    with open('existing_world_ids.txt') as f:
        for line in f:
            world_id = line.strip()
            if world_id.startswith('wrld_'):
                await db.upsert_world(
                    world_id,
                    {"imported_at": datetime.utcnow().isoformat()},
                    status="PENDING"
                )

asyncio.run(import_worlds())
```

## API Response Examples

**World Summary (from recent endpoint):**
```json
{
  "id": "wrld_xxx",
  "name": "World Name",
  "authorId": "usr_xxx",
  "imageUrl": "https://api.vrchat.cloud/api/1/file/file_xxx/1/file",
  "occupants": 5,
  "favorites": 100
}
```

**World Detail (individual world):**
```json
{
  "id": "wrld_xxx",
  "name": "World Name",
  "description": "Description",
  "capacity": 32,
  "favorites": 100,
  "visits": 1000,
  "heat": 3,
  "popularity": 5,
  "occupants": 5,
  "tags": ["system_approved"],
  "created_at": "2024-01-01T00:00:00.000Z",
  "updated_at": "2024-01-02T00:00:00.000Z"
}
```

## Monitoring and Operations

### Key Metrics to Watch
- Request rate (should stabilize near 10 req/s)
- Error rate (should be < 1% normally)
- Worlds scraped per hour
- Queue depth (pending worlds)
- Circuit breaker activations

### Common Issues

1. **Auth cookie expired**: 401 errors, need manual refresh
2. **Rate limit hit**: 429 errors, algorithm should adapt
3. **Database connection lost**: Will error, consider retry logic
4. **Disk full**: Monitor image storage directory size

### Maintenance Tasks

- Periodically push DoltDB dumps to cloud storage
- Monitor disk usage for images (237k * ~300KB ≈ 70GB)
- Check for VRChat API changes (new fields, deprecated endpoints)
- Review logs for validation errors indicating schema changes

## Future Enhancements (Not Yet Implemented)

- File metadata scraping (world download sizes)
- Search API for finding missing worlds
- Distributed scraping with DoltDB merge
- Image deduplication (some worlds share images)
- Webhook notifications for auth failures

## Remember

- This is an archival project - completeness matters
- Be respectful of VRChat's API - conservative rate limiting
- Test thoroughly, especially the rate limiter
- Keep it simple - resist adding unnecessary complexity
