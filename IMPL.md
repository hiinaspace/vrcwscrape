# VRChat Scraper Implementation Guide

This document provides tactical implementation guidance for building the VRChat world metadata scraper. It should be deleted after initial implementation.

## Project Structure

```
vrchat-scraper/
├── src/
│   ├── vrchat_scraper/
│   │   ├── __init__.py
│   │   ├── main.py              # Entry point, asyncio event loop
│   │   ├── models.py            # Pydantic models for API responses
│   │   ├── database.py          # Database connection and queries
│   │   ├── rate_limiter.py      # BBR-inspired rate limiting
│   │   ├── scraper.py           # Core scraping logic
│   │   ├── image_downloader.py  # Image fetching logic
│   │   └── config.py            # Configuration management
├── tests/
│   ├── __init__.py
│   ├── test_rate_limiter.py
│   ├── test_scraper.py
│   ├── test_database.py
│   ├── test_models.py
│   └── fixtures/
│       ├── recent_worlds.json
│       └── world_detail.json
├── pyproject.toml
├── docker-compose.yml       # For local DoltDB
├── .env.example
└── README.md
```

## Project Setup

```toml
# pyproject.toml
[project]
name = "vrchat-scraper"
version = "0.1.0"
dependencies = [
    "httpx>=0.27.0",
    "pydantic>=2.0",
    "mysql-connector-python>=8.0",
    "python-dotenv>=1.0",
    "logfire>=0.1.0",
]

[project.optional-dependencies]
test = [
    "pytest>=8.0",
    "pytest-asyncio>=0.23",
    "aioresponses>=0.7",
    "time-machine>=2.0",
    "pytest-mock>=3.0",
]

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"
```

Setup commands:
```bash
# Install uv if not already installed
curl -LsSf https://astral.sh/uv/install.sh | sh

# Create project and install dependencies
uv venv
uv pip install -e ".[test]"
```

## Core Components

### Configuration (`config.py`)
```python
from pydantic_settings import BaseSettings
from pydantic import Field

class Config(BaseSettings):
    """Application configuration from environment"""
    database_url: str = Field(..., description="MySQL connection string")
    vrchat_auth_cookie: str = Field(..., description="VRChat auth cookie")
    image_storage_path: str = Field("./images", description="Path to store images")
    log_level: str = Field("INFO", description="Logging level")
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
```

### Pydantic Models (`models.py`)
   ```python
   from pydantic import BaseModel, Field
   from datetime import datetime
   from typing import List, Optional, Dict, Any

   class WorldSummary(BaseModel):
       id: str
       name: str
       author_id: str = Field(alias="authorId")
       author_name: str = Field(alias="authorName")
       image_url: str = Field(alias="imageUrl")
       thumbnail_url: str = Field(alias="thumbnailImageUrl")
       updated_at: datetime
       
   class WorldDetail(BaseModel):
       id: str
       name: str
       description: Optional[str] = None
       author_id: str = Field(alias="authorId")
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
       # ... other fields
       
       def extract_metrics(self) -> Dict[str, int]:
           """Extract ephemeral metrics for separate storage"""
           return {
               "favorites": self.favorites,
               "heat": self.heat,
               "popularity": self.popularity,
               "occupants": self.occupants,
               "private_occupants": self.private_occupants,
               "public_occupants": self.public_occupants,
               "visits": self.visits
           }
           
       def stable_metadata(self) -> Dict[str, Any]:
           """Return metadata without ephemeral fields"""
           data = self.model_dump()
           for field in ["favorites", "heat", "popularity", "occupants", 
                        "private_occupants", "public_occupants", "visits"]:
               data.pop(field, None)
           return data
   ```

### Database Layer (`database.py`)
   ```python
   import mysql.connector
   from contextlib import asynccontextmanager
   import json
   from datetime import datetime, timedelta
   import random
   
   class Database:
       def __init__(self, connection_string: str):
           self.connection_params = self._parse_connection_string(connection_string)
           
       def _get_connection(self):
           """Create a new database connection"""
           return mysql.connector.connect(**self.connection_params)
           
       async def init_schema(self):
           """Create tables if they don't exist"""
           with self._get_connection() as conn:
               cursor = conn.cursor()
               # Create worlds table
               cursor.execute("""
                   CREATE TABLE IF NOT EXISTS worlds (
                       world_id VARCHAR(64) PRIMARY KEY,
                       metadata JSON NOT NULL,
                       publish_date DATETIME,
                       update_date DATETIME,
                       last_scrape_time DATETIME NOT NULL,
                       scrape_status ENUM('PENDING', 'SUCCESS', 'DELETED')
                   )
               """)
               # Create metrics table
               cursor.execute("""
                   CREATE TABLE IF NOT EXISTS world_metrics (
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
                   )
               """)
               conn.commit()
           
       async def upsert_world(self, world_id: str, metadata: dict, 
                             status: str = "SUCCESS"):
           """Insert or update world metadata"""
           with self._get_connection() as conn:
               cursor = conn.cursor()
               cursor.execute("""
                   INSERT INTO worlds (world_id, metadata, last_scrape_time, scrape_status)
                   VALUES (%s, %s, %s, %s)
                   ON DUPLICATE KEY UPDATE
                   metadata = VALUES(metadata),
                   last_scrape_time = VALUES(last_scrape_time),
                   scrape_status = VALUES(scrape_status)
               """, (world_id, json.dumps(metadata), datetime.utcnow(), status))
               conn.commit()
           
       async def get_worlds_to_scrape(self, limit: int = 100) -> List[str]:
           """Get world IDs that need scraping based on strategy"""
           with self._get_connection() as conn:
               cursor = conn.cursor()
               
               # First get PENDING worlds
               cursor.execute("""
                   SELECT world_id FROM worlds 
                   WHERE scrape_status = 'PENDING'
                   ORDER BY RAND()
                   LIMIT %s
               """, (limit,))
               
               world_ids = [row[0] for row in cursor.fetchall()]
               
               if len(world_ids) < limit:
                   # Get worlds that need rescraping based on age
                   remaining = limit - len(world_ids)
                   cursor.execute("""
                       SELECT world_id, 
                              TIMESTAMPDIFF(DAY, publish_date, NOW()) as age_days,
                              TIMESTAMPDIFF(HOUR, last_scrape_time, NOW()) as hours_since_scrape
                       FROM worlds
                       WHERE scrape_status = 'SUCCESS'
                       HAVING 
                           (age_days < 7 AND hours_since_scrape >= 24) OR
                           (age_days < 30 AND hours_since_scrape >= 168) OR
                           (age_days < 365 AND hours_since_scrape >= 720) OR
                           (hours_since_scrape >= 8760)
                       ORDER BY RAND()
                       LIMIT %s
                   """, (remaining,))
                   
                   world_ids.extend([row[0] for row in cursor.fetchall()])
               
               return world_ids
   ```

### Rate Limiter (`rate_limiter.py`)
   ```python
   import asyncio
   import time
   from collections import deque
   from dataclasses import dataclass
   from typing import Optional
   import logging
   
   logger = logging.getLogger(__name__)
   
   @dataclass
   class RequestResult:
       timestamp: float
       success: bool
       status_code: Optional[int] = None
   
   class RateLimiter:
       def __init__(self, 
                    base_rate: float = 10.0,  # requests per second
                    window_size: int = 120,    # seconds
                    error_threshold: float = 0.1,
                    probe_interval: float = 60.0):
           self.base_rate = base_rate
           self.window_size = window_size
           self.error_threshold = error_threshold
           self.probe_interval = probe_interval
           
           self.request_history = deque()
           self.pacing_gain = 1.0
           self.last_probe_time = time.time()
           self.circuit_breaker_until = 0
           self.backoff_seconds = 5  # Start with 5s backoff
           self.last_request_time = 0
           
       async def acquire(self):
           """Wait until it's safe to make a request"""
           now = time.time()
           
           # Check circuit breaker
           if now < self.circuit_breaker_until:
               wait_time = self.circuit_breaker_until - now
               logger.info(f"Circuit breaker active, waiting {wait_time:.1f}s")
               await asyncio.sleep(wait_time)
               now = time.time()
           
           # Clean old history
           cutoff = now - self.window_size
           while self.request_history and self.request_history[0].timestamp < cutoff:
               self.request_history.popleft()
           
           # Calculate delay based on current rate
           if self.last_request_time > 0:
               target_rate = self.base_rate * self.pacing_gain
               min_interval = 1.0 / target_rate if target_rate > 0 else 1.0
               elapsed = now - self.last_request_time
               
               if elapsed < min_interval:
                   wait_time = min_interval - elapsed
                   await asyncio.sleep(wait_time)
           
           self.last_request_time = time.time()
           
       def record_result(self, success: bool, status_code: Optional[int] = None):
           """Record the result of a request"""
           result = RequestResult(
               timestamp=time.time(),
               success=success,
               status_code=status_code
           )
           self.request_history.append(result)
           
           # Calculate error rate
           if len(self.request_history) >= 10:  # Need minimum samples
               recent_errors = sum(1 for r in self.request_history 
                                 if not r.success and 
                                 r.timestamp > time.time() - 30)  # Last 30s
               error_rate = recent_errors / len([r for r in self.request_history 
                                               if r.timestamp > time.time() - 30])
               
               if error_rate > self.error_threshold:
                   self._activate_circuit_breaker()
               elif error_rate > 0:
                   # Reduce rate on any errors
                   self.pacing_gain = max(0.5, self.pacing_gain * 0.9)
                   logger.info(f"Reduced pacing gain to {self.pacing_gain:.2f}")
               else:
                   # Consider probing for more bandwidth
                   if time.time() - self.last_probe_time > self.probe_interval:
                       self._probe_bandwidth()
           
           # Reset backoff on success
           if success:
               self.backoff_seconds = 5
                   
       def _activate_circuit_breaker(self):
           """Activate circuit breaker with exponential backoff"""
           self.circuit_breaker_until = time.time() + self.backoff_seconds
           logger.warning(f"Circuit breaker activated for {self.backoff_seconds}s")
           
           # Exponential backoff for next time
           self.backoff_seconds = min(self.backoff_seconds * 2, 300)  # Max 5 min
           
           # Reduce rate significantly
           self.pacing_gain = 0.5
           
       def _probe_bandwidth(self):
           """Temporarily increase rate to test limits"""
           if self.pacing_gain < 0.9:  # Only probe if we're not already near max
               return
               
           logger.info("Probing for additional bandwidth")
           self.pacing_gain = min(1.2, self.pacing_gain * 1.1)
           self.last_probe_time = time.time()
           
           # Schedule return to normal after probe
           asyncio.create_task(self._end_probe())
           
       async def _end_probe(self):
           """End bandwidth probe after test period"""
           await asyncio.sleep(10)  # Probe for 10 seconds
           
           # Check if probe was successful (no errors in probe period)
           probe_errors = sum(1 for r in self.request_history 
                            if not r.success and 
                            r.timestamp > self.last_probe_time)
           
           if probe_errors == 0:
               logger.info("Probe successful, keeping higher rate")
               self.pacing_gain = min(1.0, self.pacing_gain)  # Don't exceed 1.0
           else:
               logger.info("Probe hit errors, reverting rate")
               self.pacing_gain = 0.9
   ```

### Scraper (`scraper.py`)
   ```python
   import httpx
   from pathlib import Path
   from typing import List
   import logging
   from datetime import datetime
   
   logger = logging.getLogger(__name__)
   
   class AuthenticationError(Exception):
       """Raised when authentication fails"""
       pass
   
   class Scraper:
       def __init__(self, 
                    database: Database,
                    rate_limiter: RateLimiter,
                    auth_cookie: str,
                    image_storage_path: str):
           self.db = database
           self.rate_limiter = rate_limiter
           self.auth_cookie = auth_cookie
           self.image_path = Path(image_storage_path)
           
           self.client = httpx.AsyncClient(
               timeout=30.0,
               limits=httpx.Limits(max_connections=10),
               headers={"Cookie": f"auth={auth_cookie}"}
           )
           
       async def scrape_recent_worlds(self):
           """Fetch recently updated worlds and queue for scraping"""
           await self.rate_limiter.acquire()
           
           try:
               response = await self.client.get(
                   "https://api.vrchat.cloud/api/1/worlds",
                   params={"sort": "updated", "n": 1000}
               )
               response.raise_for_status()
               
               self.rate_limiter.record_result(True, response.status_code)
               
               worlds = [WorldSummary(**w) for w in response.json()]
               logger.info(f"Found {len(worlds)} recently updated worlds")
               
               # Queue all discovered worlds as PENDING
               for world in worlds:
                   await self.db.upsert_world(
                       world.id, 
                       {"discovered_at": datetime.utcnow().isoformat()},
                       status="PENDING"
                   )
                   
           except httpx.HTTPStatusError as e:
               self.rate_limiter.record_result(False, e.response.status_code)
               
               if e.response.status_code == 401:
                   logger.error("Authentication failed - cookie may be expired")
                   raise AuthenticationError("Cookie expired")
               else:
                   logger.warning(f"Recent worlds request failed: {e}")
                   
       async def scrape_world(self, world_id: str):
           """Scrape complete metadata for a single world"""
           await self.rate_limiter.acquire()
           
           try:
               response = await self.client.get(
                   f"https://api.vrchat.cloud/api/1/worlds/{world_id}"
               )
               response.raise_for_status()
               
               self.rate_limiter.record_result(True, response.status_code)
               
               world = WorldDetail(**response.json())
               
               # Store stable metadata
               await self.db.upsert_world(
                   world_id,
                   world.stable_metadata(),
                   status="SUCCESS"
               )
               
               # Store time-series metrics
               await self.db.insert_metrics(
                   world_id,
                   world.extract_metrics(),
                   datetime.utcnow()
               )
               
               logger.debug(f"Successfully scraped world {world_id}")
               
               # Queue image download if needed
               await self._queue_image_download(world)
               
           except httpx.HTTPStatusError as e:
               self.rate_limiter.record_result(False, e.response.status_code)
               
               if e.response.status_code == 404:
                   logger.info(f"World {world_id} not found (deleted?)")
                   await self.db.upsert_world(world_id, {}, status="DELETED")
               elif e.response.status_code == 401:
                   logger.error("Authentication failed during world scrape")
                   raise AuthenticationError("Cookie expired")
               else:
                   logger.warning(f"Failed to scrape world {world_id}: {e}")
                   
           except Exception as e:
               logger.error(f"Unexpected error scraping {world_id}: {e}")
               self.rate_limiter.record_result(False, None)
               
       async def _queue_image_download(self, world: WorldDetail):
           """Check if image exists, queue download if not"""
           image_path = self._get_image_path(world.id)
           
           if not image_path.exists():
               # Add to some queue or download immediately
               # For simplicity, could just download now
               try:
                   await self._download_image(world.image_url, image_path)
               except Exception as e:
                   logger.warning(f"Failed to download image for {world.id}: {e}")
                   
       def _get_image_path(self, world_id: str) -> Path:
           """Generate hierarchical path for image storage"""
           # world_id format: wrld_xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
           uuid_part = world_id[5:]  # Remove 'wrld_' prefix
           return self.image_path / uuid_part[0:2] / uuid_part[2:4] / f"{world_id}.png"
           
       async def _download_image(self, url: str, path: Path):
           """Download image from URL to path"""
           path.parent.mkdir(parents=True, exist_ok=True)
           
           # Don't count image downloads against API rate limit
           response = await self.client.get(url)
           
           if response.status_code == 404:
               # Write 0-byte file to indicate intentional missing
               path.touch()
           else:
               response.raise_for_status()
               path.write_bytes(response.content)
       
       async def close(self):
           """Clean up resources"""
           await self.client.aclose()
   ```

### Main Entry Point (`main.py`)
   ```python
   import asyncio
   import logging
   import signal
   from datetime import datetime
   import logfire
   
   from .config import Config
   from .database import Database
   from .rate_limiter import RateLimiter
   from .scraper import Scraper, AuthenticationError
   
   # Configure logging
   logging.basicConfig(
       level=logging.INFO,
       format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
   )
   logger = logging.getLogger(__name__)
   
   # Configure observability
   logfire.configure()
   
   class GracefulShutdown:
       def __init__(self):
           self.shutdown_event = asyncio.Event()
           
       def handle_signal(self):
           logger.info("Shutdown signal received")
           self.shutdown_event.set()
   
   async def main():
       # Load config
       config = Config()
       
       # Setup graceful shutdown
       shutdown = GracefulShutdown()
       loop = asyncio.get_event_loop()
       for sig in (signal.SIGTERM, signal.SIGINT):
           loop.add_signal_handler(sig, shutdown.handle_signal)
       
       # Initialize components
       db = Database(config.database_url)
       await db.init_schema()
       
       rate_limiter = RateLimiter()
       scraper = Scraper(db, rate_limiter, config.auth_cookie, 
                        config.image_storage_path)
       
       try:
           # Schedule periodic tasks
           async def recent_worlds_task():
               while not shutdown.shutdown_event.is_set():
                   try:
                       with logfire.span("scrape_recent_worlds"):
                           await scraper.scrape_recent_worlds()
                   except AuthenticationError:
                       logger.critical("Authentication failed - exiting")
                       shutdown.shutdown_event.set()
                       break
                   except Exception as e:
                       logger.error(f"Recent worlds scrape failed: {e}")
                   
                   # Wait for next run or shutdown
                   try:
                       await asyncio.wait_for(
                           shutdown.shutdown_event.wait(), 
                           timeout=3600  # 1 hour
                       )
                   except asyncio.TimeoutError:
                       pass  # Continue to next iteration
                       
           async def scrape_pending_task():
               while not shutdown.shutdown_event.is_set():
                   try:
                       world_ids = await db.get_worlds_to_scrape(limit=100)
                       
                       if not world_ids:
                           # No worlds to scrape, wait a bit
                           await asyncio.sleep(60)
                           continue
                       
                       for world_id in world_ids:
                           if shutdown.shutdown_event.is_set():
                               break
                               
                           try:
                               with logfire.span("scrape_world", world_id=world_id):
                                   await scraper.scrape_world(world_id)
                           except AuthenticationError:
                               logger.critical("Authentication failed - exiting")
                               shutdown.shutdown_event.set()
                               break
                           except Exception as e:
                               logger.error(f"Failed to scrape {world_id}: {e}")
                               
                   except Exception as e:
                       logger.error(f"Scraping task error: {e}")
                       await asyncio.sleep(60)  # Back off on errors
                       
           # Run all tasks concurrently
           tasks = [
               asyncio.create_task(recent_worlds_task()),
               asyncio.create_task(scrape_pending_task()),
           ]
           
           # Wait for shutdown
           await shutdown.shutdown_event.wait()
           logger.info("Shutting down gracefully...")
           
           # Cancel all tasks
           for task in tasks:
               task.cancel()
           
           # Wait for tasks to complete
           await asyncio.gather(*tasks, return_exceptions=True)
           
       finally:
           # Cleanup
           await scraper.close()
           logger.info("Shutdown complete")
   
   if __name__ == "__main__":
       asyncio.run(main())
   ```

## Testing Strategy

### Rate Limiter Tests (`test_rate_limiter.py`)

```python
import pytest
import asyncio
import time
from unittest.mock import Mock, patch
import time_machine

from vrchat_scraper.rate_limiter import RateLimiter

class MockVRChatAPI:
    """Simulates VRChat's rate limiting behavior"""
    def __init__(self, rate_limit=10.0, bucket_size=100):
        self.rate_limit = rate_limit
        self.bucket_size = bucket_size
        self.bucket = bucket_size
        self.last_refill = time.time()
        self.request_count = 0
        
    def handle_request(self) -> tuple[bool, int]:
        """Returns (success, status_code)"""
        now = time.time()
        
        # Refill bucket
        elapsed = now - self.last_refill
        refill = elapsed * self.rate_limit
        self.bucket = min(self.bucket_size, self.bucket + refill)
        self.last_refill = now
        
        self.request_count += 1
        
        # Random 500 errors (1% chance)
        if self.request_count % 100 == 0:
            return False, 500
        
        # Check rate limit
        if self.bucket >= 1:
            self.bucket -= 1
            return True, 200
        else:
            return False, 429
            
    def change_rate_limit(self, new_limit: float):
        """Simulate dynamic rate limit changes"""
        self.rate_limit = new_limit
        self.bucket_size = new_limit * 10

@pytest.mark.asyncio
async def test_rate_limiter_finds_stable_rate():
    """Test that rate limiter converges to API's actual rate"""
    api = MockVRChatAPI(rate_limit=5.0)
    limiter = RateLimiter(base_rate=10.0)  # Start higher than actual
    
    success_count = 0
    error_count = 0
    
    # Run for simulated 30 seconds
    with time_machine.travel(0) as traveler:
        start_time = time.time()
        
        while time.time() - start_time < 30:
            await limiter.acquire()
            success, status = api.handle_request()
            limiter.record_result(success, status)
            
            if success:
                success_count += 1
            else:
                error_count += 1
                
            # Advance time slightly
            traveler.shift(0.1)
    
    # Should converge close to 5 req/s
    actual_rate = success_count / 30
    assert 4.0 <= actual_rate <= 5.5
    assert error_count < success_count * 0.1  # Low error rate

@pytest.mark.asyncio
async def test_circuit_breaker_activation():
    """Test circuit breaker activates on high error rate"""
    limiter = RateLimiter(error_threshold=0.1)
    
    # Record many failures
    for _ in range(20):
        limiter.record_result(False, 429)
    
    # Circuit breaker should be active
    assert limiter.circuit_breaker_until > time.time()
    
    # Should delay next request
    start = time.time()
    await limiter.acquire()
    elapsed = time.time() - start
    assert elapsed >= 4.0  # Should wait at least backoff time

@pytest.mark.asyncio
async def test_bandwidth_probing():
    """Test that limiter probes for additional bandwidth"""
    limiter = RateLimiter(probe_interval=5.0)
    
    # Simulate successful requests to trigger probe
    with time_machine.travel(0) as traveler:
        for _ in range(50):
            await limiter.acquire()
            limiter.record_result(True, 200)
            traveler.shift(0.1)
        
        # After probe interval, gain should increase
        assert limiter.pacing_gain > 1.0
        
        # Probe should end after timeout
        traveler.shift(15)
        await asyncio.sleep(0)  # Let async tasks run
        assert limiter.pacing_gain <= 1.0

@pytest.mark.asyncio 
async def test_decreasing_rate_limit():
    """Test adaptation to decreasing API rate limit"""
    api = MockVRChatAPI(rate_limit=10.0)
    limiter = RateLimiter(base_rate=10.0)
    
    with time_machine.travel(0) as traveler:
        # Establish baseline
        for _ in range(100):
            await limiter.acquire()
            success, status = api.handle_request()
            limiter.record_result(success, status)
            traveler.shift(0.1)
        
        # Decrease API limit
        api.change_rate_limit(5.0)
        
        # Limiter should adapt
        errors_before_adapt = 0
        for _ in range(200):
            await limiter.acquire()
            success, status = api.handle_request()
            limiter.record_result(success, status)
            
            if not success and _ < 50:
                errors_before_adapt += 1
                
            traveler.shift(0.1)
        
        # Should see some errors initially, then adapt
        assert errors_before_adapt > 0
        assert limiter.pacing_gain < 0.9  # Should have reduced rate
```

### Database Tests (`test_database.py`)

```python
import pytest
from datetime import datetime, timedelta
from vrchat_scraper.database import Database

@pytest.fixture
async def test_db():
    """Create test database"""
    # Option 1: Use in-memory SQLite
    db = Database("sqlite:///:memory:")
    await db.init_schema()
    yield db
    
    # Option 2: Use real MySQL with transaction rollback
    # db = Database("mysql://test:test@localhost/test")
    # await db.init_schema()
    # conn = db._get_connection()
    # conn.start_transaction()
    # yield db
    # conn.rollback()

@pytest.mark.asyncio
async def test_world_state_transitions(test_db):
    """Test world moves through states correctly"""
    world_id = "wrld_test123"
    
    # Insert as PENDING
    await test_db.upsert_world(world_id, {"test": True}, "PENDING")
    
    # Verify appears in scrape queue
    to_scrape = await test_db.get_worlds_to_scrape(limit=10)
    assert world_id in to_scrape
    
    # Update to SUCCESS
    await test_db.upsert_world(world_id, {"name": "Test World"}, "SUCCESS")
    
    # Should not appear immediately (24h minimum)
    to_scrape = await test_db.get_worlds_to_scrape(limit=10)
    assert world_id not in to_scrape

@pytest.mark.asyncio
async def test_metrics_append_only(test_db):
    """Test metrics are appended, not updated"""
    world_id = "wrld_test123"
    
    # Insert metrics at different times
    metrics1 = {"favorites": 10, "visits": 100}
    metrics2 = {"favorites": 15, "visits": 150}
    
    await test_db.insert_metrics(world_id, metrics1, datetime.utcnow())
    await test_db.insert_metrics(world_id, metrics2, 
                                datetime.utcnow() + timedelta(hours=1))
    
    # Verify both entries exist
    metrics = await test_db.get_world_metrics(world_id)
    assert len(metrics) == 2
    assert metrics[0]["favorites"] == 10
    assert metrics[1]["favorites"] == 15
```

### Scraper Tests (`test_scraper.py`)

```python
import pytest
import httpx
from unittest.mock import Mock, AsyncMock, patch
import aioresponses

from vrchat_scraper.scraper import Scraper
from vrchat_scraper.models import WorldDetail

@pytest.fixture
def mock_responses():
    """Mock HTTP responses"""
    with aioresponses.aioresponses() as m:
        yield m

@pytest.mark.asyncio
async def test_scrape_world_success(mock_responses):
    """Test successful world scrape"""
    world_id = "wrld_test123"
    mock_db = AsyncMock()
    mock_limiter = AsyncMock()
    
    # Mock API response
    mock_responses.get(
        f"https://api.vrchat.cloud/api/1/worlds/{world_id}",
        payload={
            "id": world_id,
            "name": "Test World",
            "authorId": "usr_test",
            "favorites": 100,
            "visits": 1000,
            # ... other required fields
        }
    )
    
    scraper = Scraper(mock_db, mock_limiter, "test_cookie", "/tmp/images")
    await scraper.scrape_world(world_id)
    
    # Verify database calls
    mock_db.upsert_world.assert_called_once()
    mock_db.insert_metrics.assert_called_once()
    
    # Verify rate limiter interaction
    mock_limiter.acquire.assert_called_once()
    mock_limiter.record_result.assert_called_with(True, 200)

@pytest.mark.asyncio
async def test_handle_deleted_world(mock_responses):
    """Test handling of deleted worlds (404)"""
    world_id = "wrld_deleted"
    mock_db = AsyncMock()
    mock_limiter = AsyncMock()
    
    # Mock 404 response
    mock_responses.get(
        f"https://api.vrchat.cloud/api/1/worlds/{world_id}",
        status=404
    )
    
    scraper = Scraper(mock_db, mock_limiter, "test_cookie", "/tmp/images")
    await scraper.scrape_world(world_id)
    
    # Should mark as DELETED
    mock_db.upsert_world.assert_called_with(world_id, {}, status="DELETED")
    mock_limiter.record_result.assert_called_with(False, 404)

@pytest.mark.asyncio
async def test_auth_failure_raises(mock_responses):
    """Test that 401 errors raise AuthenticationError"""
    mock_responses.get(
        "https://api.vrchat.cloud/api/1/worlds",
        status=401
    )
    
    scraper = Scraper(AsyncMock(), AsyncMock(), "bad_cookie", "/tmp/images")
    
    with pytest.raises(AuthenticationError):
        await scraper.scrape_recent_worlds()
```

### Integration Test

```python
@pytest.mark.asyncio
async def test_full_scrape_flow():
    """Test complete flow from discovery to scrape"""
    # This would test the full flow with a mock API
    # that implements proper rate limiting behavior
    pass
```

## Key Implementation Notes

### Environment Configuration

```bash
# .env.example
DATABASE_URL=mysql://root:password@localhost:13306/vrchat
VRCHAT_AUTH_COOKIE=auth=authcookie_xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
IMAGE_STORAGE_PATH=/data/vrchat-images
LOG_LEVEL=INFO
```

### Error Handling Patterns

- Let transient errors naturally retry through the rescrape logic
- Only track persistent states (PENDING, SUCCESS, DELETED) in database
- Use circuit breaker for API-level issues, not individual world errors

### Async Best Practices

```python
# Ensure proper cleanup with context managers
async with scraper.client:
    await scraper.run()

# Handle graceful shutdown
signal.signal(signal.SIGTERM, lambda: shutdown_event.set())
```

### Docker Compose for Development

```yaml
version: '3.8'
services:
  dolt:
    image: dolthub/dolt-sql-server:latest
    ports:
      - "13306:3306"
    environment:
      - DOLT_ROOT_PATH=/var/lib/dolt
    volumes:
      - dolt_data:/var/lib/dolt
      
volumes:
  dolt_data:
```

### Image Storage

```python
def get_image_path(world_id: str) -> Path:
    """Generate hierarchical path to avoid large directories"""
    # world_id format: wrld_xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
    uuid_part = world_id[5:]  # Remove 'wrld_' prefix
    return Path(
        image_storage_path,
        uuid_part[0:2],
        uuid_part[2:4],
        f"{world_id}.png"
    )
```

## Bootstrap Process

1. Start with empty database
2. Run recent worlds scrape to get initial set
3. Import 237k world IDs from existing dataset:
   ```python
   # One-time import script
   with open('existing_world_ids.txt') as f:
       for world_id in f:
           await db.upsert_world(
               world_id.strip(),
               {"imported_at": datetime.utcnow()},
               status="PENDING"
           )
   ```

## Monitoring Checklist

- [ ] Request rate metrics
- [ ] Error rate by status code
- [ ] Queue depth (pending worlds)
- [ ] Scrape latency percentiles
- [ ] Database query performance
- [ ] Disk usage for images
- [ ] Auth cookie expiration alerts
