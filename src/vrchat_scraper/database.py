"""Database connection and operations for VRChat scraper."""

from datetime import datetime
from enum import Enum
from typing import Dict, List

from sqlalchemy import DateTime, Integer, String, func, select, JSON
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.sql import and_, or_


class Base(DeclarativeBase):
    """Base class for SQLAlchemy models."""

    pass


class ScrapeStatus(str, Enum):
    """Scrape status enumeration."""

    PENDING = "PENDING"
    SUCCESS = "SUCCESS"
    DELETED = "DELETED"


class World(Base):
    """World metadata table."""

    __tablename__ = "worlds"

    world_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    world_metadata: Mapped[dict] = mapped_column(JSON)
    publish_date: Mapped[datetime | None] = mapped_column(DateTime)
    update_date: Mapped[datetime | None] = mapped_column(DateTime)
    last_scrape_time: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    scrape_status: Mapped[ScrapeStatus] = mapped_column(String(20), nullable=False)


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


class Database:
    """Database layer for VRChat world metadata storage."""

    def __init__(self, connection_string: str):
        """Initialize database connection."""
        self.connection_string = connection_string
        self.engine = create_async_engine(
            self._convert_connection_string(connection_string)
        )
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
                # Update existing world
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
                return func.timestampdiff(func.literal_column("DAY"), date2, date1)

            def hours_diff(date1, date2):
                return func.timestampdiff(func.literal_column("HOUR"), date2, date1)

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
