"""Age-aware rescrape cadence: interval = min(tier, max(1, K * age_at_last_scrape))."""

from datetime import datetime, timedelta

import pytest

from src.vrchat_scraper.database import Database, ScrapeStatus, World, WorldMetrics

NOW = datetime.utcnow()
ZERO = dict(heat=0, popularity=0, occupants=0)
POPULAR = dict(heat=60, popularity=0, occupants=0)


async def make_db(age_k=0.25):
    db = Database("sqlite:///:memory:", age_k=age_k)
    await db.init_schema()
    return db


async def add_world(
    db,
    wid,
    *,
    published,
    scraped,
    metrics=ZERO,
    updated=None,
    status=ScrapeStatus.SUCCESS,
):
    """published/scraped/updated are datetimes; scraped=None means no metrics."""
    async with db.async_session() as s:
        s.add(
            World(
                world_id=wid,
                world_metadata={},
                publish_date=published,
                update_date=updated,
                last_scrape_time=scraped or NOW,
                scrape_status=status,
            )
        )
        if scraped is not None:
            s.add(
                WorldMetrics(
                    world_id=wid,
                    scrape_time=scraped,
                    favorites=0,
                    private_occupants=0,
                    public_occupants=0,
                    visits=0,
                    **metrics,
                )
            )
        await s.commit()


async def due(db, limit=100):
    return await db.get_worlds_to_scrape(limit=limit)


@pytest.mark.asyncio
async def test_young_world_zero_metrics_due_after_one_day():
    db = await make_db()
    pub = NOW - timedelta(days=1, hours=9)
    await add_world(db, "young", published=pub, scraped=pub + timedelta(hours=9))
    assert "young" in await due(db)
    assert (await db.count_worlds_due_for_scraping())["due_for_rescrape"] == 1


@pytest.mark.asyncio
async def test_young_world_not_due_before_one_day():
    db = await make_db()
    pub = NOW - timedelta(hours=20)
    await add_world(db, "young", published=pub, scraped=NOW - timedelta(hours=11))
    assert await due(db) == []


@pytest.mark.asyncio
async def test_old_world_zero_metrics_keeps_180():
    db = await make_db()
    pub = NOW - timedelta(days=1000)
    await add_world(db, "old_a", published=pub, scraped=NOW - timedelta(days=170))
    await add_world(db, "old_b", published=pub, scraped=NOW - timedelta(days=181))
    assert await due(db) == ["old_b"]


@pytest.mark.asyncio
async def test_popular_world_keeps_one_day_tier():
    db = await make_db()
    pub = NOW - timedelta(days=400)
    await add_world(
        db, "hot_a", published=pub, scraped=NOW - timedelta(hours=12), metrics=POPULAR
    )
    await add_world(
        db, "hot_b", published=pub, scraped=NOW - timedelta(hours=25), metrics=POPULAR
    )
    assert await due(db) == ["hot_b"]


@pytest.mark.asyncio
async def test_age_interval_grows_with_age():
    db = await make_db()
    # both scraped 11 days ago; b was 40 days old then (interval 10 d, due),
    # a was 80 days old then (interval 20 d, not due)
    scraped = NOW - timedelta(days=11)
    await add_world(db, "a", published=scraped - timedelta(days=80), scraped=scraped)
    await add_world(db, "b", published=scraped - timedelta(days=40), scraped=scraped)
    assert await due(db) == ["b"]


@pytest.mark.asyncio
async def test_updated_world_counts_as_young():
    db = await make_db()
    pub = NOW - timedelta(days=400)
    upd = NOW - timedelta(days=3)
    scraped = NOW - timedelta(days=2)  # born 1 day before scrape -> interval 1 day
    await add_world(db, "upd", published=pub, updated=upd, scraped=scraped)
    await add_world(db, "plain", published=pub, scraped=scraped)
    assert await due(db) == ["upd"]


@pytest.mark.asyncio
async def test_null_dates_fall_back_to_tier():
    db = await make_db()
    await add_world(db, "nodates", published=None, scraped=NOW - timedelta(days=100))
    assert await due(db) == []
    assert (await db.count_worlds_due_for_scraping())["due_for_rescrape"] == 0


@pytest.mark.asyncio
async def test_no_metrics_still_due():
    db = await make_db()
    await add_world(db, "nm", published=NOW - timedelta(days=5), scraped=None)
    assert await due(db) == ["nm"]


@pytest.mark.asyncio
async def test_k_zero_reproduces_tier_only():
    db = await make_db(age_k=0)
    pub = NOW - timedelta(hours=40)
    await add_world(db, "young", published=pub, scraped=pub + timedelta(hours=9))
    await add_world(
        db,
        "old",
        published=NOW - timedelta(days=400),
        scraped=NOW - timedelta(days=181),
    )
    await add_world(
        db,
        "hot",
        published=NOW - timedelta(days=400),
        scraped=NOW - timedelta(hours=25),
        metrics=POPULAR,
    )
    assert sorted(await due(db)) == ["hot", "old"]
    assert (await db.count_worlds_due_for_scraping())["due_for_rescrape"] == 2


@pytest.mark.asyncio
async def test_most_overdue_first_and_count_matches():
    db = await make_db()
    pub = NOW - timedelta(days=400)
    # all zero-metrics old worlds (180d tier); overdue ratios 2.0, 1.5, 1.1
    await add_world(db, "r15", published=pub, scraped=NOW - timedelta(days=270))
    await add_world(db, "r11", published=pub, scraped=NOW - timedelta(days=198))
    await add_world(db, "r20", published=pub, scraped=NOW - timedelta(days=360))
    assert await due(db) == ["r20", "r15", "r11"]
    assert (await db.count_worlds_due_for_scraping())["due_for_rescrape"] == 3
    db.reset_rescrape_cache()
    assert await due(db, limit=2) == ["r20", "r15"]


@pytest.mark.asyncio
async def test_pending_first():
    db = await make_db()
    pub = NOW - timedelta(days=400)
    await add_world(db, "old", published=pub, scraped=NOW - timedelta(days=360))
    await add_world(
        db, "pend", published=pub, scraped=None, status=ScrapeStatus.PENDING
    )
    assert await due(db) == ["pend", "old"]


def test_pure_rule():
    from src.vrchat_scraper.database import (
        compute_born,
        is_due,
        overdue_ratio,
        rescrape_interval_days,
        tier_days,
    )

    last = datetime(2026, 1, 10)
    born = last - timedelta(hours=9)
    assert rescrape_interval_days(180, born, last, 0.25) == 1.0
    assert rescrape_interval_days(180, born, last, 0) == 180
    assert rescrape_interval_days(180, None, last, 0.25) == 180
    assert rescrape_interval_days(1, last - timedelta(days=400), last, 0.25) == 1
    assert rescrape_interval_days(180, last - timedelta(days=40), last, 0.25) == 10
    assert rescrape_interval_days(180, last - timedelta(days=1000), last, 0.25) == 180
    assert compute_born(None, None) is None
    assert compute_born(last, None) == last
    assert compute_born(last - timedelta(days=1), last) == last
    assert tier_days(0, 0, 0) == 180 and tier_days(None, 0, 5) == 7
    assert tier_days(60, 0, 0) == 1 and tier_days(1, 0, 0) == 30
    now = last + timedelta(hours=23)
    assert not is_due(180, born, last, now, 0.25)
    assert is_due(180, born, last, last + timedelta(hours=24), 0.25)
    assert is_due(180, born, None, now, 0.25)
    assert overdue_ratio(180, born, last, last + timedelta(days=2), 0.25) == 2.0
    assert overdue_ratio(180, born, None, now, 0.25) == float("inf")


@pytest.mark.asyncio
async def test_batches_pop_from_cache_without_repeats():
    db = await make_db()
    pub = NOW - timedelta(days=1000)
    for i in range(5):
        await add_world(
            db, f"w{i}", published=pub, scraped=NOW - timedelta(days=200 + i * 10)
        )
    first = await db.get_worlds_to_scrape(limit=2)
    second = await db.get_worlds_to_scrape(limit=2)
    third = await db.get_worlds_to_scrape(limit=2)
    assert first == ["w4", "w3"] and second == ["w2", "w1"] and third == ["w0"]
    assert (await db.count_worlds_due_for_scraping())["due_for_rescrape"] == 5
    db.reset_rescrape_cache()
    assert len(await db.get_worlds_to_scrape(limit=10)) == 5
