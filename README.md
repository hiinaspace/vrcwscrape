# VRChat World Scraper (vrcwscrape)

A scraper for mirroring VRChat public world metadata from the [official VRChat API][0]
into a relational database. I run this to maintain a public mirror as a Dolt database at:

https://www.dolthub.com/repositories/hiinaspace/vrcwscrape

[0]: https://vrchatapi.github.io/docs/api

## Why

VRChat's world search sucks, so I wanted a copy of the world metadata to build a better search index.
This scraper produces a fairly "raw" scrape, so it's not easy to query on its own. However, I do plan
to use it to make a more queriable database and search UI later, as a website and potentially a VRChat
world as well.

And eventually the VC money will run out and VRChat will enshittify (more) and/or shut down. So this'll
be a nice archival copy of the public world metadata, for when that happens.

## Includes

- `worlds`: raw JSON responses from the [GetWorld HTTP API](https://vrchatapi.github.io/docs/api)
  - There are ~237k public worlds as of June 2025.
  - includes all the fields e.g. name, description, tags, instance limit, published/updated dates.
- `file_metadata`: raw JSON responses from the [ShowFile HTTP API](https://vrchat.community/openapi/get-file) for images and unityPackages
  - this is mainly useful for the world download sizes.
- `image_content`: sha256 hashes of the latest version of the full world images
  - the scraper also downloads the full images to calculate these, but they're too big to store in this database directly.
  - I'll provide some way to get copies of the full images at some point, as addressed by these hashes.
- `world_metrics`: append-only metrics snapshots of dynamic world metadata (visits, favorite count, heat, popularity, etc)
  - metrics are re-scraped on a cadence based on recent heat/popularity, with a minimum of daily and a maximum of ~6 months.

## Excludes

- unityPackages aka AssetBundles
  - vrchat does provide the `md5` hashes in the file metadata at least.
- VRChat user metadata
  - World metadata has both an `author_name` and `author_id` field though.
- avatars
  - There are however other scrapers for public avatar metadata such as https://github.com/ShayBox/VRC-LOG if you're interested.
- nice indices or columns for the actual metadata
  - i.e. this is a somewhat raw scrape. If you actually want to query this a lot, you should probably transform the json columns into something more useful.

## Scraper

The scraper periodically fetches recently updated worlds from the [Recent Worlds API](https://vrchatapi.github.io/docs/api#recent-worlds) and scrapes any related file metadata and images.
It rescrapes existing worlds on a cadence derived from their latest metrics (heat/popularity/occupants), with a daily minimum and a multi-month maximum.

The [DESIGN.md](DESIGN.md) file has more details on the design and implementation of the scraper, as
slopped up by claude.

### Rate Limiter

Since VRChat doesn't publish an actual rate limit, this project contains a very overengineered client-side
rate limiter that discovers the rate limit in a control algorithm inspired by [TCP BBR Congestion Control](https://www.ietf.org/archive/id/draft-ietf-ccwg-bbr-02.html) . It's totally overkill for simple maintenance,
but it was kind of cool to watch it operate when doing the initial backfill of the ~237k worlds.

### Dolt

[Dolt](https://www.dolthub.com/) is essentially mysql backed by git. It's in
theory kind of nice for projects like this, in that it gives you a rigid
content-addressed form of the database. Practically it's kind of hokey. At the
scale of this data, it probably would've been better to just use sqlite. It's
not terrible though.

The project does use SQLAlchemy so if you don't want to deal with dolt yourself,
it should scrape into any other database that SQLAlchemy supports.

### Logfire

I got memed into using [pydantic logfire](https://logfire.pydantic.dev/) for observability. It's not awful,
and it is nice to see all the spans and metrics and stuff. Their web UI is clunky though; luckily it all uses
[OpenTelemetry](https://opentelemetry.io/) so you can switch it to a different backend if you want, say self-hosted
jaeger and prometheus.

### Claude Code

This project is about 95% slop as far as commited lines, but maybe 30% slop in terms of design. I did have to
guide claude pretty hard to put this all together, and there are some deficiencies that I missed when (not) reviewing its commits, but claude is good at converting a pretty detailed design into actual python when pressed. There still is a general slop feel in the codebase, where there are things that could be more elegant, plus like vestigial code; it does all work though, so if you follow the vibe and just don't look at how the sausage is made,
it's not too bad. Welcome to current year.

### Authentication

You'll need to provide a VRChat authentication cookie to the scraper, which you can get by logging into the VRChat website and copying the `auth` cookie from your browser, from the same IP address that the scraper will run on.

These auth cookies apparently last for a year, plus you have to do some annoying 2FA email stuff to auth anyway,
so expect to manually refresh the token every year.

## Contributing

In theory, the nature of dolt means you should be able to download the database and run scraper on it, and reasonably merge different scrapes with a fairly minimal custom merge policy (for each row, choose the latest by `scrape_time`). Dolt's default merge might even work, I dunno.

In practice, there are only about ~250 new worlds a day (and probably some similar amount updated), so a single scraper does fine. If I do ever stop my scraper though, it should be easiser to pick back up where I left off this way.
