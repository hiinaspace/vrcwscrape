# VRChat World Scrape (vrcwscrape)

A mirror of all the public VRChat worlds metadata, periodically scraped from the [public VRChat API](https://vrchatapi.github.io/docs/api).

## Includes

- `worlds`: raw JSON responses from the [GetWorld HTTP API](https://vrchatapi.github.io/docs/api)
  - There are ~237k public worlds as of June 2025.
  - includes all the fields e.g. name, description, tags, instance limit, published/updated dates.
- `file_metadata`: raw JSON responses from the [ShowFile HTTP API(https://vrchat.community/openapi/get-file) for images and unityPackages
  - this is mainly useful for the world download sizes.
- `image_content`: sha256 hashes of the latest version of the full world images
  - the scraper also downloads the full images to calculate these, but they're too big to store in this database directly.
  - I'll provide some way to get copies of the full images at some point, as addressed by these hashes.
- `world_metrics`: Append-only metrics of dynamic world metadata (visits, favorite count, popularity, etc)
  - these are also available in the dolt history, but it seemed nice to have a separate table as well.

## Excludes

- unityPackages aka AssetBundles
  - vrchat does provide the `md5` hashes in the file metadata at least.
- VRChat user metadata
  - World metadata has both an `author_name` and `author_id` field though.
- avatars
  - There are however other scrapers for public avatar metadata such as https://github.com/ShayBox/VRC-LOG if you're interested.
- nice indices or columns for the actual metadata
  - i.e. this is a somewhat raw scrape. If you actually want to query this a lot, you should probably transform the json columns into something more useful.

## Scraper code

I'll publish it at some point. It's just a bunch of python, slopped up by claude. It scrapes new or updated worlds from the [SearchWorld API](https://vrchat.community/openapi/search-worlds), and rescrapes existing worlds periodically to get updated metrics.

## Contributing

In theory, the nature of dolt means you should be able to download this database, run the same scraper on it, and reasonably merge different scrapes with a fairly minimal custom merge policy (for each row, choose the latest by `scrape_time`). Dolt's default merge might even work, I dunno.

In practice, there are only about ~250 new worlds a day (and probably some similar amount updated), so a single scraper does fine.
