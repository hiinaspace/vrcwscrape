"""Did the image block help the worlds it was meant to (thin author text)?

The captioned set (captions.parquet) is exactly the thin-desc (<40 char) + thumb
worlds. Compare their noise rate (didn't join any cluster) in text-only vs
image-fused clustering. If the image block gives weak-text worlds usable signal,
their noise rate should drop relative to rich-text worlds.
"""

from __future__ import annotations

import polars as pl

THIN = set(pl.read_parquet("artifacts/captions.parquet")["world_id"].to_list())

for name in ("artifacts_text_leaf", "artifacts_img0.3_leaf"):
    c = pl.read_parquet(f"{name}/clusters.parquet").with_columns(
        thin=pl.col("world_id").is_in(list(THIN)),
        noise=pl.col("cluster_id") == -1,
    )
    thin = c.filter("thin")
    rich = c.filter(~pl.col("thin"))
    tn = thin.filter("noise").height
    rn = rich.filter("noise").height
    print(
        f"{name}: thin-text noise {tn:,}/{thin.height:,} "
        f"({100 * tn / thin.height:.0f}%)  |  rich-text noise {rn:,}/{rich.height:,} "
        f"({100 * rn / rich.height:.0f}%)"
    )
