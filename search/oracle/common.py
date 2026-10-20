"""Shared semantics (mirrors SEMANTICS.md) and data loading for the oracle."""

from __future__ import annotations

import re
from pathlib import Path

import duckdb

HERE = Path(__file__).parent
DEFAULT_PARQUET = "/work/projects/vrcwscrape/mapgen/data/worlds_search.parquet"
CJK_RE = re.compile(r"[぀-ヿ㐀-䶿一-鿿가-힯]")
KANA_RE = re.compile(r"[぀-ヿ]")
HANGUL_RE = re.compile(r"[가-힯]")
HAN_RE = re.compile(r"[㐀-䶿一-鿿]")
CJK_RUN_RE = re.compile(r"[぀-ヿ㐀-䶿一-鿿가-힯]+")


def connect(parquet: str) -> duckdb.DuckDBPyConnection:
    """DuckDB with view `w` holding the parquet plus derived columns (see SEMANTICS.md)."""
    con = duckdb.connect()
    con.execute(
        f"""
        create view w as select *,
          pc_size_mb is not null as has_pc,
          quest_size_mb is not null as has_quest,
          list_sort(list_distinct(list_transform(coalesce(tags, []),
              t -> regexp_replace(t, '^author_tag_', '')))) as tagset
        from read_parquet('{parquet}')
        """
    )
    return con


def sql_str(s: str) -> str:
    return "'" + s.replace("'", "''") + "'"


def where_sql(f: dict) -> str:
    """Filter semantics -> SQL boolean expression over view `w`."""
    c = ["true"]
    if f.get("cap_min") is not None:
        c.append(f"capacity >= {int(f['cap_min'])}")
    if f.get("cap_max") is not None:
        c.append(f"capacity <= {int(f['cap_max'])}")
    plat = f.get("platform")
    if plat == "pc":
        c.append("has_pc")
    elif plat == "quest":
        c.append("has_quest")
    elif plat == "both":
        c.append("(has_pc and has_quest)")
    if f.get("size_max_mb") is not None:
        x = float(f["size_max_mb"])
        if plat == "pc":
            c.append(f"pc_size_mb <= {x}")
        elif plat == "quest":
            c.append(f"quest_size_mb <= {x}")
        elif plat == "both":
            c.append(f"(pc_size_mb <= {x} and quest_size_mb <= {x})")
        else:
            c.append(f"least(coalesce(pc_size_mb, 1e18), coalesce(quest_size_mb, 1e18)) <= {x}")
    if f.get("after"):
        c.append(f"updated_at >= timestamp {sql_str(f['after'] + ' 00:00:00')}")
    if f.get("before"):
        c.append(f"updated_at < timestamp {sql_str(f['before'] + ' 00:00:00')} + interval 1 day")
    for t in f.get("tags") or []:
        c.append(f"list_contains(tagset, {sql_str(t)})")
    if f.get("author"):
        c.append(f"author_id = {sql_str(f['author'])}")
    return " and ".join(c)


def facet_truth(con, f: dict, top_tags: int = 20) -> dict:
    wh = where_sql(f)
    total, pc, quest, both = con.execute(
        f"select count(*), count(*) filter (has_pc), count(*) filter (has_quest), "
        f"count(*) filter (has_pc and has_quest) from w where {wh}"
    ).fetchone()
    tags = con.execute(
        f"select t, count(*) n from (select unnest(tagset) t from w where {wh}) "
        f"group by t order by n desc, t asc limit {top_tags}"
    ).fetchall()
    cap = con.execute(
        f"""select case when capacity <= 8 then '1-8' when capacity <= 16 then '9-16'
            when capacity <= 32 then '17-32' when capacity <= 64 then '33-64' else '65+' end b,
            count(*) from w where {wh} and capacity is not null group by b"""
    ).fetchall()
    return {
        "total": total,
        "platform": {"pc": pc, "quest": quest, "both": both},
        "tags": {t: n for t, n in tags},
        "capacity": {b: n for b, n in cap},
    }


def script_of(name: str) -> str:
    """'cjk' if the name contains any kana/han/hangul, else 'latin'."""
    return "cjk" if CJK_RE.search(name or "") else "latin"
