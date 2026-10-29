"""Generation-pointer state directories (stdlib only; shared by the thumbnail, embedding and history state).

A state dir holds immutable generation dirs `gen-<N>/` and one pointer file `CURRENT` ("gen-<N>\\n"). A new generation is written completely
into `gen-<N>.tmp/`, fsynced, renamed to `gen-<N>`, and ONLY THEN `CURRENT` is replaced (tmp + fsync + os.replace + fsync of the dir). That
replace is the single commit point: a kill anywhere before it leaves the previous generation current and untouched, a kill after it leaves the
new one current. Leftovers (`*.tmp`, a `gen-<N+1>` that CURRENT does not point to, generations older than the previous one) are garbage and are
removed by the next publish. Nothing is ever repaired in place.

`hook(step)` is called before every durable step; the tests replace it with a function that raises to simulate a kill at that step.
"""
from __future__ import annotations

import os
import re
import shutil
from pathlib import Path
from typing import Callable

GEN = re.compile(r"gen-(\d+)$")


class StateError(RuntimeError):
    """The state on disk is inconsistent. Never repaired silently: the job fails and a human decides."""


def hook(step: str) -> None:  # noqa: ARG001  (kill-injection point)
    pass


def fsync_file(p: Path) -> None:
    fd = os.open(p, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def fsync_dir(p: Path) -> None:
    fd = os.open(p, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def fsync_tree(p: Path) -> None:
    for f in sorted(p.rglob("*")):
        if f.is_file():
            fsync_file(f)
    for d in [p, *sorted(x for x in p.rglob("*") if x.is_dir())]:
        fsync_dir(d)


def write_durable(path: Path, data: bytes) -> None:
    with open(path, "wb") as f:
        f.write(data)
        f.flush()
        os.fsync(f.fileno())


def link_or_copy(src: Path, dst: Path) -> None:
    """Generations share unchanged immutable files by hard link (same dataset); copy when the filesystem refuses."""
    try:
        os.link(src, dst)
    except OSError:
        shutil.copyfile(src, dst)


def gen_numbers(state: Path) -> list[int]:
    return sorted(int(m.group(1)) for d in state.iterdir() if d.is_dir() and (m := GEN.match(d.name)))


def read_pointer(state: Path) -> Path | None:
    p = state / "CURRENT"
    if not p.is_file():
        return None
    name = p.read_text().strip()
    if not GEN.match(name) or not (state / name).is_dir():
        raise StateError(f"{p} points to {name!r}, which is not a generation dir in {state}")
    return state / name


def write_pointer(state: Path, name: str) -> None:
    tmp = state / "CURRENT.tmp"
    hook("pointer-tmp")
    write_durable(tmp, (name + "\n").encode())
    hook("pointer-replace")
    os.replace(tmp, state / "CURRENT")
    fsync_dir(state)
    hook("pointer-replaced")


def migrate(state: Path, moves: list[tuple[str, str]], extra_dirs: tuple[str, ...] = (), finish: Callable[[Path], None] | None = None) -> None:
    """Adopt a pre-generation flat layout as gen-0 without copying bytes: every flat entry in `moves` ((flat rel path, path inside gen-0)) is
    renamed into gen-0, then CURRENT is written. Idempotent and resumable: whatever is still flat is moved, whatever is already in gen-0
    stays; with no CURRENT, a gen-0 whose flat leftovers are all moved is committed as is. No flat entries and no gen-0 = no state."""
    state = Path(state)
    if (state / "CURRENT").is_file():
        return
    left = [(s, d) for s, d in moves if (state / s).exists()]
    g0 = state / "gen-0"
    if not left and not g0.is_dir():
        return
    g0.mkdir(exist_ok=True)
    for s, d in left:
        hook("migrate-move")
        (g0 / d).parent.mkdir(parents=True, exist_ok=True)
        os.rename(state / s, g0 / d)
    for d in extra_dirs:  # now-empty flat dirs
        try:
            (state / d).rmdir()
        except OSError:
            pass
    if finish:
        finish(g0)
    fsync_tree(g0)
    fsync_dir(state)
    write_pointer(state, "gen-0")


def resolve(state: Path) -> Path | None:
    state = Path(state)
    return read_pointer(state) if state.is_dir() else None


def gc(state: Path, keep: int = 2) -> None:
    """After CURRENT moved: drop everything except CURRENT's generation and the one before it, and stale tmp dirs."""
    cur = read_pointer(state)
    assert cur is not None
    n = int(GEN.match(cur.name).group(1))  # type: ignore[union-attr]
    older = [g for g in gen_numbers(state) if g < n]
    keep_set = {n, *older[-(keep - 1) :]} if keep > 1 else {n}
    for g in gen_numbers(state):
        if g not in keep_set:
            hook("gc")
            shutil.rmtree(state / f"gen-{g}", ignore_errors=True)
    for d in state.glob("gen-*.tmp"):
        shutil.rmtree(d, ignore_errors=True)


def publish(state: Path, populate: Callable[[Path], None], keep: int = 2) -> Path:
    """Write a new generation with populate(tmp_dir), then move CURRENT. Returns the new generation dir."""
    state = Path(state)
    state.mkdir(parents=True, exist_ok=True)
    cur = read_pointer(state)
    n = int(GEN.match(cur.name).group(1)) + 1 if cur else (max(gen_numbers(state), default=-1) + 1)  # type: ignore[union-attr]
    tmp, final = state / f"gen-{n}.tmp", state / f"gen-{n}"
    shutil.rmtree(tmp, ignore_errors=True)
    tmp.mkdir()
    populate(tmp)
    hook("gen-populated")
    fsync_tree(tmp)
    shutil.rmtree(final, ignore_errors=True)  # an uncommitted generation left by a killed run (CURRENT never pointed at it)
    hook("gen-rename")
    os.rename(tmp, final)
    fsync_dir(state)
    write_pointer(state, final.name)
    gc(state, keep)
    return final
