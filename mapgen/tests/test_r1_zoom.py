from __future__ import annotations

import numpy as np
import pytest
from shapely import Polygon

from mapgen.r1_zoom import dominant_cluster_per_district, zoom_window


def test_zoom_window_centers_and_spans() -> None:
    assert zoom_window(10.0, 20.0, 5.0) == (5.0, 15.0, 15.0, 25.0)
    with pytest.raises(ValueError):
        zoom_window(0.0, 0.0, 0.0)


def test_dominant_cluster_per_district_picks_modal_label() -> None:
    left = Polygon([(0, 0), (10, 0), (10, 10), (0, 10)])
    right = Polygon([(10, 0), (20, 0), (20, 10), (10, 10)])
    # left: 2 "A" + 1 "B" -> A; right: 2 "C" -> C; a stray point hits neither.
    xs = np.array([1.0, 2.0, 3.0, 11.0, 12.0, 100.0])
    ys = np.array([1.0, 1.0, 1.0, 1.0, 1.0, 100.0])
    names = ["A", "A", "B", "C", "C", "Z"]

    out = dominant_cluster_per_district([left, right], xs, ys, names)
    assert out[0] == ("A", 2, 3)
    assert out[1] == ("C", 2, 2)
    # The district index for an empty district is simply absent.
    assert set(out.keys()) == {0, 1}


def test_dominant_cluster_handles_empty_inputs() -> None:
    assert dominant_cluster_per_district([], np.array([]), np.array([]), []) == {}
    sq = Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])
    assert dominant_cluster_per_district([sq], np.array([]), np.array([]), []) == {}
