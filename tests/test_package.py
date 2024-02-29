from __future__ import annotations

import importlib.metadata

import disolv_positions as m


def test_version():
    assert importlib.metadata.version("disolv_positions") == m.__version__
