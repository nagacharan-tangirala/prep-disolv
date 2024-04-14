from __future__ import annotations

import importlib.metadata

import prep_disolv as m


def test_version():
    assert importlib.metadata.version("disolv_positions") == m.__version__
