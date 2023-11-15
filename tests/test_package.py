from __future__ import annotations

import importlib.metadata

import prep_pavenet as m


def test_version():
    assert importlib.metadata.version("prep_pavenet") == m.__version__
