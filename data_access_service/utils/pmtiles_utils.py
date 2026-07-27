"""Helpers for reading PMTiles files.

The PyPI package is also named ``pmtiles``. Local packages with that name
(``data_access_service.batch.pmtiles``, ``tests.batch.pmtiles``) can land on
``sys.path`` first and break ``from pmtiles.reader import Reader``. Imports
below resolve the *installed* package under site-packages explicitly.
"""

from __future__ import annotations

import importlib
import importlib.util
import sys
from contextlib import contextmanager
from pathlib import Path
from types import ModuleType
from typing import Any, Iterator

import requests


def _load_site_packages_pmtiles_reader() -> Any:
    """Return ``pmtiles.reader.Reader`` from the installed PyPI package."""
    # Fast path when the real package is already on sys.modules.
    existing = sys.modules.get("pmtiles")
    if existing is not None:
        existing_file = getattr(existing, "__file__", None) or ""
        if "site-packages" in Path(existing_file).as_posix():
            try:
                from pmtiles.reader import Reader

                return Reader
            except ModuleNotFoundError:
                pass

    # Drop any non-site-packages shadow (local batch/tests packages).
    for key in list(sys.modules):
        if key == "pmtiles" or key.startswith("pmtiles."):
            mod = sys.modules[key]
            mod_file = getattr(mod, "__file__", None) or ""
            paths = getattr(mod, "__path__", None)
            path_str = " ".join(str(p) for p in paths) if paths else mod_file
            if "site-packages" not in Path(path_str).as_posix():
                del sys.modules[key]

    # Prefer a normal import if site-packages wins on sys.path.
    try:
        from pmtiles.reader import Reader

        return Reader
    except ModuleNotFoundError:
        pass

    # Force-load from site-packages by file location (shadow still on sys.path).
    import site

    site_roots: list[str] = []
    try:
        site_roots.extend(site.getsitepackages())
    except Exception:
        pass
    try:
        user_site = site.getusersitepackages()
        if user_site:
            site_roots.append(user_site)
    except Exception:
        pass
    # Also scan every sys.path entry for a real pmtiles install.
    site_roots.extend(sys.path)

    seen: set[str] = set()
    for root in site_roots:
        if not root or root in seen:
            continue
        seen.add(root)
        pkg_dir = Path(root) / "pmtiles"
        init_py = pkg_dir / "__init__.py"
        reader_py = pkg_dir / "reader.py"
        if not init_py.is_file() or not reader_py.is_file():
            continue
        if (
            "site-packages" not in pkg_dir.as_posix()
            and "dist-packages" not in pkg_dir.as_posix()
        ):
            # Skip local project trees named pmtiles.
            continue

        def _load(name: str, path: Path, package: bool = False) -> ModuleType:
            if package:
                spec = importlib.util.spec_from_file_location(
                    name,
                    path,
                    submodule_search_locations=[str(path.parent)],
                )
            else:
                spec = importlib.util.spec_from_file_location(name, path)
            if spec is None or spec.loader is None:
                raise ImportError(f"Cannot load {name} from {path}")
            module = importlib.util.module_from_spec(spec)
            sys.modules[name] = module
            spec.loader.exec_module(module)
            return module

        _load("pmtiles", init_py, package=True)
        reader_mod = _load("pmtiles.reader", reader_py)
        return reader_mod.Reader

    raise ModuleNotFoundError(
        "Could not import pmtiles.reader.Reader. Install the PyPI package "
        "`pmtiles` (see pyproject.toml) and ensure no local directory named "
        "`pmtiles` shadows it on sys.path "
        "(e.g. data_access_service/batch/pmtiles or tests/batch/pmtiles)."
    )


# Resolve once at import time so open_pmtiles can use a normal name.
Reader = _load_site_packages_pmtiles_reader()


@contextmanager
def open_pmtiles(path: str) -> Iterator[Any]:
    f = open(path, "rb")

    def get_bytes(offset: int, length: int) -> bytes:
        f.seek(offset)
        return f.read(length)

    try:
        yield Reader(get_bytes)
    finally:
        f.close()


@contextmanager
def open_pmtiles_http(url: str) -> Iterator[Any]:
    session = requests.Session()

    def get_bytes(offset: int, length: int) -> bytes:
        headers = {"Range": f"bytes={offset}-{offset + length - 1}"}
        r = session.get(url, headers=headers)
        r.raise_for_status()
        return r.content

    try:
        yield Reader(get_bytes)
    finally:
        session.close()


def is_local_pmtiles_valid(remote_url: str, file_path: str) -> bool:
    """
    By default, data is increasing. Therefore, if local new generated pmtiles has less data than remote one,
    There may be something wrong with local one. So the local one is considered invalid and let developers to doublecheck.
    """

    def get_layer_counts(metadata):
        if "tilestats" not in metadata or "layers" not in metadata["tilestats"]:
            return None

        return {
            layer["layer"]: layer["count"] for layer in metadata["tilestats"]["layers"]
        }

    with open_pmtiles_http(remote_url) as reader:
        remote_metadata = reader.metadata()

    with open_pmtiles(file_path) as reader:
        local_metadata = reader.metadata()

    remote_counts = get_layer_counts(remote_metadata)
    local_counts = get_layer_counts(local_metadata)

    if local_counts is None or remote_counts is None:
        return False

    if set(remote_counts.keys()) != set(local_counts.keys()):
        return False

    for layer_name in remote_counts:
        if remote_counts[layer_name] > local_counts[layer_name]:
            return False

    return True
