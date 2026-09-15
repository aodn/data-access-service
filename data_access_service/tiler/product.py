from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Product:
    id: str
    uuid: str = ""
    dataset: str = ""
    variable: str = ""
    timestamps: tuple[str, ...] = ()
    n_i: int = 0
    n_j: int = 0
    lat_min: float = 0.0
    lat_max: float = 0.0
    lon_min: float = 0.0
    lon_max: float = 0.0
    vmin: float = 0.0
    vmax: float = 1.0
    source_path: str = ""
    visual: bool = True
    ocean_masked: bool = False

    @property
    def metadata_uuid(self) -> str:
        return self.uuid

    @property
    def variables(self) -> list[str]:
        return [self.variable]


_PRODUCTS: dict[str, Product] = {}
PRODUCTS = _PRODUCTS


def load_products(products: dict[str, Product]) -> None:
    _PRODUCTS.clear()
    _PRODUCTS.update(products)


def get_product(product_id: str) -> Product | None:
    return _PRODUCTS.get(product_id)


def iter_products() -> list[Product]:
    return list(_PRODUCTS.values())


def iter_product_items() -> list[tuple[str, Product]]:
    return list(_PRODUCTS.items())
