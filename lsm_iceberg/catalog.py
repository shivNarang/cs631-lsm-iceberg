import os

from pyiceberg.catalog.sql import SqlCatalog

DEFAULT_WAREHOUSE = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "warehouse")


def get_catalog(warehouse_dir: str = None, name: str = "local") -> SqlCatalog:
    """A local Iceberg catalog: SQLite holds the catalog, files live under warehouse_dir."""
    warehouse_dir = os.path.abspath(warehouse_dir or os.environ.get("LSM_WAREHOUSE", DEFAULT_WAREHOUSE))
    os.makedirs(warehouse_dir, exist_ok=True)
    return SqlCatalog(
        name,
        uri=f"sqlite:///{os.path.join(warehouse_dir, 'catalog.db')}",
        warehouse=f"file://{warehouse_dir}",
    )
