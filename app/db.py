from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path


class DatabaseError(RuntimeError):
    """Raised for database setup/runtime errors."""


class Database:
    def __init__(self, db_path: Path, schema_path: Path) -> None:
        self.db_path = db_path
        self.schema_path = schema_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

    @contextmanager
    def connect(self):
        try:
            import duckdb  # type: ignore
        except ImportError as exc:
            raise DatabaseError(
                "duckdb package is required. Install dependencies before running the app."
            ) from exc

        conn = duckdb.connect(str(self.db_path))
        try:
            yield conn
        finally:
            conn.close()

    def init_schema(self) -> None:
        schema_sql = self.schema_path.read_text(encoding="utf-8")
        with self.connect() as conn:
            conn.execute(schema_sql)
