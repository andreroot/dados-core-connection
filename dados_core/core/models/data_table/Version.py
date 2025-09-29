from __future__ import annotations

from dados_core.core.models.Base import Base
from dados_core.core.models.data_table.DataTable import DataTable

class Version(Base):
    data_table: DataTable | None = None
    type: str | None = None
    data_version: str
    date: str
    data_entries: int
    schema_change: bool
    deprectated: dict
