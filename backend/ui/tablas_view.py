"""Vistas de tablas SQLite para la UI de escritorio RailOps."""

from __future__ import annotations

import tkinter as tk
from tkinter import ttk
from typing import Any

from backend.db.sqlite_service import get_connection


TABLE_CONFIG: dict[str, dict[str, str]] = {
    "tba": {"title": "Tabla TBA", "empty": "No hay registros en TBA."},
    "tbp": {"title": "Tabla TBP", "empty": "No hay registros en TBP."},
    "mallas": {"title": "Tabla Mallas", "empty": "No hay registros en Mallas."},
    "velocidades": {"title": "Tabla Velocidades", "empty": "No hay registros en Velocidades."},
    "conflictos": {"title": "Tabla Conflictos", "empty": "No hay registros en Conflictos."},
}


class TablaView(ttk.Frame):
    def __init__(self, parent: ttk.Widget, table_name: str):
        super().__init__(parent)
        if table_name not in TABLE_CONFIG:
            raise ValueError(f"Tabla no soportada: {table_name}")

        self.table_name = table_name
        self.columns: list[str] = []

        header = ttk.Frame(self)
        header.pack(fill="x", padx=12, pady=(12, 6))

        self.title_label = ttk.Label(
            header,
            text=TABLE_CONFIG[table_name]["title"],
            font=("Segoe UI", 12, "bold"),
        )
        self.title_label.pack(side="left")

        self.info_label = ttk.Label(header, text="")
        self.info_label.pack(side="right")

        table_frame = ttk.Frame(self)
        table_frame.pack(fill=tk.BOTH, expand=True, padx=12, pady=(0, 12))

        self.tree = ttk.Treeview(table_frame, show="headings")
        self.tree.pack(side="left", fill=tk.BOTH, expand=True)

        vsb = ttk.Scrollbar(table_frame, orient="vertical", command=self.tree.yview)
        hsb = ttk.Scrollbar(table_frame, orient="horizontal", command=self.tree.xview)
        self.tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)

        vsb.pack(side="right", fill="y")
        hsb.pack(side="bottom", fill="x")

        self.empty_label = ttk.Label(
            self,
            text=TABLE_CONFIG[table_name]["empty"],
            foreground="#6b7280",
        )

    def load_data(self) -> int:
        rows = self._fetch_rows()
        self._build_columns(rows)
        self._populate(rows)

        total = len(rows)
        self.info_label.config(text=f"Registros: {total}")

        if total == 0:
            self.empty_label.pack(anchor="w", padx=12, pady=(0, 10))
        else:
            self.empty_label.pack_forget()

        return total

    def _fetch_rows(self) -> list[dict[str, Any]]:
        conn = get_connection()
        try:
            cur = conn.cursor()
            cur.execute(f"SELECT * FROM {self.table_name} ORDER BY id DESC LIMIT 1000")
            return cur.fetchall()
        finally:
            conn.close()

    def _build_columns(self, rows: list[dict[str, Any]]) -> None:
        if rows:
            self.columns = list(rows[0].keys())
        else:
            self.columns = self._fetch_schema_columns()

        self.tree.delete(*self.tree.get_children())
        self.tree["columns"] = self.columns

        for col in self.columns:
            self.tree.heading(col, text=col)
            self.tree.column(col, width=130, minwidth=80, stretch=True, anchor="w")

    def _fetch_schema_columns(self) -> list[str]:
        conn = get_connection()
        try:
            cur = conn.cursor()
            cur.execute(f"PRAGMA table_info({self.table_name})")
            schema_rows = cur.fetchall()
            return [row["name"] for row in schema_rows]
        finally:
            conn.close()

    def _populate(self, rows: list[dict[str, Any]]) -> None:
        for row in rows:
            values = [self._safe_value(row.get(col)) for col in self.columns]
            self.tree.insert("", "end", values=values)

    @staticmethod
    def _safe_value(value: Any) -> str:
        if value is None:
            return ""
        return str(value)
