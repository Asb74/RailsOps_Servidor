"""Componente reutilizable de tabla con filtros y ordenación para Tkinter/ttk."""

from __future__ import annotations

import tkinter as tk
from tkinter import ttk
from typing import Any, Callable


class ColumnFilterPopup(tk.Toplevel):
    """Popup tipo Excel para filtrar una columna por valores únicos."""

    def __init__(
        self,
        parent: tk.Widget,
        column: str,
        values: list[str],
        selected_values: set[str] | None,
        on_apply: Callable[[set[str] | None], None],
        on_sort: Callable[[bool], None],
    ) -> None:
        super().__init__(parent)
        self.title(f"Filtro: {column}")
        self.transient(parent.winfo_toplevel())
        self.resizable(False, False)

        self._on_apply = on_apply
        self._on_sort = on_sort
        self._value_vars: dict[str, tk.BooleanVar] = {}

        outer = ttk.Frame(self, padding=10)
        outer.pack(fill="both", expand=True)

        sort_frame = ttk.LabelFrame(outer, text="Orden")
        sort_frame.pack(fill="x", pady=(0, 8))
        ttk.Button(sort_frame, text="Ascendente ▲", command=lambda: self._handle_sort(False)).pack(
            side="left", padx=6, pady=6
        )
        ttk.Button(sort_frame, text="Descendente ▼", command=lambda: self._handle_sort(True)).pack(
            side="left", padx=6, pady=6
        )

        values_frame = ttk.LabelFrame(outer, text="Valores")
        values_frame.pack(fill="both", expand=True)

        select_all_default = selected_values is None
        self.select_all_var = tk.BooleanVar(value=select_all_default)
        ttk.Checkbutton(
            values_frame,
            text="Seleccionar todo",
            variable=self.select_all_var,
            command=self._toggle_all,
        ).pack(anchor="w", padx=8, pady=(8, 4))

        list_container = ttk.Frame(values_frame)
        list_container.pack(fill="both", expand=True, padx=8, pady=(0, 8))

        canvas = tk.Canvas(list_container, width=280, height=220, highlightthickness=0)
        scrollbar = ttk.Scrollbar(list_container, orient="vertical", command=canvas.yview)
        inner = ttk.Frame(canvas)

        inner.bind(
            "<Configure>",
            lambda e: canvas.configure(scrollregion=canvas.bbox("all")),
        )

        canvas.create_window((0, 0), window=inner, anchor="nw")
        canvas.configure(yscrollcommand=scrollbar.set)

        for value in values:
            checked = selected_values is None or value in selected_values
            var = tk.BooleanVar(value=checked)
            self._value_vars[value] = var
            ttk.Checkbutton(inner, text=value or "(vacío)", variable=var).pack(anchor="w", pady=1)

        canvas.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")

        actions = ttk.Frame(outer)
        actions.pack(fill="x", pady=(8, 0))
        ttk.Button(actions, text="Aplicar", command=self._apply).pack(side="right", padx=(8, 0))
        ttk.Button(actions, text="Cancelar", command=self.destroy).pack(side="right")

        self.grab_set()
        self.protocol("WM_DELETE_WINDOW", self.destroy)

    def _toggle_all(self) -> None:
        state = self.select_all_var.get()
        for var in self._value_vars.values():
            var.set(state)

    def _handle_sort(self, reverse: bool) -> None:
        self._on_sort(reverse)

    def _apply(self) -> None:
        selected = {value for value, var in self._value_vars.items() if var.get()}
        if len(selected) == len(self._value_vars):
            self._on_apply(None)
        else:
            self._on_apply(selected)
        self.destroy()


class FilterableTable(ttk.Frame):
    """Tabla reutilizable con filtros por columna, filtros rápidos y ordenación."""

    def __init__(
        self,
        parent: ttk.Widget,
        on_counts_changed: Callable[[int, int], None] | None = None,
        header_aliases: dict[str, str] | None = None,
    ) -> None:
        super().__init__(parent)
        self.on_counts_changed = on_counts_changed
        self.header_aliases = header_aliases or {}

        self.columns: list[str] = []
        self.datos_originales: list[dict[str, Any]] = []
        self.datos_filtrados: list[dict[str, Any]] = []
        self.filtros_columna: dict[str, set[str] | None] = {}
        self.filtros_texto: dict[str, str] = {}
        self.sort_column: str | None = None
        self.sort_reverse = False

        self._entry_vars: dict[str, tk.StringVar] = {}

        self._build_ui()

    def _build_ui(self) -> None:
        top = ttk.LabelFrame(self, text="Filtros rápidos")
        top.pack(fill="x", padx=0, pady=(0, 8))

        self.quick_filters_frame = ttk.Frame(top)
        self.quick_filters_frame.pack(fill="x", padx=8, pady=8)

        actions = ttk.Frame(top)
        actions.pack(fill="x", padx=8, pady=(0, 8))
        ttk.Button(actions, text="Filtrar", command=self.apply_filters).pack(side="left")
        ttk.Button(actions, text="Limpiar filtros", command=self.clear_filters).pack(side="left", padx=8)

        table_frame = ttk.Frame(self)
        table_frame.pack(fill=tk.BOTH, expand=True)

        self.tree = ttk.Treeview(table_frame, show="headings")
        self.tree.pack(side="left", fill=tk.BOTH, expand=True)

        vsb = ttk.Scrollbar(table_frame, orient="vertical", command=self.tree.yview)
        hsb = ttk.Scrollbar(table_frame, orient="horizontal", command=self.tree.xview)
        self.tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)

        vsb.pack(side="right", fill="y")
        hsb.pack(side="bottom", fill="x")

    def set_columns(self, columns: list[str]) -> None:
        self.columns = columns
        self.filtros_columna = {col: None for col in columns}
        self.filtros_texto = {col: "" for col in columns}
        self._entry_vars = {}

        self.tree.delete(*self.tree.get_children())
        self.tree["columns"] = columns

        for col in columns:
            self.tree.heading(col, text=self._build_heading_text(col), command=lambda c=col: self._open_column_popup(c))
            self.tree.column(col, width=130, minwidth=80, stretch=True, anchor="w")

        for child in self.quick_filters_frame.winfo_children():
            child.destroy()

        for idx, col in enumerate(columns):
            ttk.Label(self.quick_filters_frame, text=self.header_aliases.get(col, col)).grid(
                row=0, column=idx, sticky="w", padx=(0, 6), pady=(0, 2)
            )
            var = tk.StringVar(value="")
            self._entry_vars[col] = var
            entry = ttk.Entry(self.quick_filters_frame, textvariable=var, width=16)
            entry.grid(row=1, column=idx, sticky="ew", padx=(0, 6))

        for idx in range(len(columns)):
            self.quick_filters_frame.grid_columnconfigure(idx, weight=1)

    def set_data(self, rows: list[dict[str, Any]]) -> None:
        self.datos_originales = list(rows)
        self.apply_filters()

    def clear_filters(self) -> None:
        self.filtros_columna = {col: None for col in self.columns}
        self.filtros_texto = {col: "" for col in self.columns}
        self.sort_column = None
        self.sort_reverse = False

        for col, var in self._entry_vars.items():
            var.set("")
            self.tree.heading(col, text=self._build_heading_text(col), command=lambda c=col: self._open_column_popup(c))

        self.apply_filters()

    def _sync_text_filters_from_entries(self) -> None:
        for col, var in self._entry_vars.items():
            self.filtros_texto[col] = var.get().strip().lower()

    def apply_filters(self) -> None:
        self._sync_text_filters_from_entries()

        filtered = []
        for row in self.datos_originales:
            if not self._row_matches_filters(row):
                continue
            filtered.append(row)

        if self.sort_column:
            col = self.sort_column
            filtered.sort(
                key=lambda r: self._sortable_value(r.get(col)),
                reverse=self.sort_reverse,
            )

        self.datos_filtrados = filtered
        self._render_rows(filtered)
        self._refresh_headings()
        self._notify_counts()

    def _row_matches_filters(self, row: dict[str, Any]) -> bool:
        for col in self.columns:
            value = self._safe_value(row.get(col))

            selected_values = self.filtros_columna.get(col)
            if selected_values is not None and value not in selected_values:
                return False

            text_filter = self.filtros_texto.get(col, "")
            if text_filter and text_filter not in value.lower():
                return False

        return True

    def _render_rows(self, rows: list[dict[str, Any]]) -> None:
        self.tree.delete(*self.tree.get_children())
        for row in rows:
            values = [self._safe_value(row.get(col)) for col in self.columns]
            self.tree.insert("", "end", values=values)

    def _notify_counts(self) -> None:
        visibles = len(self.datos_filtrados)
        total = len(self.datos_originales)
        if self.on_counts_changed:
            self.on_counts_changed(visibles, total)

    def _open_column_popup(self, column: str) -> None:
        values = self._get_unique_values_for_column(column)
        selected = self.filtros_columna.get(column)

        ColumnFilterPopup(
            parent=self,
            column=self.header_aliases.get(column, column),
            values=values,
            selected_values=selected,
            on_apply=lambda selected_values: self._apply_column_filter(column, selected_values),
            on_sort=lambda reverse: self._set_sort(column, reverse),
        )

    def _apply_column_filter(self, column: str, selected_values: set[str] | None) -> None:
        self.filtros_columna[column] = selected_values
        self.apply_filters()

    def _set_sort(self, column: str, reverse: bool) -> None:
        self.sort_column = column
        self.sort_reverse = reverse
        self.apply_filters()

    def _get_unique_values_for_column(self, column: str) -> list[str]:
        values: set[str] = set()
        for row in self.datos_originales:
            if not self._row_matches_except_column(row, column):
                continue
            values.add(self._safe_value(row.get(column)))
        return sorted(values, key=self._sortable_value)

    def _row_matches_except_column(self, row: dict[str, Any], ignored_column: str) -> bool:
        for col in self.columns:
            value = self._safe_value(row.get(col))

            if col != ignored_column:
                selected_values = self.filtros_columna.get(col)
                if selected_values is not None and value not in selected_values:
                    return False

            text_filter = self.filtros_texto.get(col, "")
            if text_filter and text_filter not in value.lower():
                return False

        return True

    def _refresh_headings(self) -> None:
        for col in self.columns:
            self.tree.heading(col, text=self._build_heading_text(col), command=lambda c=col: self._open_column_popup(c))

    def _build_heading_text(self, col: str) -> str:
        base = self.header_aliases.get(col, col)
        if self.sort_column == col:
            return f"{base} {'▼' if self.sort_reverse else '▲'}"
        return base

    @staticmethod
    def _safe_value(value: Any) -> str:
        if value is None:
            return ""
        return str(value)

    @staticmethod
    def _sortable_value(value: Any) -> tuple[int, Any]:
        if value is None:
            return (2, "")

        if isinstance(value, (int, float)):
            return (0, value)

        text = str(value).strip()
        if text == "":
            return (2, "")

        try:
            if "." in text:
                return (0, float(text))
            return (0, int(text))
        except ValueError:
            return (1, text.lower())
