"""Componente reutilizable de tabla con filtros estilo Excel para Tkinter/ttk."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, date
import tkinter as tk
from tkinter import ttk
from typing import Any, Callable

DATE_FORMATS = (
    "%Y-%m-%d",
    "%d/%m/%Y",
    "%d-%m-%Y",
    "%Y/%m/%d",
    "%d/%m/%y",
)


@dataclass
class FilterState:
    selected_values: set[str] | None = None
    advanced_enabled: bool = False
    operator: str = ""
    value1: str = ""
    value2: str = ""


class AdvancedFilterDialog(tk.Toplevel):
    OPS_BY_TYPE = {
        "texto": ["contiene", "no contiene", "empieza por", "termina en", "igual", "distinto"],
        "numerico": ["=", "≠", ">", "<", "≥", "≤", "entre"],
        "fecha": ["es", "antes de", "después de", "entre", "hoy", "este mes"],
        "booleano": ["True", "False"],
    }

    def __init__(self, parent: tk.Widget, column: str, column_type: str, state: FilterState, on_apply: Callable[[FilterState], None]) -> None:
        super().__init__(parent)
        self.title(f"Filtro avanzado: {column}")
        self.transient(parent.winfo_toplevel())
        self.resizable(False, False)
        self._on_apply = on_apply
        self.column_type = column_type

        self.enabled_var = tk.BooleanVar(value=state.advanced_enabled)
        self.op_var = tk.StringVar(value=state.operator or self.OPS_BY_TYPE[column_type][0])
        self.val1_var = tk.StringVar(value=state.value1)
        self.val2_var = tk.StringVar(value=state.value2)

        main = ttk.Frame(self, padding=10)
        main.pack(fill="both", expand=True)

        ttk.Checkbutton(main, text="Activar filtro avanzado", variable=self.enabled_var, command=self._refresh_inputs).pack(anchor="w", pady=(0, 8))

        ttk.Label(main, text="Operador").pack(anchor="w")
        self.ops_combo = ttk.Combobox(main, values=self.OPS_BY_TYPE[column_type], textvariable=self.op_var, state="readonly", width=26)
        self.ops_combo.pack(fill="x", pady=(0, 8))
        self.ops_combo.bind("<<ComboboxSelected>>", lambda _: self._refresh_inputs())

        ttk.Label(main, text="Valor 1").pack(anchor="w")
        self.val1_entry = ttk.Entry(main, textvariable=self.val1_var)
        self.val1_entry.pack(fill="x", pady=(0, 8))

        ttk.Label(main, text="Valor 2").pack(anchor="w")
        self.val2_entry = ttk.Entry(main, textvariable=self.val2_var)
        self.val2_entry.pack(fill="x")

        actions = ttk.Frame(main)
        actions.pack(fill="x", pady=(10, 0))
        ttk.Button(actions, text="Aplicar", command=self._apply).pack(side="right", padx=(8, 0))
        ttk.Button(actions, text="Cancelar", command=self.destroy).pack(side="right")

        self._refresh_inputs()
        self.grab_set()

    def _refresh_inputs(self) -> None:
        enabled = self.enabled_var.get()
        op = self.op_var.get()

        for widget in (self.ops_combo, self.val1_entry, self.val2_entry):
            widget.configure(state="normal" if enabled else "disabled")
        if enabled:
            self.ops_combo.configure(state="readonly")

        if not enabled:
            return

        no_values = {"hoy", "este mes", "True", "False"}
        if op in no_values:
            self.val1_entry.configure(state="disabled")
            self.val2_entry.configure(state="disabled")
        elif op == "entre":
            self.val1_entry.configure(state="normal")
            self.val2_entry.configure(state="normal")
        else:
            self.val1_entry.configure(state="normal")
            self.val2_entry.configure(state="disabled")

    def _apply(self) -> None:
        state = FilterState(
            advanced_enabled=self.enabled_var.get(),
            operator=self.op_var.get(),
            value1=self.val1_var.get().strip(),
            value2=self.val2_var.get().strip(),
        )
        self._on_apply(state)
        self.destroy()


class ColumnFilterPopup(tk.Toplevel):
    def __init__(
        self,
        parent: tk.Widget,
        column: str,
        values: list[str],
        filter_state: FilterState,
        column_type: str,
        on_apply: Callable[[FilterState], None],
        on_sort: Callable[[bool], None],
    ) -> None:
        super().__init__(parent)
        self.title(f"Filtro: {column}")
        self.transient(parent.winfo_toplevel())
        self.resizable(False, False)

        self._on_apply = on_apply
        self._column_type = column_type
        self._value_vars: dict[str, tk.BooleanVar] = {}
        self._search_var = tk.StringVar(value="")
        self._base_state = FilterState(
            selected_values=None if filter_state.selected_values is None else set(filter_state.selected_values),
            advanced_enabled=filter_state.advanced_enabled,
            operator=filter_state.operator,
            value1=filter_state.value1,
            value2=filter_state.value2,
        )

        outer = ttk.Frame(self, padding=10)
        outer.pack(fill="both", expand=True)

        sort_frame = ttk.LabelFrame(outer, text="Orden")
        sort_frame.pack(fill="x", pady=(0, 8))
        ttk.Button(sort_frame, text="Ascendente ▲", command=lambda: on_sort(False)).pack(side="left", padx=6, pady=6)
        ttk.Button(sort_frame, text="Descendente ▼", command=lambda: on_sort(True)).pack(side="left", padx=6, pady=6)

        adv = ttk.Frame(outer)
        adv.pack(fill="x", pady=(0, 8))
        ttk.Button(adv, text="Filtro avanzado", command=self._abrir_filtro_avanzado).pack(side="left")

        values_frame = ttk.LabelFrame(outer, text="Valores")
        values_frame.pack(fill="both", expand=True)

        ttk.Entry(values_frame, textvariable=self._search_var).pack(fill="x", padx=8, pady=(8, 4))
        self._search_var.trace_add("write", lambda *_: self._refrescar_lista())

        self.select_all_var = tk.BooleanVar(value=filter_state.selected_values is None)
        ttk.Checkbutton(values_frame, text="Seleccionar todo", variable=self.select_all_var, command=self._toggle_all).pack(anchor="w", padx=8, pady=(0, 4))

        self.list_container = ttk.Frame(values_frame)
        self.list_container.pack(fill="both", expand=True, padx=8, pady=(0, 8))

        self.canvas = tk.Canvas(self.list_container, width=280, height=220, highlightthickness=0)
        scrollbar = ttk.Scrollbar(self.list_container, orient="vertical", command=self.canvas.yview)
        self.inner = ttk.Frame(self.canvas)
        self.inner.bind("<Configure>", lambda _: self.canvas.configure(scrollregion=self.canvas.bbox("all")))
        self.canvas.create_window((0, 0), window=self.inner, anchor="nw")
        self.canvas.configure(yscrollcommand=scrollbar.set)
        self.canvas.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")

        checked_all = filter_state.selected_values is None
        for value in values:
            checked = checked_all or value in (filter_state.selected_values or set())
            self._value_vars[value] = tk.BooleanVar(value=checked)

        self._refrescar_lista()

        actions = ttk.Frame(outer)
        actions.pack(fill="x", pady=(8, 0))
        ttk.Button(actions, text="Aplicar", command=self._apply).pack(side="right", padx=(8, 0))
        ttk.Button(actions, text="Cancelar", command=self.destroy).pack(side="right")

        self.grab_set()

    def _abrir_filtro_avanzado(self) -> None:
        def _on_adv_apply(state: FilterState) -> None:
            self._base_state.advanced_enabled = state.advanced_enabled
            self._base_state.operator = state.operator
            self._base_state.value1 = state.value1
            self._base_state.value2 = state.value2

        AdvancedFilterDialog(self, self.title().replace("Filtro: ", ""), self._column_type, self._base_state, _on_adv_apply)

    def _refrescar_lista(self) -> None:
        for child in self.inner.winfo_children():
            child.destroy()
        needle = self._search_var.get().strip().lower()
        for value, var in self._value_vars.items():
            if needle and needle not in value.lower():
                continue
            ttk.Checkbutton(self.inner, text=value or "(vacío)", variable=var).pack(anchor="w", pady=1)

    def _toggle_all(self) -> None:
        value = self.select_all_var.get()
        for var in self._value_vars.values():
            var.set(value)

    def _apply(self) -> None:
        selected = {value for value, var in self._value_vars.items() if var.get()}
        self._base_state.selected_values = None if len(selected) == len(self._value_vars) else selected
        self._on_apply(self._base_state)
        self.destroy()


class FilterTable(ttk.Frame):
    """Tabla reutilizable con filtros por columna, avanzados y ordenación."""

    def __init__(self, parent: ttk.Widget, on_counts_changed: Callable[[int, int], None] | None = None, header_aliases: dict[str, str] | None = None, treeview: ttk.Treeview | None = None) -> None:
        super().__init__(parent)
        self.on_counts_changed = on_counts_changed
        self.header_aliases = header_aliases or {}

        self.columnas: list[str] = []
        self.datos_originales: list[dict[str, Any]] = []
        self.filtros_activos: dict[str, FilterState] = {}
        self.column_types: dict[str, str] = {}
        self.sort_column: str | None = None
        self.sort_reverse = False
        self._entry_vars: dict[str, tk.StringVar] = {}
        self._datos_filtrados: list[dict[str, Any]] = []

        self._build_ui(treeview)

    def _build_ui(self, external_tree: ttk.Treeview | None) -> None:
        top = ttk.LabelFrame(self, text="Filtros rápidos")
        top.pack(fill="x", pady=(0, 8))

        self.quick_filters_frame = ttk.Frame(top)
        self.quick_filters_frame.pack(fill="x", padx=8, pady=8)

        actions = ttk.Frame(top)
        actions.pack(fill="x", padx=8, pady=(0, 8))
        ttk.Button(actions, text="Filtrar", command=self.aplicar_filtros).pack(side="left")
        ttk.Button(actions, text="Limpiar filtros", command=self.limpiar_filtros).pack(side="left", padx=8)

        table_frame = ttk.Frame(self)
        table_frame.pack(fill=tk.BOTH, expand=True)

        self.tree = external_tree or ttk.Treeview(table_frame, show="headings")
        self.tree.pack(side="left", fill=tk.BOTH, expand=True)

        vsb = ttk.Scrollbar(table_frame, orient="vertical", command=self.tree.yview)
        hsb = ttk.Scrollbar(table_frame, orient="horizontal", command=self.tree.xview)
        self.tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)
        vsb.pack(side="right", fill="y")
        hsb.pack(side="bottom", fill="x")

        self.tree.bind("<Button-3>", self._on_tree_right_click)

    def configurar_tabla(self, columnas: list[str], datos: list[dict[str, Any]], treeview: ttk.Treeview | None = None) -> None:
        if treeview is not None and treeview is not self.tree:
            self.tree = treeview
        self.set_columns(columnas)
        self.set_data(datos)

    def set_columns(self, columns: list[str]) -> None:
        self.columnas = list(columns)
        self.filtros_activos = {col: FilterState() for col in columns}
        self.column_types = {col: "texto" for col in columns}
        self._entry_vars = {}

        self.tree.delete(*self.tree.get_children())
        self.tree["columns"] = columns

        for col in columns:
            self.tree.heading(col, text=self._build_heading_text(col), command=lambda c=col: self.mostrar_filtro_columna(c))
            self.tree.column(col, width=130, minwidth=80, stretch=True, anchor="w")

        for child in self.quick_filters_frame.winfo_children():
            child.destroy()

        for idx, col in enumerate(columns):
            ttk.Label(self.quick_filters_frame, text=self.header_aliases.get(col, col)).grid(row=0, column=idx, sticky="w", padx=(0, 6), pady=(0, 2))
            var = tk.StringVar(value="")
            self._entry_vars[col] = var
            ttk.Entry(self.quick_filters_frame, textvariable=var, width=16).grid(row=1, column=idx, sticky="ew", padx=(0, 6))

        for idx in range(len(columns)):
            self.quick_filters_frame.grid_columnconfigure(idx, weight=1)

    def set_data(self, rows: list[dict[str, Any]]) -> None:
        self.datos_originales = list(rows)
        self._infer_column_types()
        self.aplicar_filtros()

    def _infer_column_types(self) -> None:
        for col in self.columnas:
            samples = [self._safe_value(r.get(col)).strip() for r in self.datos_originales if self._safe_value(r.get(col)).strip() != ""]
            if not samples:
                self.column_types[col] = "texto"
                continue
            if all(self._parse_bool(v) is not None for v in samples):
                self.column_types[col] = "booleano"
            elif all(self._parse_number(v) is not None for v in samples):
                self.column_types[col] = "numerico"
            elif all(self._parse_date(v) is not None for v in samples):
                self.column_types[col] = "fecha"
            else:
                self.column_types[col] = "texto"

    def row_matches_filter(self, value: Any, filter_state: FilterState, column_type: str) -> bool:
        text = self._safe_value(value)

        if filter_state.selected_values is not None and text not in filter_state.selected_values:
            return False

        if not filter_state.advanced_enabled:
            return True

        op = filter_state.operator
        v1 = filter_state.value1
        v2 = filter_state.value2

        if column_type == "texto":
            v = text.lower()
            a = v1.lower()
            if op == "contiene":
                return a in v
            if op == "no contiene":
                return a not in v
            if op == "empieza por":
                return v.startswith(a)
            if op == "termina en":
                return v.endswith(a)
            if op == "igual":
                return v == a
            if op == "distinto":
                return v != a
            return True

        if column_type == "numerico":
            n = self._parse_number(text)
            x1 = self._parse_number(v1)
            x2 = self._parse_number(v2)
            if n is None:
                return False
            if op == "=":
                return x1 is not None and n == x1
            if op == "≠":
                return x1 is not None and n != x1
            if op == ">":
                return x1 is not None and n > x1
            if op == "<":
                return x1 is not None and n < x1
            if op == "≥":
                return x1 is not None and n >= x1
            if op == "≤":
                return x1 is not None and n <= x1
            if op == "entre":
                return x1 is not None and x2 is not None and min(x1, x2) <= n <= max(x1, x2)
            return True

        if column_type == "fecha":
            d = self._parse_date(text)
            d1 = self._parse_date(v1)
            d2 = self._parse_date(v2)
            today = datetime.now().date()
            if d is None:
                return False
            if op == "es":
                return d1 is not None and d == d1
            if op == "antes de":
                return d1 is not None and d < d1
            if op == "después de":
                return d1 is not None and d > d1
            if op == "entre":
                return d1 is not None and d2 is not None and min(d1, d2) <= d <= max(d1, d2)
            if op == "hoy":
                return d == today
            if op == "este mes":
                return d.month == today.month and d.year == today.year
            return True

        if column_type == "booleano":
            b = self._parse_bool(text)
            if op == "True":
                return b is True
            if op == "False":
                return b is False
            return True

        return True

    def _row_matches_filters(self, row: dict[str, Any]) -> bool:
        for col in self.columnas:
            raw = row.get(col)
            if not self.row_matches_filter(raw, self.filtros_activos[col], self.column_types.get(col, "texto")):
                return False

            quick = self._entry_vars.get(col)
            quick_text = quick.get().strip().lower() if quick else ""
            if quick_text and quick_text not in self._safe_value(raw).lower():
                return False
        return True

    def _valores_base(self, columna: str) -> list[dict[str, Any]]:
        base: list[dict[str, Any]] = []
        for row in self.datos_originales:
            ok = True
            for col in self.columnas:
                if col == columna:
                    continue
                raw = row.get(col)
                if not self.row_matches_filter(raw, self.filtros_activos[col], self.column_types.get(col, "texto")):
                    ok = False
                    break
                quick = self._entry_vars.get(col)
                quick_text = quick.get().strip().lower() if quick else ""
                if quick_text and quick_text not in self._safe_value(raw).lower():
                    ok = False
                    break
            if ok:
                base.append(row)
        return base

    def _valores_unicos(self, columna: str) -> list[str]:
        values = {self._safe_value(row.get(columna)) for row in self._valores_base(columna)}
        return sorted(values, key=self._sortable_value)

    def aplicar_filtros(self) -> None:
        filtrados = [row for row in self.datos_originales if self._row_matches_filters(row)]

        if self.sort_column:
            filtrados.sort(key=lambda r: self._sortable_value(r.get(self.sort_column)), reverse=self.sort_reverse)

        self._datos_filtrados = filtrados
        self._render_rows(filtrados)
        self._refresh_headings()
        self._notify_counts()

    def limpiar_filtros(self) -> None:
        self.filtros_activos = {col: FilterState() for col in self.columnas}
        self.sort_column = None
        self.sort_reverse = False
        for var in self._entry_vars.values():
            var.set("")
        self.aplicar_filtros()

    def mostrar_filtro_columna(self, columna: str) -> None:
        ColumnFilterPopup(
            parent=self,
            column=self.header_aliases.get(columna, columna),
            values=self._valores_unicos(columna),
            filter_state=self.filtros_activos[columna],
            column_type=self.column_types.get(columna, "texto"),
            on_apply=lambda state: self._apply_column_filter(columna, state),
            on_sort=lambda reverse: self._set_sort(columna, reverse),
        )

    def _apply_column_filter(self, column: str, state: FilterState) -> None:
        self.filtros_activos[column] = state
        self.aplicar_filtros()

    def _set_sort(self, column: str, reverse: bool) -> None:
        self.sort_column = column
        self.sort_reverse = reverse
        self.aplicar_filtros()

    def _render_rows(self, rows: list[dict[str, Any]]) -> None:
        self.tree.delete(*self.tree.get_children())
        for row in rows:
            self.tree.insert("", "end", values=[self._safe_value(row.get(col)) for col in self.columnas])

    def _refresh_headings(self) -> None:
        for col in self.columnas:
            self.tree.heading(col, text=self._build_heading_text(col), command=lambda c=col: self.mostrar_filtro_columna(c))

    def _build_heading_text(self, col: str) -> str:
        base = self.header_aliases.get(col, col)
        if self.sort_column == col:
            return f"{base} {'▼' if self.sort_reverse else '▲'}"
        return base

    def _notify_counts(self) -> None:
        if self.on_counts_changed:
            self.on_counts_changed(len(self._datos_filtrados), len(self.datos_originales))

    def _on_tree_right_click(self, event: tk.Event) -> None:
        if self.tree.identify_region(event.x, event.y) != "heading":
            return
        col_id = self.tree.identify_column(event.x)
        if not col_id:
            return
        idx = int(col_id.replace("#", "")) - 1
        if 0 <= idx < len(self.columnas):
            self.mostrar_filtro_columna(self.columnas[idx])

    @staticmethod
    def _safe_value(value: Any) -> str:
        return "" if value is None else str(value)

    @staticmethod
    def _parse_number(text: str) -> float | None:
        try:
            return float(text.replace(",", "."))
        except Exception:
            return None

    @staticmethod
    def _parse_bool(text: str) -> bool | None:
        t = text.strip().lower()
        if t in {"true", "1", "si", "sí", "yes"}:
            return True
        if t in {"false", "0", "no"}:
            return False
        return None

    @staticmethod
    def _parse_date(text: str) -> date | None:
        raw = text.strip()
        if not raw:
            return None
        for fmt in DATE_FORMATS:
            try:
                return datetime.strptime(raw, fmt).date()
            except ValueError:
                continue
        try:
            return datetime.fromisoformat(raw).date()
        except ValueError:
            return None

    @staticmethod
    def _sortable_value(value: Any) -> tuple[int, Any]:
        if value is None:
            return (3, "")
        text = str(value).strip()
        if text == "":
            return (3, "")
        num = FilterTable._parse_number(text)
        if num is not None:
            return (0, num)
        d = FilterTable._parse_date(text)
        if d is not None:
            return (1, d.toordinal())
        b = FilterTable._parse_bool(text)
        if b is not None:
            return (2, int(b))
        return (4, text.lower())
