"""Vista de tablas.
Responsabilidad: mostrar datos tabulares importados (TBA/TBP/velocidades).
"""

import tkinter as tk
from tkinter import ttk


class TablasView(ttk.Frame):
    def __init__(self, parent):
        super().__init__(parent)
        ttk.Label(self, text="Tablas operativas", font=("Segoe UI", 12, "bold")).pack(
            anchor="w", padx=12, pady=12
        )
        self.tree = ttk.Treeview(self, columns=("col1", "col2"), show="headings")
        self.tree.heading("col1", text="Campo")
        self.tree.heading("col2", text="Valor")
        self.tree.pack(fill=tk.BOTH, expand=True, padx=12, pady=12)
