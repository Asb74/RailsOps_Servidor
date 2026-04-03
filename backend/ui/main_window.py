"""Ventana principal Tkinter.
Responsabilidad: orquestar vistas de la app de escritorio RailOps.
"""

import tkinter as tk
from tkinter import ttk

from backend.ui.conflictos_view import ConflictosView
from backend.ui.tablas_view import TablasView
from backend.ui.trenes_view import TrenesView


class MainWindow(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("RailOps")
        self.geometry("1024x720")

        tabs = ttk.Notebook(self)
        tabs.pack(fill="both", expand=True)

        self.tablas_view = TablasView(tabs)
        self.trenes_view = TrenesView(tabs)
        self.conflictos_view = ConflictosView(tabs)

        tabs.add(self.tablas_view, text="Tablas")
        tabs.add(self.trenes_view, text="Trenes")
        tabs.add(self.conflictos_view, text="Conflictos")
