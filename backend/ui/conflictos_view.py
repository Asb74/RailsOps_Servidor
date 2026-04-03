"""Vista de conflictos.
Responsabilidad: presentar conflictos detectados por el motor de reglas.
"""

from tkinter import ttk


class ConflictosView(ttk.Frame):
    def __init__(self, parent):
        super().__init__(parent)
        ttk.Label(self, text="Conflictos operativos (en construcción)").pack(
            anchor="w", padx=12, pady=12
        )
