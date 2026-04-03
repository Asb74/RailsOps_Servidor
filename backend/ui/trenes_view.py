"""Vista de trenes.
Responsabilidad: albergar interacción de gestión/consulta de trenes.
"""

from tkinter import ttk


class TrenesView(ttk.Frame):
    def __init__(self, parent):
        super().__init__(parent)
        ttk.Label(self, text="Gestión de trenes (en construcción)").pack(
            anchor="w", padx=12, pady=12
        )
