"""Compatibilidad de entrada.
Responsabilidad: mantener backend FastAPI y habilitar UI de escritorio Tkinter.
"""

from __future__ import annotations

import argparse

from backend.main import app  # re-export para uvicorn/gunicorn
from backend.core.motor_conflictos import detectar_conflictos
from backend.db import sqlite_service


def run_desktop() -> None:
    from backend.ui.main_window import MainWindow

    window = MainWindow()
    window.mainloop()


def run_api() -> None:
    import uvicorn

    uvicorn.run("backend.main:app", host="0.0.0.0", port=8000, reload=True)


def run_conflicts(tren: str | None = None) -> None:
    """Ejemplo de uso del motor de conflictos desde la entrada principal."""
    conflictos = detectar_conflictos(sqlite_service, tren=tren)
    print(f"Conflictos detectados: {len(conflictos)}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="RailOps entrypoint")
    parser.add_argument(
        "--ui",
        action="store_true",
        help="Lanza la aplicación de escritorio Tkinter en lugar de la API FastAPI",
    )
    parser.add_argument(
        "--recalcular-conflictos",
        action="store_true",
        help="Ejecuta el motor de conflictos sobre SQLite y finaliza",
    )
    parser.add_argument(
        "--tren",
        type=str,
        default=None,
        help="Filtro opcional de tren para recalcular conflictos",
    )
    args = parser.parse_args()

    if args.recalcular_conflictos:
        run_conflicts(tren=args.tren)
    elif args.ui:
        run_desktop()
    else:
        run_api()
