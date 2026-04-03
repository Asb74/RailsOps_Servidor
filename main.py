"""Compatibilidad de entrada.
Responsabilidad: mantener backend FastAPI y habilitar UI de escritorio Tkinter.
"""

from __future__ import annotations

import argparse

from backend.main import app  # re-export para uvicorn/gunicorn


def run_desktop() -> None:
    from backend.ui.main_window import MainWindow

    window = MainWindow()
    window.mainloop()


def run_api() -> None:
    import uvicorn

    uvicorn.run("backend.main:app", host="0.0.0.0", port=8000, reload=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="RailOps entrypoint")
    parser.add_argument(
        "--ui",
        action="store_true",
        help="Lanza la aplicación de escritorio Tkinter en lugar de la API FastAPI",
    )
    args = parser.parse_args()

    if args.ui:
        run_desktop()
    else:
        run_api()
