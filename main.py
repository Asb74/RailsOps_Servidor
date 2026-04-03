"""Compatibilidad de entrada.
Responsabilidad: mantener el punto de entrada histórico delegando al backend modular.
"""

from backend.main import app  # re-export para uvicorn/gunicorn


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("backend.main:app", host="0.0.0.0", port=8000, reload=True)
