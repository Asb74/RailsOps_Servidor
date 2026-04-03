"""Configuración central de RailOps.
Responsabilidad: definir rutas y parámetros de entorno compartidos.
"""

from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
INPUT_FOLDER = BASE_DIR / "data" / "input"
CONTROL_FILE = BASE_DIR / "control_procesados.json"

# Mantener compatibilidad con despliegues actuales en Windows.
FIREBASE_CREDENTIALS = r"C:\RailOps\railops.json"
GMAIL_CREDENTIALS = r"C:\RailOps\client_secret.json"
