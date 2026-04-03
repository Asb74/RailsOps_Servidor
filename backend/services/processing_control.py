"""Control de idempotencia de procesamiento.
Responsabilidad: evitar reprocesar correos ya tratados.
"""

import json
from pathlib import Path

from backend.config import CONTROL_FILE


def cargar_control():
    control_path = Path(CONTROL_FILE)
    if not control_path.exists():
        return {"gmail_ids": []}

    with control_path.open("r", encoding="utf-8") as f:
        return json.load(f)


def guardar_control(data):
    control_path = Path(CONTROL_FILE)
    with control_path.open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=4, ensure_ascii=False)


def ya_procesado(gmail_id):
    data = cargar_control()
    return gmail_id in data["gmail_ids"]


def marcar_procesado(gmail_id):
    data = cargar_control()
    if gmail_id not in data["gmail_ids"]:
        data["gmail_ids"].append(gmail_id)
        guardar_control(data)
