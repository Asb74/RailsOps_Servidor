import json
import os

CONTROL_FILE = "control_procesados.json"


def cargar_control():
    if not os.path.exists(CONTROL_FILE):
        return {"gmail_ids": []}

    with open(CONTROL_FILE, "r") as f:
        return json.load(f)


def guardar_control(data):
    with open(CONTROL_FILE, "w") as f:
        json.dump(data, f, indent=4)


def ya_procesado(gmail_id):
    data = cargar_control()
    return gmail_id in data["gmail_ids"]


def marcar_procesado(gmail_id):
    data = cargar_control()

    if gmail_id not in data["gmail_ids"]:
        data["gmail_ids"].append(gmail_id)
        guardar_control(data)