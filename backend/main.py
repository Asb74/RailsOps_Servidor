"""Punto de entrada backend.
Responsabilidad: exponer API operativa y bootstrap opcional de UI de escritorio.
"""

import os
import shutil

from fastapi import FastAPI, File, UploadFile

from backend.config import INPUT_FOLDER
from backend.core.parser_malla import parse_malla
from backend.core.parser_tba import parse_tba
from backend.core.parser_tbp import parse_tbp
from backend.core.parser_velocidades import parse_velocidades
from backend.services.gmail_reader import clasificar_documento, descargar_adjuntos
from backend.services.processing_control import marcar_procesado, ya_procesado
from firebase_service import guardar_documento, guardar_restricciones

app = FastAPI()
os.makedirs(INPUT_FOLDER, exist_ok=True)


@app.post("/upload")
async def upload_file(file: UploadFile = File(...)):
    file_path = os.path.join(INPUT_FOLDER, file.filename)
    with open(file_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    return {"mensaje": "Archivo subido correctamente", "archivo": file.filename}


@app.post("/parse/{filename}")
def parse_file(filename: str):
    file_path = os.path.join(INPUT_FOLDER, filename)
    if not os.path.exists(file_path):
        return {"error": "Archivo no encontrado"}

    data = parse_tba(file_path)
    guardar_restricciones(data)

    return {"archivo": filename, "resultados": data}


@app.get("/procesar_gmail")
def procesar_gmail():
    archivos = descargar_adjuntos()
    resultados_totales = []

    for item in archivos:
        archivo = item["filename"]
        gmail_id = item["gmail_id"]

        if ya_procesado(gmail_id):
            print("Ya procesado:", archivo)
            continue

        tipo = clasificar_documento(archivo)
        print(f"Procesando: {archivo} | Tipo: {tipo}")

        if tipo is None:
            print("Ignorado:", archivo)
            marcar_procesado(gmail_id)
            continue

        doc_id = guardar_documento(archivo, tipo, gmail_id)
        file_path = os.path.join(INPUT_FOLDER, archivo)

        if tipo == "TBA":
            data = parse_tba(file_path)
        elif tipo == "TBP":
            data = parse_tbp(file_path)
        elif tipo == "MALLA":
            data = parse_malla(file_path)
        elif tipo == "VELOCIDADES":
            data = parse_velocidades(file_path)
        else:
            data = []

        if data:
            for r in data:
                r["documento_id"] = doc_id
                r["archivo"] = archivo
            guardar_restricciones(data)
            resultados_totales.extend(data)
        else:
            print("Sin datos parseados:", archivo)

        marcar_procesado(gmail_id)

    return {
        "archivos_procesados": [a["filename"] for a in archivos],
        "total_archivos": len(archivos),
        "resultados": resultados_totales,
    }
