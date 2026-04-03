"""Punto de entrada backend.
Responsabilidad: exponer API operativa y bootstrap opcional de UI de escritorio.
"""

import os
import shutil

from fastapi import FastAPI, File, UploadFile

from backend.config import INPUT_FOLDER
from backend.core.parser_malla import parse_malla
from backend.core.parser_tba import parse_tba, procesar_tba
from backend.core.parser_tbp import parse_tbp
from backend.core.parser_velocidades import parse_velocidades
from backend.db import sqlite_service
from backend.services.ingest_service import ejecutar_ingestion_gmail
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


@app.post("/parse_tba_sqlite/{filename}")
def parse_tba_sqlite(filename: str):
    """Ejemplo de uso coordinado: parseo TBA + inserción en SQLite."""
    file_path = os.path.join(INPUT_FOLDER, filename)
    if not os.path.exists(file_path):
        return {"error": "Archivo no encontrado"}

    documento_id = sqlite_service.insertar_documento(nombre=filename, tipo="TBA")
    data = procesar_tba(file_path, documento_id, sqlite_service)

    return {
        "archivo": filename,
        "documento_id": documento_id,
        "restricciones_insertadas": len(data),
    }


@app.get("/procesar_gmail")
def procesar_gmail():
    """Ejecuta ingestión automática Gmail -> SQLite (sin Firebase)."""
    return ejecutar_ingestion_gmail()
