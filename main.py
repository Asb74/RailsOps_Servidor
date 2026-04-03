from fastapi import FastAPI, UploadFile, File
import shutil
import os

# Parsers
from parser_tba import parse_tba
from parser_tbp import parse_tbp
from parser_malla import parse_malla
from parser_velocidades import parse_velocidades

# Config y servicios
from config import INPUT_FOLDER
from firebase_service import guardar_restricciones, guardar_documento

# Gmail + control
from gmail_reader import descargar_adjuntos, clasificar_documento
from control_local import ya_procesado, marcar_procesado

app = FastAPI()

# Crear carpeta si no existe
os.makedirs(INPUT_FOLDER, exist_ok=True)


# -----------------------------
# 🔹 SUBIDA MANUAL
# -----------------------------
@app.post("/upload")
async def upload_file(file: UploadFile = File(...)):
    file_path = os.path.join(INPUT_FOLDER, file.filename)

    with open(file_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    return {
        "mensaje": "Archivo subido correctamente",
        "archivo": file.filename
    }


# -----------------------------
# 🔹 PARSEO MANUAL
# -----------------------------
@app.post("/parse/{filename}")
def parse_file(filename: str):
    file_path = os.path.join(INPUT_FOLDER, filename)

    if not os.path.exists(file_path):
        return {"error": "Archivo no encontrado"}

    # Solo TBA manual por ahora
    data = parse_tba(file_path)

    guardar_restricciones(data)

    return {
        "archivo": filename,
        "resultados": data
    }


# -----------------------------
# 🔥 PROCESO AUTOMÁTICO GMAIL
# -----------------------------
@app.get("/procesar_gmail")
def procesar_gmail():

    archivos = descargar_adjuntos()

    resultados_totales = []

    for item in archivos:

        archivo = item["filename"]
        gmail_id = item["gmail_id"]

        # 🔹 EVITAR DUPLICADOS (LOCAL)
        if ya_procesado(gmail_id):
            print("Ya procesado:", archivo)
            continue

        tipo = clasificar_documento(archivo)

        print(f"Procesando: {archivo} | Tipo: {tipo}")

        # 🔥 IGNORAR ARCHIVOS NO ÚTILES
        if tipo is None:
            print("Ignorado:", archivo)
            marcar_procesado(gmail_id)
            continue

        # 🔹 Guardar documento
        doc_id = guardar_documento(archivo, tipo, gmail_id)

        file_path = os.path.join(INPUT_FOLDER, archivo)

        # 🔥 SELECCIÓN DE PARSER
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

        # 🔹 GUARDAR RESULTADOS SI HAY DATOS
        if data:
            for r in data:
                r["documento_id"] = doc_id
                r["archivo"] = archivo

            guardar_restricciones(data)
            resultados_totales.extend(data)

        else:
            print("Sin datos parseados:", archivo)

        # 🔥 MARCAR COMO PROCESADO
        marcar_procesado(gmail_id)

    return {
        "archivos_procesados": [a["filename"] for a in archivos],
        "total_archivos": len(archivos),
        "resultados": resultados_totales
    }