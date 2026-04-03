"""Parser de malla/hoja de ruta.
Responsabilidad: extraer hitos de estación y hora desde documentos operativos.
"""

import re

import pdfplumber


def parse_malla(pdf_path):
    resultados = []

    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            texto = page.extract_text()
            if not texto:
                continue

            for linea in texto.split("\n"):
                match = re.search(r"([A-Z\-]+)\s+(\d{2}:\d{2})", linea)
                if match:
                    resultados.append(
                        {"estacion": match.group(1), "hora": match.group(2)}
                    )

    return resultados
