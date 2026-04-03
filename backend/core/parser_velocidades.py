"""Parser de cuadros de velocidades.
Responsabilidad: extraer límites de velocidad por PK.
"""

import re

import pdfplumber


def parse_velocidades(pdf_path):
    resultados = []

    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            texto = page.extract_text()
            if not texto:
                continue

            matches = re.findall(r"(\d+\.\d+)\s+(\d+)", texto)
            for pk, vel_max in matches:
                resultados.append({"pk": float(pk), "velocidad_max": int(vel_max)})

    return resultados
