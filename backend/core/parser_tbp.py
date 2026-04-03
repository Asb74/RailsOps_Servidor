"""Parser de boletines TBP.
Responsabilidad: extraer pares PK/velocidad desde PDFs TBP.
"""

import re

import pdfplumber


def parse_tbp(pdf_path):
    resultados = []

    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            texto = page.extract_text()
            if not texto:
                continue

            matches = re.findall(r"(\d+\.\d+)\s+(\d+)", texto)
            for pk, velocidad in matches:
                resultados.append({"pk": float(pk), "velocidad": int(velocidad)})

    return resultados
