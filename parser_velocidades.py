import pdfplumber
import re


def parse_velocidades(pdf_path):
    resultados = []

    with pdfplumber.open(pdf_path) as pdf:

        for page in pdf.pages:
            texto = page.extract_text()

            if not texto:
                continue

            matches = re.findall(r"(\d+\.\d+)\s+(\d+)", texto)

            for m in matches:
                resultados.append({
                    "pk": float(m[0]),
                    "velocidad_max": int(m[1])
                })

    return resultados