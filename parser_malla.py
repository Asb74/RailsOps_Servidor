import pdfplumber
import re


def parse_malla(pdf_path):
    resultados = []

    with pdfplumber.open(pdf_path) as pdf:

        for page in pdf.pages:
            texto = page.extract_text()

            if not texto:
                continue

            # 🔥 detectar estaciones + horas
            lineas = texto.split("\n")

            for l in lineas:

                match = re.search(r"([A-Z\-]+)\s+(\d{2}:\d{2})", l)

                if match:
                    resultados.append({
                        "estacion": match.group(1),
                        "hora": match.group(2)
                    })

    return resultados