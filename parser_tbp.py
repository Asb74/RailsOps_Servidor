import pdfplumber
import re


def parse_tbp(pdf_path):
    resultados = []

    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            texto = page.extract_text()

            if not texto:
                continue

            # 🔥 PK + velocidad típica
            matches = re.findall(r"(\d+\.\d+)\s+(\d+)", texto)

            for m in matches:
                resultados.append({
                    "pk": float(m[0]),
                    "velocidad": int(m[1])
                })

    return resultados