"""Parser de boletines TBA.
Responsabilidad: extraer restricciones operativas desde PDFs TBA.
"""

import re

import pdfplumber


def limpiar_estacion(texto):
    if not texto:
        return texto

    texto = re.sub(r"\d{5}\s*-\s*", "", texto)
    texto = texto.replace("\n", "")
    texto = re.sub(r"\s+", " ", texto)
    texto = texto.rstrip(".")

    return texto.strip()


def limpiar_tipo(texto):
    if not texto:
        return texto

    texto = texto.replace("\n", " ")
    texto = re.sub(r"\s+", " ", texto)

    return texto.strip()


def parse_tba(pdf_path):
    resultados = []

    with pdfplumber.open(pdf_path) as pdf:
        linea = None
        estacion_inicio = None
        estacion_fin = None
        pk_inicio = None
        pk_fin = None
        vias_base = None

        for page in pdf.pages:
            tablas = page.extract_tables()

            for tabla in tablas:
                for fila in tabla:
                    if not fila:
                        continue

                    fila_str = " ".join([str(c) for c in fila if c])

                    if "BRAZATORTAS" in fila_str and "GUADALMEZ" in fila_str:
                        try:
                            linea = fila[0]
                            estacion_inicio = limpiar_estacion(fila[1])
                            pk_inicio = float(fila[2])
                            estacion_fin = limpiar_estacion(fila[4])
                            pk_fin = float(fila[5])
                            vias_base = fila[7] if len(fila) > 7 else None
                        except Exception:
                            pass

                    if fila[0] and "Corte" in str(fila[0]):
                        try:
                            tipo = limpiar_tipo(str(fila[0]).upper())
                            fecha_inicio = fila[3]
                            hora_inicio = fila[4]
                            fecha_fin = fila[5]
                            hora_fin = fila[6]
                            periodicidad = fila[7]
                            linea_det = fila[8]
                            punto_inicio = limpiar_estacion(fila[9])
                            punto_fin = limpiar_estacion(fila[10])
                            vias = fila[11] if len(fila) > 11 else vias_base

                            resultados.append(
                                {
                                    "linea": linea_det if linea_det else linea,
                                    "estacion_inicio": punto_inicio if punto_inicio else estacion_inicio,
                                    "estacion_fin": punto_fin if punto_fin else estacion_fin,
                                    "pk_inicio": pk_inicio,
                                    "pk_fin": pk_fin,
                                    "tipo": tipo,
                                    "fecha_inicio": fecha_inicio,
                                    "hora_inicio": hora_inicio,
                                    "fecha_fin": fecha_fin,
                                    "hora_fin": hora_fin,
                                    "periodicidad": periodicidad,
                                    "vias": vias,
                                }
                            )
                        except Exception:
                            pass

    return resultados
