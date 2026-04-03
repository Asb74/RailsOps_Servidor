"""Parser de boletines TBA.
Responsabilidad: extraer restricciones operativas desde PDFs TBA y persistir en SQLite.
"""

from __future__ import annotations

import logging
import re
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import pdfplumber

logger = logging.getLogger(__name__)


def _limpiar_texto_base(texto: Any) -> str | None:
    if texto is None:
        return None

    texto_limpio = str(texto).replace("\n", " ")
    texto_limpio = re.sub(r"\s+", " ", texto_limpio).strip()

    return texto_limpio or None


def limpiar_estacion(texto: Any) -> str | None:
    texto_limpio = _limpiar_texto_base(texto)
    if not texto_limpio:
        return texto_limpio

    texto_limpio = re.sub(r"\d{5}\s*-\s*", "", texto_limpio)
    texto_limpio = texto_limpio.rstrip(".").strip()

    return texto_limpio or None


def limpiar_tipo(texto: Any) -> str | None:
    texto_limpio = _limpiar_texto_base(texto)
    if not texto_limpio:
        return texto_limpio

    return texto_limpio.upper()


def limpiar_periodicidad(texto: Any) -> str | None:
    return _limpiar_texto_base(texto)


def limpiar_vias(texto: Any) -> str | None:
    return _limpiar_texto_base(texto)


def _parse_float(valor: Any) -> float | None:
    if valor is None:
        return None

    texto = str(valor).strip().replace(",", ".")
    if not texto:
        return None

    try:
        return float(texto)
    except ValueError:
        return None


def parse_tba(pdf_path: str) -> list[dict[str, Any]]:
    """Parsea un PDF TBA y retorna una lista de restricciones.

    Esta función es pura: no realiza persistencia ni llamadas a servicios externos.
    """
    resultados: list[dict[str, Any]] = []

    try:
        with pdfplumber.open(pdf_path) as pdf:
            linea_base = None
            estacion_inicio_base = None
            estacion_fin_base = None
            pk_inicio_base = None
            pk_fin_base = None
            vias_base = None

            for page_idx, page in enumerate(pdf.pages, start=1):
                tablas = page.extract_tables() or []

                for tabla_idx, tabla in enumerate(tablas, start=1):
                    if not tabla:
                        continue

                    for fila_idx, fila in enumerate(tabla, start=1):
                        if not fila:
                            continue

                        fila_normalizada = list(fila)
                        fila_str = " ".join(str(celda) for celda in fila_normalizada if celda)

                        if "BRAZATORTAS" in fila_str and "GUADALMEZ" in fila_str:
                            try:
                                linea_base = _limpiar_texto_base(fila_normalizada[0])
                                estacion_inicio_base = limpiar_estacion(fila_normalizada[1])
                                pk_inicio_base = _parse_float(fila_normalizada[2])
                                estacion_fin_base = limpiar_estacion(fila_normalizada[4])
                                pk_fin_base = _parse_float(fila_normalizada[5])
                                vias_base = limpiar_vias(fila_normalizada[7] if len(fila_normalizada) > 7 else None)
                            except Exception as exc:  # pragma: no cover - defensivo por formato variable PDF
                                logger.warning(
                                    "No se pudo leer cabecera base TBA en página=%s tabla=%s fila=%s: %s",
                                    page_idx,
                                    tabla_idx,
                                    fila_idx,
                                    exc,
                                )

                        primera_celda = fila_normalizada[0] if len(fila_normalizada) > 0 else None
                        if primera_celda and "Corte" in str(primera_celda):
                            try:
                                tipo = limpiar_tipo(primera_celda)
                                fecha_inicio = _limpiar_texto_base(fila_normalizada[3] if len(fila_normalizada) > 3 else None)
                                hora_inicio = _limpiar_texto_base(fila_normalizada[4] if len(fila_normalizada) > 4 else None)
                                fecha_fin = _limpiar_texto_base(fila_normalizada[5] if len(fila_normalizada) > 5 else None)
                                hora_fin = _limpiar_texto_base(fila_normalizada[6] if len(fila_normalizada) > 6 else None)
                                periodicidad = limpiar_periodicidad(fila_normalizada[7] if len(fila_normalizada) > 7 else None)
                                linea_det = _limpiar_texto_base(fila_normalizada[8] if len(fila_normalizada) > 8 else None)
                                punto_inicio = limpiar_estacion(fila_normalizada[9] if len(fila_normalizada) > 9 else None)
                                punto_fin = limpiar_estacion(fila_normalizada[10] if len(fila_normalizada) > 10 else None)
                                vias = limpiar_vias(fila_normalizada[11] if len(fila_normalizada) > 11 else vias_base)

                                resultados.append(
                                    {
                                        "linea": linea_det or linea_base,
                                        "estacion_inicio": punto_inicio or estacion_inicio_base,
                                        "estacion_fin": punto_fin or estacion_fin_base,
                                        "pk_inicio": pk_inicio_base,
                                        "pk_fin": pk_fin_base,
                                        "tipo": tipo,
                                        "fecha_inicio": fecha_inicio,
                                        "hora_inicio": hora_inicio,
                                        "fecha_fin": fecha_fin,
                                        "hora_fin": hora_fin,
                                        "periodicidad": periodicidad,
                                        "vias": vias,
                                    }
                                )
                            except Exception as exc:  # pragma: no cover - tolerancia a filas corruptas
                                logger.warning(
                                    "Fila TBA omitida por error en página=%s tabla=%s fila=%s: %s",
                                    page_idx,
                                    tabla_idx,
                                    fila_idx,
                                    exc,
                                )
    except Exception as exc:
        logger.exception("Error general parseando PDF TBA '%s': %s", pdf_path, exc)

    return resultados


def procesar_tba(pdf_path: str, documento_id: int, sqlite_service: Any) -> list[dict[str, Any]]:
    """Parsea TBA e inserta resultados en SQLite (tabla `tba`)."""
    resultados = parse_tba(pdf_path)
    nombre_pdf = Path(pdf_path).name

    for idx, restriccion in enumerate(resultados, start=1):
        try:
            if not isinstance(restriccion, Mapping):
                logger.warning("Resultado TBA no válido en índice %s: %r", idx, restriccion)
                continue

            sqlite_service.insertar_tba(
                documento_id=documento_id,
                linea=restriccion.get("linea") or "DESCONOCIDA",
                estacion_inicio=restriccion.get("estacion_inicio"),
                estacion_fin=restriccion.get("estacion_fin"),
                pk_inicio=restriccion.get("pk_inicio"),
                pk_fin=restriccion.get("pk_fin"),
                fecha_inicio=restriccion.get("fecha_inicio"),
                hora_inicio=restriccion.get("hora_inicio"),
                fecha_fin=restriccion.get("fecha_fin"),
                hora_fin=restriccion.get("hora_fin"),
                tipo=limpiar_tipo(restriccion.get("tipo")),
                periodicidad=limpiar_periodicidad(restriccion.get("periodicidad")),
                vias=limpiar_vias(restriccion.get("vias")),
                archivo=nombre_pdf,
            )
            if isinstance(restriccion, dict):
                restriccion["archivo"] = nombre_pdf
        except Exception as exc:  # pragma: no cover - no romper procesamiento por una fila
            logger.warning(
                "No se pudo insertar restricción TBA idx=%s para documento_id=%s: %s",
                idx,
                documento_id,
                exc,
            )

    return resultados
