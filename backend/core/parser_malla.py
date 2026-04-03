"""Parser de malla/hoja de ruta.
Responsabilidad: extraer pasos de tren (estación + hora) desde PDFs de marcha.
"""

from __future__ import annotations

import logging
import re
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import pdfplumber

logger = logging.getLogger(__name__)


_RE_ESPACIOS = re.compile(r"\s+")
_RE_HORA = re.compile(r"\b([01]?\d|2[0-3]):[0-5]\d\b")
_RE_PK = re.compile(r"\b(?:pk|p\.k\.|km)\s*[:\-]?\s*(\d+(?:[\.,]\d+)?)\b", re.I)


def _limpiar_texto_base(texto: Any) -> str | None:
    if texto is None:
        return None

    limpio = str(texto).replace("\n", " ")
    limpio = _RE_ESPACIOS.sub(" ", limpio).strip()
    return limpio or None


def limpiar_nombre_estacion(texto: Any) -> str | None:
    """Normaliza nombres de estación para extracción homogénea."""
    estacion = _limpiar_texto_base(texto)
    if not estacion:
        return estacion

    estacion = re.sub(r"^\d{3,6}\s*[-–—]\s*", "", estacion)
    estacion = re.sub(r"\b(?:est\.?)\s+", "", estacion, flags=re.I)
    estacion = re.sub(r"\s*\([^\)]*\)\s*$", "", estacion)
    estacion = estacion.strip(" .;-:")

    return estacion.upper() if estacion else None


def _parse_pk(texto: Any) -> float | None:
    if texto is None:
        return None

    match = _RE_PK.search(str(texto))
    if not match:
        return None

    try:
        return float(match.group(1).replace(",", "."))
    except ValueError:
        return None


def _detectar_cabecera(texto_completo: str) -> dict[str, str | None]:
    """Detecta datos globales del tren: tren, línea, origen y destino."""
    cabecera: dict[str, str | None] = {
        "tren": None,
        "linea": None,
        "origen": None,
        "destino": None,
    }

    patrones = {
        "tren": [
            r"\b(?:tren|circulacion|circulación|tren n[ºo°]?|n[ºo°]? tren)\s*[:\-]?\s*([A-Z0-9\-/]{2,})",
            r"\bN[ºo°]?\s*([0-9]{3,6})\b",
        ],
        "linea": [
            r"\b(?:linea|línea)\s*[:\-]?\s*([^\n|;]{2,})",
        ],
        "origen": [
            r"\b(?:origen|desde|salida)\s*[:\-]?\s*([^\n|;]{2,})",
        ],
        "destino": [
            r"\b(?:destino|hasta|llegada)\s*[:\-]?\s*([^\n|;]{2,})",
        ],
    }

    for campo, regs in patrones.items():
        for regex in regs:
            m = re.search(regex, texto_completo, re.I)
            if not m:
                continue

            valor = _limpiar_texto_base(m.group(1))
            if not valor:
                continue

            if campo in {"origen", "destino"}:
                valor = limpiar_nombre_estacion(valor)

            cabecera[campo] = valor
            break

    # Fallback habitual: "ORIGEN - DESTINO" en una misma línea.
    if not cabecera["origen"] or not cabecera["destino"]:
        m_ruta = re.search(
            r"\b([A-ZÁÉÍÓÚÑ][A-ZÁÉÍÓÚÑ\s\-']{2,})\s*[\-–—/]\s*([A-ZÁÉÍÓÚÑ][A-ZÁÉÍÓÚÑ\s\-']{2,})\b",
            texto_completo,
        )
        if m_ruta:
            cabecera["origen"] = cabecera["origen"] or limpiar_nombre_estacion(m_ruta.group(1))
            cabecera["destino"] = cabecera["destino"] or limpiar_nombre_estacion(m_ruta.group(2))

    return cabecera


def _parse_fila_estacion_hora(linea: str) -> tuple[str | None, str | None, float | None]:
    hora_match = _RE_HORA.search(linea)
    if not hora_match:
        return None, None, None

    hora = hora_match.group(0)
    pk = _parse_pk(linea)

    prefijo = linea[: hora_match.start()]
    sufijo = linea[hora_match.end() :]
    sin_pk = _RE_PK.sub("", f"{prefijo} {sufijo}")

    tokens_ruido = (
        "paso",
        "llegada",
        "salida",
        "hora",
        "orden",
        "tren",
        "linea",
        "línea",
        "origen",
        "destino",
    )
    candidato = _limpiar_texto_base(sin_pk)
    if not candidato:
        return None, None, pk

    if any(t in candidato.lower() for t in tokens_ruido):
        return None, None, pk

    estacion = limpiar_nombre_estacion(candidato)
    if not estacion:
        return None, None, pk

    return estacion, hora, pk


def parse_malla(pdf_path: str) -> list[dict[str, Any]]:
    """Parsea un PDF de malla/hoja de ruta.

    Retorna lista de pasos con claves:
    tren, linea, origen, destino, estacion, hora, orden, pk, documento_id.
    """
    resultados: list[dict[str, Any]] = []

    try:
        with pdfplumber.open(pdf_path) as pdf:
            paginas_texto = [(page.extract_text() or "") for page in pdf.pages]

        texto_completo = "\n".join(paginas_texto)
        cabecera = _detectar_cabecera(texto_completo)

        orden = 1
        vistos: set[tuple[str, str]] = set()

        for texto in paginas_texto:
            if not texto:
                continue

            for linea in texto.split("\n"):
                estacion, hora, pk = _parse_fila_estacion_hora(linea)
                if not estacion or not hora:
                    continue

                dedupe_key = (estacion, hora)
                if dedupe_key in vistos:
                    continue
                vistos.add(dedupe_key)

                resultados.append(
                    {
                        "tren": cabecera.get("tren"),
                        "linea": cabecera.get("linea"),
                        "origen": cabecera.get("origen"),
                        "destino": cabecera.get("destino"),
                        "estacion": estacion,
                        "hora": hora,
                        "orden": orden,
                        "pk": pk,
                        "documento_id": None,
                    }
                )
                orden += 1
    except Exception as exc:
        logger.exception("Error general parseando PDF MALLA '%s': %s", pdf_path, exc)

    return resultados


def procesar_malla(pdf_path: str, documento_id: int, sqlite_service: Any) -> list[dict[str, Any]]:
    """Parsea MALLA e inserta pasos en SQLite (tabla `mallas`)."""
    resultados = parse_malla(pdf_path)
    nombre_pdf = Path(pdf_path).name

    for idx, paso in enumerate(resultados, start=1):
        try:
            if not isinstance(paso, Mapping):
                logger.warning("Resultado MALLA no válido en índice %s: %r", idx, paso)
                continue

            paso_con_doc = dict(paso)
            paso_con_doc["documento_id"] = documento_id

            sqlite_service.insertar_malla(
                documento_id=documento_id,
                tren=paso_con_doc.get("tren") or "DESCONOCIDO",
                linea=paso_con_doc.get("linea") or "",
                estacion=paso_con_doc.get("estacion"),
                pk=paso_con_doc.get("pk"),
                hora=paso_con_doc.get("hora"),
                orden=paso_con_doc.get("orden"),
                archivo=nombre_pdf,
            )

            paso_con_doc["archivo"] = nombre_pdf
            resultados[idx - 1] = paso_con_doc
        except Exception as exc:  # pragma: no cover - tolerancia por fila
            logger.warning(
                "No se pudo insertar paso MALLA idx=%s para documento_id=%s: %s",
                idx,
                documento_id,
                exc,
            )

    return resultados
