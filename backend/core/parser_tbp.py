"""Parser de boletines TBP.
Responsabilidad: extraer restricciones operativas desde PDFs TBP y persistir en SQLite.
"""

from __future__ import annotations

import logging
import re
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import pdfplumber

from backend.core.parser_tba import (
    _limpiar_texto_base,
    limpiar_estacion,
    limpiar_periodicidad,
    limpiar_tipo,
    limpiar_vias,
)

logger = logging.getLogger(__name__)


# Dependiente de estructura tabular: alias de cabeceras esperadas en tablas TBP.
HEADER_ALIASES: dict[str, tuple[str, ...]] = {
    "linea": ("linea", "línea"),
    "estacion_inicio": (
        "estacion inicio",
        "estación inicio",
        "desde",
        "origen",
        "punto inicio",
    ),
    "estacion_fin": (
        "estacion fin",
        "estación fin",
        "hasta",
        "destino",
        "punto fin",
    ),
    "pk_inicio": ("pk inicio", "p.k. inicio", "pk desde", "km inicio"),
    "pk_fin": ("pk fin", "p.k. fin", "pk hasta", "km fin"),
    "fecha_inicio": ("fecha inicio", "inicio fecha", "f. inicio", "fecha desde"),
    "hora_inicio": ("hora inicio", "inicio hora", "h. inicio", "hora desde"),
    "fecha_fin": ("fecha fin", "fin fecha", "f. fin", "fecha hasta"),
    "hora_fin": ("hora fin", "fin hora", "h. fin", "hora hasta"),
    "tipo": ("tipo", "tipo restriccion", "tipo restricción", "naturaleza"),
    "periodicidad": ("periodicidad", "dias", "días", "vigencia"),
    "vias": ("vias", "vías", "via", "vía"),
    "velocidad_limitada": (
        "velocidad limitada",
        "v limitada",
        "velocidad",
        "limitacion velocidad",
        "limitación velocidad",
        "vl",
    ),
}


def _normalizar_header(texto: Any) -> str:
    base = _limpiar_texto_base(texto) or ""
    base = base.lower()
    base = base.replace("á", "a").replace("é", "e").replace("í", "i").replace("ó", "o").replace("ú", "u")
    return re.sub(r"[^a-z0-9 ]+", " ", base).strip()


def _parse_float(valor: Any) -> float | None:
    if valor is None:
        return None

    texto = str(valor).strip().replace(",", ".")
    if not texto:
        return None

    m = re.search(r"-?\d+(?:\.\d+)?", texto)
    if not m:
        return None

    try:
        return float(m.group(0))
    except ValueError:
        return None


def _row_base() -> dict[str, Any]:
    return {
        "linea": None,
        "estacion_inicio": None,
        "estacion_fin": None,
        "pk_inicio": None,
        "pk_fin": None,
        "fecha_inicio": None,
        "hora_inicio": None,
        "fecha_fin": None,
        "hora_fin": None,
        "tipo": None,
        "periodicidad": None,
        "vias": None,
        "velocidad_limitada": None,
    }


def _mapear_headers(fila: list[Any]) -> dict[str, int]:
    mapping: dict[str, int] = {}
    for idx, celda in enumerate(fila):
        celda_norm = _normalizar_header(celda)
        if not celda_norm:
            continue

        for field, aliases in HEADER_ALIASES.items():
            if field in mapping:
                continue
            if any(alias in celda_norm for alias in aliases):
                mapping[field] = idx
                break

    return mapping


def _extract_by_tables(page: pdfplumber.page.Page) -> list[dict[str, Any]]:
    """Dependiente de estructura tabular: intenta extraer TBP desde tablas."""
    resultados: list[dict[str, Any]] = []
    tablas = page.extract_tables() or []

    for tabla in tablas:
        if not tabla:
            continue

        header_idx = None
        header_map: dict[str, int] = {}
        for idx, fila in enumerate(tabla):
            if not fila:
                continue
            mapping = _mapear_headers(list(fila))
            # Umbral heurístico mínimo para considerar la fila como cabecera.
            if len(mapping) >= 3 and ("pk_inicio" in mapping or "velocidad_limitada" in mapping or "tipo" in mapping):
                header_idx = idx
                header_map = mapping
                break

        if header_idx is None:
            continue

        for fila in tabla[header_idx + 1 :]:
            if not fila:
                continue

            row = _row_base()

            for campo, col_idx in header_map.items():
                valor = fila[col_idx] if col_idx < len(fila) else None
                if campo in {"pk_inicio", "pk_fin", "velocidad_limitada"}:
                    row[campo] = _parse_float(valor)
                elif campo in {"estacion_inicio", "estacion_fin"}:
                    row[campo] = limpiar_estacion(valor)
                elif campo == "tipo":
                    row[campo] = limpiar_tipo(valor)
                elif campo == "periodicidad":
                    row[campo] = limpiar_periodicidad(valor)
                elif campo == "vias":
                    row[campo] = limpiar_vias(valor)
                else:
                    row[campo] = _limpiar_texto_base(valor)

            # Heurística de descarte: evita filas vacías o separadores.
            if not any(row.values()):
                continue

            resultados.append(row)

    return resultados


def _extract_by_text(texto: str) -> list[dict[str, Any]]:
    """Heurístico sobre texto libre cuando no hay tabla reconocible."""
    resultados: list[dict[str, Any]] = []
    if not texto:
        return resultados

    bloques = [b.strip() for b in re.split(r"\n\s*\n", texto) if b.strip()]
    for bloque in bloques:
        row = _row_base()
        low = bloque.lower()

        # Heurística por etiquetas explícitas.
        row["linea"] = _limpiar_texto_base(re.search(r"(?:linea|línea)\s*[:\-]\s*([^\n]+)", bloque, re.I).group(1)) if re.search(r"(?:linea|línea)\s*[:\-]\s*([^\n]+)", bloque, re.I) else None
        row["estacion_inicio"] = limpiar_estacion(re.search(r"(?:desde|origen|estacion inicio|estación inicio)\s*[:\-]\s*([^\n]+)", bloque, re.I).group(1)) if re.search(r"(?:desde|origen|estacion inicio|estación inicio)\s*[:\-]\s*([^\n]+)", bloque, re.I) else None
        row["estacion_fin"] = limpiar_estacion(re.search(r"(?:hasta|destino|estacion fin|estación fin)\s*[:\-]\s*([^\n]+)", bloque, re.I).group(1)) if re.search(r"(?:hasta|destino|estacion fin|estación fin)\s*[:\-]\s*([^\n]+)", bloque, re.I) else None

        pk_match = re.search(r"(?:pk|p\.k\.|km)\s*([0-9]+(?:[\.,][0-9]+)?)\s*(?:-|a|hasta)\s*([0-9]+(?:[\.,][0-9]+)?)", bloque, re.I)
        if pk_match:
            row["pk_inicio"] = _parse_float(pk_match.group(1))
            row["pk_fin"] = _parse_float(pk_match.group(2))

        vel_match = re.search(r"(?:velocidad(?:\s+limitada)?|v\.?l\.?)\s*[:\-]?\s*([0-9]+(?:[\.,][0-9]+)?)", bloque, re.I)
        if vel_match:
            row["velocidad_limitada"] = _parse_float(vel_match.group(1))

        # Heurística por fecha/hora en rango.
        fecha_hora = re.search(
            r"(\d{1,2}/\d{1,2}/\d{2,4})\s+(\d{1,2}:\d{2})\s*(?:-|a|hasta)\s*(\d{1,2}/\d{1,2}/\d{2,4})\s+(\d{1,2}:\d{2})",
            bloque,
            re.I,
        )
        if fecha_hora:
            row["fecha_inicio"] = _limpiar_texto_base(fecha_hora.group(1))
            row["hora_inicio"] = _limpiar_texto_base(fecha_hora.group(2))
            row["fecha_fin"] = _limpiar_texto_base(fecha_hora.group(3))
            row["hora_fin"] = _limpiar_texto_base(fecha_hora.group(4))

        if "corte" in low or "limitacion" in low or "limitación" in low:
            tipo_match = re.search(r"(?:tipo|restriccion|restricción)\s*[:\-]\s*([^\n]+)", bloque, re.I)
            row["tipo"] = limpiar_tipo(tipo_match.group(1) if tipo_match else "Corte/Limitación")

        per_match = re.search(r"(?:periodicidad|dias|días|vigencia)\s*[:\-]\s*([^\n]+)", bloque, re.I)
        if per_match:
            row["periodicidad"] = limpiar_periodicidad(per_match.group(1))

        vias_match = re.search(r"(?:vias|vías|via|vía)\s*[:\-]\s*([^\n]+)", bloque, re.I)
        if vias_match:
            row["vias"] = limpiar_vias(vias_match.group(1))

        if any(row.values()):
            resultados.append(row)

    return resultados


def parse_tbp(pdf_path: str) -> list[dict[str, Any]]:
    """Parsea un PDF TBP y retorna una lista de restricciones operativas."""
    resultados: list[dict[str, Any]] = []

    try:
        with pdfplumber.open(pdf_path) as pdf:
            for page_idx, page in enumerate(pdf.pages, start=1):
                try:
                    page_rows = _extract_by_tables(page)
                    if page_rows:
                        resultados.extend(page_rows)
                        continue

                    # Heurístico secundario solo si no se detectaron tablas útiles.
                    texto = page.extract_text() or ""
                    resultados.extend(_extract_by_text(texto))
                except Exception as exc:  # pragma: no cover - tolerancia PDF variable
                    logger.warning("Página TBP omitida page=%s por error: %s", page_idx, exc)
    except Exception as exc:
        logger.exception("Error general parseando PDF TBP '%s': %s", pdf_path, exc)

    return resultados


def procesar_tbp(pdf_path: str, documento_id: int, sqlite_service: Any) -> list[dict[str, Any]]:
    """Parsea TBP e inserta resultados en SQLite (tabla `tbp`)."""
    resultados = parse_tbp(pdf_path)
    nombre_pdf = Path(pdf_path).name

    for idx, restriccion in enumerate(resultados, start=1):
        try:
            if not isinstance(restriccion, Mapping):
                logger.warning("Resultado TBP no válido en índice %s: %r", idx, restriccion)
                continue

            sqlite_service.insertar_tbp(
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
                velocidad_limitada=_parse_float(restriccion.get("velocidad_limitada")),
                archivo=nombre_pdf,
            )
            if isinstance(restriccion, dict):
                restriccion["archivo"] = nombre_pdf
        except Exception as exc:  # pragma: no cover - no romper procesamiento por una fila
            logger.warning(
                "No se pudo insertar restricción TBP idx=%s para documento_id=%s: %s",
                idx,
                documento_id,
                exc,
            )

    return resultados
