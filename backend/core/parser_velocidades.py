"""Parser de Cuadros de Velocidades Máximas (CVM).

Objetivo: extraer límites de velocidad por PK y línea para su comparación
con mallas y restricciones operativas.

Limitaciones conocidas del parseo CVM:
- Muchos CVM escaneados no conservan estructura tabular; en esos casos la
  extracción depende de OCR/calidad del texto y puede perder filas.
- El parser evita inferencias agresivas: si PK o velocidad no aparecen de
  forma explícita, la fila se descarta (no se inventan datos).
- La detección de columnas N/A/B es heurística y depende de cabeceras
  reconocibles en el PDF (p.ej. "N", "A", "B", "Tipo N", etc.).
"""

from __future__ import annotations

import logging
import re
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import pdfplumber

logger = logging.getLogger(__name__)

_HEADER_ALIASES: dict[str, tuple[str, ...]] = {
    "linea": ("linea", "línea", "line", "trayecto"),
    "pk": ("pk", "p.k", "p.k.", "km", "kilometro", "kilómetro"),
    "velocidad_max": (
        "velocidad",
        "velocidad max",
        "velocidad máxima",
        "vmax",
        "v. max",
    ),
    "velocidad_n": (" tipo n", " tren n", " n", "vn", "v n"),
    "velocidad_a": (" tipo a", " tren a", " a", "va", "v a"),
    "velocidad_b": (" tipo b", " tren b", " b", "vb", "v b"),
}


def _normalizar_texto(texto: Any) -> str:
    if texto is None:
        return ""

    limpio = str(texto).replace("\n", " ").strip().lower()
    limpio = (
        limpio.replace("á", "a")
        .replace("é", "e")
        .replace("í", "i")
        .replace("ó", "o")
        .replace("ú", "u")
    )
    return re.sub(r"\s+", " ", limpio)


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


def _detectar_linea(texto: str) -> str | None:
    m = re.search(r"(?:linea|línea)\s*[:\-]?\s*([^\n|;]{2,})", texto or "", re.I)
    if not m:
        return None

    linea = re.sub(r"\s+", " ", m.group(1)).strip(" .:-")
    return linea or None


def _mapear_headers(header_row: list[Any]) -> dict[str, int]:
    mapping: dict[str, int] = {}

    for idx, celda in enumerate(header_row):
        celda_norm = f" {_normalizar_texto(celda)} "
        if not celda_norm.strip():
            continue

        for field, aliases in _HEADER_ALIASES.items():
            if field in mapping:
                continue
            if any(alias in celda_norm for alias in aliases):
                mapping[field] = idx
                break

    return mapping


def _crear_registro(linea: str | None, pk: float, velocidad: float, tipo_tren: str | None) -> dict[str, Any]:
    return {
        "linea": linea,
        "pk": pk,
        "velocidad_max": velocidad,
        "tipo_tren": tipo_tren,
        "documento_id": None,
    }


def _extraer_velocidades_por_tipos(fila: list[Any], mapping: dict[str, int]) -> list[tuple[float, str | None]]:
    pares: list[tuple[float, str | None]] = []

    idx_vel_general = mapping.get("velocidad_max")
    if idx_vel_general is not None and idx_vel_general < len(fila):
        vel = _parse_float(fila[idx_vel_general])
        if vel is not None:
            pares.append((vel, None))

    for field, tipo in (("velocidad_n", "N"), ("velocidad_a", "A"), ("velocidad_b", "B")):
        col = mapping.get(field)
        if col is None or col >= len(fila):
            continue
        vel = _parse_float(fila[col])
        if vel is not None:
            pares.append((vel, tipo))

    return pares


def _extract_by_tables(page: pdfplumber.page.Page, linea_contexto: str | None) -> list[dict[str, Any]]:
    """Prioriza extracción tabular al parsear CVM."""
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
            tiene_pk = "pk" in mapping
            tiene_vel = any(k in mapping for k in ("velocidad_max", "velocidad_n", "velocidad_a", "velocidad_b"))
            if tiene_pk and tiene_vel:
                header_idx = idx
                header_map = mapping
                break

        if header_idx is None:
            continue

        for fila in tabla[header_idx + 1 :]:
            if not fila:
                continue

            idx_pk = header_map.get("pk")
            if idx_pk is None or idx_pk >= len(fila):
                continue

            pk = _parse_float(fila[idx_pk])
            if pk is None:
                continue

            linea = linea_contexto
            idx_linea = header_map.get("linea")
            if idx_linea is not None and idx_linea < len(fila):
                linea_fila = str(fila[idx_linea]).strip() if fila[idx_linea] is not None else ""
                if linea_fila:
                    linea = linea_fila

            for velocidad, tipo in _extraer_velocidades_por_tipos(fila, header_map):
                resultados.append(_crear_registro(linea, pk, velocidad, tipo))

    return resultados


def _extract_by_text(texto: str, linea_contexto: str | None) -> list[dict[str, Any]]:
    """Fallback heurístico por texto libre si no hay tabla utilizable."""
    resultados: list[dict[str, Any]] = []
    if not texto:
        return resultados

    linea = _detectar_linea(texto) or linea_contexto

    for raw in texto.splitlines():
        fila = raw.strip()
        if not fila:
            continue

        pk_match = re.search(r"(?:\bpk\b|\bp\.k\.\b|\bkm\b)\s*[:\-]?\s*(\d+(?:[\.,]\d+)?)", fila, re.I)
        if not pk_match:
            continue

        pk = _parse_float(pk_match.group(1))
        if pk is None:
            continue

        n_match = re.search(r"\bN\b\s*[:=\-]?\s*(\d+(?:[\.,]\d+)?)", fila, re.I)
        a_match = re.search(r"\bA\b\s*[:=\-]?\s*(\d+(?:[\.,]\d+)?)", fila, re.I)
        b_match = re.search(r"\bB\b\s*[:=\-]?\s*(\d+(?:[\.,]\d+)?)", fila, re.I)

        tipos_detectados = False
        for match, tipo in ((n_match, "N"), (a_match, "A"), (b_match, "B")):
            if not match:
                continue
            vel = _parse_float(match.group(1))
            if vel is None:
                continue
            resultados.append(_crear_registro(linea, pk, vel, tipo))
            tipos_detectados = True

        if tipos_detectados:
            continue

        vel_general = re.search(
            r"(?:velocidad(?:\s+max(?:ima)?)?|vmax|v\.\s*max)\s*[:=\-]?\s*(\d+(?:[\.,]\d+)?)",
            fila,
            re.I,
        )
        if vel_general:
            vel = _parse_float(vel_general.group(1))
            if vel is not None:
                resultados.append(_crear_registro(linea, pk, vel, None))

    return resultados


def _deduplicar_registros(registros: list[dict[str, Any]]) -> list[dict[str, Any]]:
    unicos: list[dict[str, Any]] = []
    vistos: set[tuple[str | None, float, float, str | None]] = set()

    for reg in registros:
        if not isinstance(reg, Mapping):
            continue

        linea = reg.get("linea")
        pk = _parse_float(reg.get("pk"))
        vel = _parse_float(reg.get("velocidad_max"))
        tipo = reg.get("tipo_tren")

        if pk is None or vel is None:
            continue

        key = (linea, pk, vel, tipo)
        if key in vistos:
            continue

        vistos.add(key)
        unicos.append(
            {
                "linea": linea,
                "pk": pk,
                "velocidad_max": vel,
                "tipo_tren": tipo,
                "documento_id": reg.get("documento_id"),
            }
        )

    return unicos


def parse_velocidades(pdf_path: str) -> list[dict[str, Any]]:
    """Parsea un PDF CVM y retorna lista de dicts: linea, pk, velocidad_max, tipo_tren, documento_id."""
    resultados: list[dict[str, Any]] = []

    try:
        with pdfplumber.open(pdf_path) as pdf:
            for page_idx, page in enumerate(pdf.pages, start=1):
                try:
                    texto = page.extract_text() or ""
                    linea_contexto = _detectar_linea(texto)

                    page_rows = _extract_by_tables(page, linea_contexto)
                    if page_rows:
                        resultados.extend(page_rows)
                        continue

                    resultados.extend(_extract_by_text(texto, linea_contexto))
                except Exception as exc:  # pragma: no cover - tolerancia a PDFs heterogéneos
                    logger.warning("Página CVM omitida page=%s por error: %s", page_idx, exc)
    except Exception as exc:
        logger.exception("Error general parseando PDF CVM '%s': %s", pdf_path, exc)

    return _deduplicar_registros(resultados)


def procesar_velocidades(pdf_path: str, documento_id: int, sqlite_service: Any) -> list[dict[str, Any]]:
    """Parsea CVM e inserta resultados en SQLite (tabla `velocidades`)."""
    resultados = parse_velocidades(pdf_path)
    nombre_pdf = Path(pdf_path).name

    for idx, velocidad in enumerate(resultados, start=1):
        try:
            if not isinstance(velocidad, Mapping):
                logger.warning("Resultado CVM no válido en índice %s: %r", idx, velocidad)
                continue

            pk = _parse_float(velocidad.get("pk"))
            vel_max = _parse_float(velocidad.get("velocidad_max"))
            if pk is None or vel_max is None:
                logger.warning(
                    "Fila CVM descartada idx=%s por falta de PK/velocidad explícita: %r",
                    idx,
                    velocidad,
                )
                continue

            velocidad_con_doc = dict(velocidad)
            velocidad_con_doc["documento_id"] = documento_id

            sqlite_service.insertar_velocidad(
                documento_id=documento_id,
                linea=velocidad_con_doc.get("linea") or "DESCONOCIDA",
                pk=pk,
                velocidad_max=vel_max,
                tipo_tren=velocidad_con_doc.get("tipo_tren"),
                archivo=nombre_pdf,
            )

            velocidad_con_doc["archivo"] = nombre_pdf
            resultados[idx - 1] = velocidad_con_doc
        except Exception as exc:  # pragma: no cover - no interrumpir por fila defectuosa
            logger.warning(
                "No se pudo insertar velocidad CVM idx=%s para documento_id=%s: %s",
                idx,
                documento_id,
                exc,
            )

    return resultados
