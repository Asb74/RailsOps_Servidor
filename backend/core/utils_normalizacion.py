"""Utilidades de normalización para el motor de conflictos."""

from __future__ import annotations

import re
from datetime import datetime
from typing import Any

_PATRON_NUMERO = re.compile(r"-?\d+(?:[\.,]\d+)?")


def normalizar_linea(linea_raw: Any) -> int | str | None:
    """Normaliza identificador de línea.

    Reglas:
    - Extrae número de línea si existe ("430 De: ..." -> 430)
    - "DESCONOCIDA" -> None
    - Si no hay número, devuelve texto limpio en mayúsculas
    """
    if linea_raw is None:
        return None

    valor = str(linea_raw).strip()
    if not valor:
        return None

    if valor.upper() == "DESCONOCIDA":
        return None

    match = re.search(r"\d+", valor)
    if match:
        return int(match.group())

    return valor.upper()


def normalizar_pk(pk_raw: Any) -> float | None:
    """Extrae PK como float desde formatos sucios.

    Ejemplos:
    - "19.2 BIF..." -> 19.2
    - "19,2" -> 19.2
    - 19 -> 19.0
    """
    if pk_raw is None:
        return None

    if isinstance(pk_raw, (int, float)):
        return float(pk_raw)

    valor = str(pk_raw).strip()
    if not valor:
        return None

    match = _PATRON_NUMERO.search(valor)
    if not match:
        return None

    numero = match.group().replace(",", ".")
    try:
        return float(numero)
    except ValueError:
        return None


def normalizar_hora(hora_raw: Any) -> str | None:
    """Convierte una hora a formato HH:MM para comparaciones."""
    if hora_raw is None:
        return None

    valor = str(hora_raw).strip()
    if not valor:
        return None

    for fmt in ("%H:%M", "%H:%M:%S"):
        try:
            return datetime.strptime(valor, fmt).strftime("%H:%M")
        except ValueError:
            continue
    return None


def normalizar_fecha(fecha_raw: Any) -> str | None:
    """Convierte una fecha textual a formato ISO YYYY-MM-DD."""
    if fecha_raw is None:
        return None

    valor = str(fecha_raw).strip()
    if not valor:
        return None

    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d/%m/%y", "%d-%m-%Y", "%d-%m-%y"):
        try:
            return datetime.strptime(valor, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def normalizar_fecha_hora(fecha_raw: Any, hora_raw: Any) -> datetime | None:
    """Combina fecha + hora y retorna datetime normalizado si es válido."""
    fecha = normalizar_fecha(fecha_raw)
    hora = normalizar_hora(hora_raw)
    if not fecha or not hora:
        return None

    try:
        return datetime.strptime(f"{fecha} {hora}", "%Y-%m-%d %H:%M")
    except ValueError:
        return None
