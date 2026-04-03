"""Motor de detección de conflictos ferroviarios.

Evalúa conflictos entre:
- mallas (pasos de tren)
- TBA (cortes)
- TBP (limitaciones)
- velocidades

y persiste resultados en la tabla `conflictos` de SQLite.
"""

from __future__ import annotations

import sqlite3
from datetime import datetime
from typing import Any


def safe_float(value: Any) -> float | None:
    """Convierte un valor numérico potencialmente textual a float."""
    try:
        return float(str(value).replace(",", "."))
    except (TypeError, ValueError):
        return None


def _normalizar_hora(hora_raw: Any) -> str | None:
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


def _hora_en_rango(hora: str | None, inicio: str | None, fin: str | None) -> bool:
    """Valida si hora HH:MM está entre inicio y fin (inclusive), con cruce de medianoche."""
    if not hora or not inicio or not fin:
        return False

    if inicio <= fin:
        return inicio <= hora <= fin

    # cruza medianoche
    return hora >= inicio or hora <= fin


def _pk_en_rango(pk: float | None, pk_inicio: float | None, pk_fin: float | None) -> bool:
    if pk is None or pk_inicio is None or pk_fin is None:
        return False

    bajo = min(pk_inicio, pk_fin)
    alto = max(pk_inicio, pk_fin)
    return bajo <= pk <= alto


def _buscar_velocidad_max(paso: dict[str, Any], velocidades: list[dict[str, Any]]) -> dict[str, Any] | None:
    """Encuentra el registro de velocidad aplicable al paso por línea+PK.

    Regla: se toma la primera coincidencia exacta por línea y PK. Si no hay,
    no se marca conflicto de velocidad.
    """
    linea = paso.get("linea")
    pk = safe_float(paso.get("pk"))

    if pk is None:
        return None

    for vel in velocidades:
        if linea and vel.get("linea") != linea:
            continue
        if safe_float(vel.get("pk")) == pk:
            return vel
    return None


def _es_conexion_sqlite(origen: Any) -> bool:
    return isinstance(origen, sqlite3.Connection)


def _obtener_rows(origen: Any, tabla: str, order_by: str, tren: str | None = None) -> list[dict[str, Any]]:
    if _es_conexion_sqlite(origen):
        cur = origen.cursor()
        if tabla == "mallas" and tren:
            cur.execute(f"SELECT * FROM {tabla} WHERE tren = ? ORDER BY {order_by}", (tren,))
        else:
            cur.execute(f"SELECT * FROM {tabla} ORDER BY {order_by}")
        return cur.fetchall()

    if tabla == "mallas":
        return origen.obtener_mallas(tren=tren)
    if tabla == "tba":
        return origen.obtener_tba()
    if tabla == "tbp":
        return origen.obtener_tbp()
    if tabla == "velocidades":
        return origen.obtener_velocidades()
    raise ValueError(f"Tabla no soportada en motor: {tabla}")


def limpiar_conflictos(sqlite_service: Any) -> None:
    """Borra tabla conflictos antes de recalcular."""
    if _es_conexion_sqlite(sqlite_service):
        cur = sqlite_service.cursor()
        cur.execute("DELETE FROM conflictos")
        sqlite_service.commit()
        return

    sqlite_service.limpiar_conflictos()


def insertar_conflictos(sqlite_service: Any, conflictos: list[dict[str, Any]]) -> int:
    """Inserta conflictos detectados en SQLite evitando duplicados."""
    insertados = 0
    dedupe: set[tuple[Any, ...]] = set()

    for conflicto in conflictos:
        llave = (
            conflicto.get("tren"),
            conflicto.get("linea"),
            conflicto.get("pk"),
            conflicto.get("hora"),
            conflicto.get("tipo_conflicto"),
            conflicto.get("documento_origen"),
            conflicto.get("archivo"),
        )
        if llave in dedupe:
            continue
        dedupe.add(llave)

        if _es_conexion_sqlite(sqlite_service):
            cur = sqlite_service.cursor()
            cur.execute(
                """
                INSERT INTO conflictos (
                    tren, linea, pk, hora, tipo_conflicto, descripcion,
                    accion, documento_origen, archivo, fecha_detectado
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                """,
                (
                    conflicto.get("tren"),
                    conflicto.get("linea"),
                    conflicto.get("pk"),
                    conflicto.get("hora"),
                    conflicto.get("tipo_conflicto"),
                    conflicto.get("descripcion"),
                    conflicto.get("accion"),
                    conflicto.get("documento_origen"),
                    conflicto.get("archivo"),
                ),
            )
            sqlite_service.commit()
        else:
            sqlite_service.insertar_conflicto(
                tren=conflicto.get("tren"),
                linea=conflicto.get("linea"),
                pk=conflicto.get("pk"),
                hora=conflicto.get("hora"),
                tipo_conflicto=conflicto.get("tipo_conflicto"),
                descripcion=conflicto.get("descripcion"),
                accion=conflicto.get("accion"),
                documento_origen=conflicto.get("documento_origen"),
                archivo=conflicto.get("archivo"),
            )
        insertados += 1

    return insertados


def detectar_conflictos(sqlite_service: Any, tren: str | None = None) -> list[dict[str, Any]]:
    """Detecta conflictos operativos y los persiste en tabla conflictos.

    Reglas implementadas:
    1) TBA: CORTE TOTAL
    2) TBP: LIMITACION TEMPORAL
    3) Velocidad: EXCESO VELOCIDAD cuando velocidad_max < velocidad_teorica.
    """
    mallas = _obtener_rows(sqlite_service, "mallas", "tren, orden, hora", tren=tren)
    tba_rows = _obtener_rows(sqlite_service, "tba", "linea, pk_inicio, hora_inicio")
    tbp_rows = _obtener_rows(sqlite_service, "tbp", "linea, pk_inicio, hora_inicio")
    velocidades = _obtener_rows(sqlite_service, "velocidades", "linea, pk")

    limpiar_conflictos(sqlite_service)

    conflictos: list[dict[str, Any]] = []

    for paso in mallas:
        pk = safe_float(paso.get("pk"))
        hora = _normalizar_hora(paso.get("hora"))
        if pk is None or hora is None:
            continue

        linea = paso.get("linea")

        # 1) CONFLICTO TBA (CRÍTICO)
        for tba in tba_rows:
            pk_inicio = safe_float(tba.get("pk_inicio"))
            pk_fin = safe_float(tba.get("pk_fin"))
            if pk_inicio is None or pk_fin is None:
                continue
            print(f"Comparando PK {pk} con rango {pk_inicio}-{pk_fin}")
            if not _pk_en_rango(pk, pk_inicio, pk_fin):
                continue

            hora_inicio = _normalizar_hora(tba.get("hora_inicio"))
            hora_fin = _normalizar_hora(tba.get("hora_fin"))
            print(f"Comparando hora {hora} con {hora_inicio}-{hora_fin}")
            if not _hora_en_rango(hora, hora_inicio, hora_fin):
                continue

            conflictos.append(
                {
                    "tren": paso.get("tren"),
                    "linea": linea,
                    "pk": pk,
                    "hora": hora,
                    "tipo_conflicto": "CORTE TOTAL",
                    "descripcion": (
                        f"Paso en PK {pk} a las {hora} dentro de corte TBA "
                        f"[{tba.get('pk_inicio')}, {tba.get('pk_fin')}]"
                    ),
                    "accion": "Detener tren antes de estación de entrada",
                    "documento_origen": str(tba.get("documento_id") or ""),
                    "archivo": tba.get("archivo"),
                }
            )

        # 2) CONFLICTO TBP (LIMITACIÓN)
        for tbp in tbp_rows:
            pk_inicio = safe_float(tbp.get("pk_inicio"))
            pk_fin = safe_float(tbp.get("pk_fin"))
            if pk_inicio is None or pk_fin is None:
                continue
            print(f"Comparando PK {pk} con rango {pk_inicio}-{pk_fin}")
            if not _pk_en_rango(pk, pk_inicio, pk_fin):
                continue

            hora_inicio = _normalizar_hora(tbp.get("hora_inicio"))
            hora_fin = _normalizar_hora(tbp.get("hora_fin"))
            print(f"Comparando hora {hora} con {hora_inicio}-{hora_fin}")
            if not _hora_en_rango(hora, hora_inicio, hora_fin):
                continue

            conflictos.append(
                {
                    "tren": paso.get("tren"),
                    "linea": linea,
                    "pk": pk,
                    "hora": hora,
                    "tipo_conflicto": "LIMITACION TEMPORAL",
                    "descripcion": (
                        f"Paso en PK {pk} a las {hora} dentro de limitación TBP "
                        f"[{tbp.get('pk_inicio')}, {tbp.get('pk_fin')}]"
                    ),
                    "accion": "Reducir velocidad / ajustar horario",
                    "documento_origen": str(tbp.get("documento_id") or ""),
                    "archivo": tbp.get("archivo"),
                }
            )

        # 3) CONFLICTO VELOCIDAD
        vel_row = _buscar_velocidad_max(paso, velocidades)
        if vel_row:
            velocidad_max = vel_row.get("velocidad_max")
            velocidad_teorica = paso.get("velocidad_teorica")

            if velocidad_teorica is None:
                conflictos.append(
                    {
                        "tren": paso.get("tren"),
                        "linea": linea,
                        "pk": pk,
                        "hora": hora,
                        "tipo_conflicto": "EXCESO VELOCIDAD",
                        "descripcion": (
                            f"Existe velocidad máxima {velocidad_max} en PK {pk}, "
                            "sin velocidad teórica informada por el tren"
                        ),
                        "accion": "Reducir velocidad",
                        "documento_origen": str(vel_row.get("documento_id") or ""),
                        "archivo": vel_row.get("archivo"),
                    }
                )
            elif velocidad_max is not None and float(velocidad_max) < float(velocidad_teorica):
                conflictos.append(
                    {
                        "tren": paso.get("tren"),
                        "linea": linea,
                        "pk": pk,
                        "hora": hora,
                        "tipo_conflicto": "EXCESO VELOCIDAD",
                        "descripcion": (
                            f"Velocidad teórica {velocidad_teorica} supera máxima "
                            f"permitida {velocidad_max} en PK {pk}"
                        ),
                        "accion": "Reducir velocidad",
                        "documento_origen": str(vel_row.get("documento_id") or ""),
                        "archivo": vel_row.get("archivo"),
                    }
                )

    insertar_conflictos(sqlite_service, conflictos)
    return conflictos
