"""Servicio de detección de conflictos ferroviarios.

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

from backend.core.utils_normalizacion import (
    normalizar_fecha_hora,
    normalizar_hora,
    normalizar_linea,
    normalizar_pk,
)


def safe_float(value: Any) -> float | None:
    """Compatibilidad retroactiva: alias de normalización de PK/numérico."""
    return normalizar_pk(value)


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


def _fecha_hora_en_intervalo(
    fecha_hora: datetime | None,
    fecha_inicio: Any,
    hora_inicio: Any,
    fecha_fin: Any,
    hora_fin: Any,
) -> bool:
    """Valida si fecha_hora cae dentro de [fecha_inicio+hora_inicio, fecha_fin+hora_fin]."""
    if fecha_hora is None:
        return False

    inicio = normalizar_fecha_hora(fecha_inicio, hora_inicio)
    fin = normalizar_fecha_hora(fecha_fin, hora_fin)
    if inicio is None or fin is None:
        return False

    if inicio <= fin:
        return inicio <= fecha_hora <= fin

    # defensivo por datos invertidos
    return fin <= fecha_hora <= inicio


def _buscar_velocidad_max(paso: dict[str, Any], velocidades: list[dict[str, Any]]) -> dict[str, Any] | None:
    """Encuentra el registro de velocidad aplicable al paso por línea+PK."""
    linea = normalizar_linea(paso.get("linea"))
    pk = normalizar_pk(paso.get("pk"))

    if pk is None:
        return None

    for vel in velocidades:
        linea_vel = normalizar_linea(vel.get("linea"))
        if linea is not None and linea_vel != linea:
            continue
        if normalizar_pk(vel.get("pk")) == pk:
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


def detectar_conflictos_tba(
    paso: dict[str, Any],
    tba_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Detecta conflictos MALLAS vs TBA para un paso de malla."""
    conflictos: list[dict[str, Any]] = []
    pk = normalizar_pk(paso.get("pk"))
    hora = normalizar_hora(paso.get("hora"))
    linea = normalizar_linea(paso.get("linea"))
    fecha_hora_paso = normalizar_fecha_hora(paso.get("fecha"), paso.get("hora"))

    print(f"[DEBUG][TBA] linea={linea} pk={pk} hora={hora}")

    if pk is None or hora is None or linea is None:
        return conflictos

    for tba in tba_rows:
        linea_tba = normalizar_linea(tba.get("linea"))
        if linea_tba != linea:
            continue

        pk_inicio = normalizar_pk(tba.get("pk_inicio"))
        pk_fin = normalizar_pk(tba.get("pk_fin"))
        if not _pk_en_rango(pk, pk_inicio, pk_fin):
            continue

        if fecha_hora_paso is not None:
            if not _fecha_hora_en_intervalo(
                fecha_hora_paso,
                tba.get("fecha_inicio"),
                tba.get("hora_inicio"),
                tba.get("fecha_fin"),
                tba.get("hora_fin"),
            ):
                continue
        else:
            hora_inicio = normalizar_hora(tba.get("hora_inicio"))
            hora_fin = normalizar_hora(tba.get("hora_fin"))
            if not _hora_en_rango(hora, hora_inicio, hora_fin):
                continue

        tipo_tba = str(tba.get("tipo") or "").upper()
        tipo_conflicto = "CORTE TENSIÓN" if "TENSION" in tipo_tba or "TENSIÓN" in tipo_tba else "CORTE TOTAL"

        conflictos.append(
            {
                "tren": paso.get("tren"),
                "linea": linea,
                "pk": pk,
                "hora": hora,
                "tipo_conflicto": tipo_conflicto,
                "descripcion": "Tren entra en tramo con restricción TBA",
                "accion": "Reprogramar / detener circulación",
                "documento_origen": str(tba.get("documento_id") or ""),
                "archivo": tba.get("archivo"),
            }
        )

    return conflictos


def detectar_conflictos_tbp(
    paso: dict[str, Any],
    tbp_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Detecta conflictos MALLAS vs TBP para un paso de malla."""
    conflictos: list[dict[str, Any]] = []
    pk = normalizar_pk(paso.get("pk"))
    hora = normalizar_hora(paso.get("hora"))
    linea = normalizar_linea(paso.get("linea"))
    fecha_hora_paso = normalizar_fecha_hora(paso.get("fecha"), paso.get("hora"))

    print(f"[DEBUG][TBP] linea={linea} pk={pk} hora={hora}")

    if pk is None or hora is None or linea is None:
        return conflictos

    for tbp in tbp_rows:
        linea_tbp = normalizar_linea(tbp.get("linea"))
        if linea_tbp != linea:
            continue

        pk_inicio = normalizar_pk(tbp.get("pk_inicio"))
        pk_fin = normalizar_pk(tbp.get("pk_fin"))
        if not _pk_en_rango(pk, pk_inicio, pk_fin):
            continue

        if fecha_hora_paso is not None:
            if not _fecha_hora_en_intervalo(
                fecha_hora_paso,
                tbp.get("fecha_inicio"),
                tbp.get("hora_inicio"),
                tbp.get("fecha_fin"),
                tbp.get("hora_fin"),
            ):
                continue
        else:
            hora_inicio = normalizar_hora(tbp.get("hora_inicio"))
            hora_fin = normalizar_hora(tbp.get("hora_fin"))
            if not _hora_en_rango(hora, hora_inicio, hora_fin):
                continue

        conflictos.append(
            {
                "tren": paso.get("tren"),
                "linea": linea,
                "pk": pk,
                "hora": hora,
                "tipo_conflicto": "LIMITACIÓN",
                "descripcion": "Tren circula en tramo con limitación TBP",
                "accion": "Reducir velocidad",
                "documento_origen": str(tbp.get("documento_id") or ""),
                "archivo": tbp.get("archivo"),
            }
        )

    return conflictos


def detectar_conflictos_velocidad(
    paso: dict[str, Any],
    velocidades: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Detecta conflictos de velocidad para un paso de malla."""
    conflictos: list[dict[str, Any]] = []

    pk = normalizar_pk(paso.get("pk"))
    hora = normalizar_hora(paso.get("hora"))
    linea = normalizar_linea(paso.get("linea"))

    print(f"[DEBUG][VEL] linea={linea} pk={pk} hora={hora}")

    if pk is None or hora is None:
        return conflictos

    vel_row = _buscar_velocidad_max(paso, velocidades)
    if not vel_row:
        return conflictos

    velocidad_max = normalizar_pk(vel_row.get("velocidad_max"))
    velocidad_teorica = normalizar_pk(paso.get("velocidad_teorica"))

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
    elif velocidad_max is not None and velocidad_max < velocidad_teorica:
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

    return conflictos


def calcular_conflictos(sqlite_service: Any, tren: str | None = None) -> list[dict[str, Any]]:
    """Detecta conflictos operativos y los persiste en tabla conflictos."""
    mallas = _obtener_rows(sqlite_service, "mallas", "tren, orden, hora", tren=tren)
    tba_rows = _obtener_rows(sqlite_service, "tba", "linea, pk_inicio, hora_inicio")
    tbp_rows = _obtener_rows(sqlite_service, "tbp", "linea, pk_inicio, hora_inicio")
    velocidades = _obtener_rows(sqlite_service, "velocidades", "linea, pk")

    limpiar_conflictos(sqlite_service)

    conflictos: list[dict[str, Any]] = []

    for paso in mallas:
        conflictos.extend(detectar_conflictos_tba(paso, tba_rows))
        conflictos.extend(detectar_conflictos_tbp(paso, tbp_rows))
        conflictos.extend(detectar_conflictos_velocidad(paso, velocidades))

    insertar_conflictos(sqlite_service, conflictos)
    return conflictos


def detectar_conflictos(sqlite_service: Any, tren: str | None = None) -> list[dict[str, Any]]:
    """Alias de compatibilidad con nombre previo."""
    return calcular_conflictos(sqlite_service, tren=tren)
