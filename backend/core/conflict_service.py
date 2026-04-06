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
from collections import defaultdict
from datetime import date, datetime, time, timedelta
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


def hay_solape_pk(
    pk1_ini: float | None,
    pk1_fin: float | None,
    pk2_ini: float | None,
    pk2_fin: float | None,
) -> bool:
    """Retorna True si los rangos PK [pk1_ini, pk1_fin] y [pk2_ini, pk2_fin] se solapan."""
    if pk1_ini is None or pk1_fin is None or pk2_ini is None or pk2_fin is None:
        return False

    a_ini, a_fin = sorted((pk1_ini, pk1_fin))
    b_ini, b_fin = sorted((pk2_ini, pk2_fin))
    return max(a_ini, b_ini) <= min(a_fin, b_fin)


def normalizar_intervalo_datetime(inicio: datetime, fin: datetime) -> tuple[datetime, datetime]:
    """Normaliza intervalo datetime para contemplar cruces de medianoche."""
    if fin < inicio:
        fin = fin + timedelta(days=1)
    return inicio, fin


def hay_solape_temporal(
    inicio1: datetime | None,
    fin1: datetime | None,
    inicio2: datetime | None,
    fin2: datetime | None,
) -> bool:
    """Retorna True si dos intervalos temporales se solapan, incluyendo medianoche.

    Se prueban desplazamientos de ±1 día para soportar intervalos anclados sin fecha real.
    """
    if inicio1 is None or fin1 is None or inicio2 is None or fin2 is None:
        return False

    a_ini, a_fin = normalizar_intervalo_datetime(inicio1, fin1)
    b_ini, b_fin = normalizar_intervalo_datetime(inicio2, fin2)

    for day_shift in (-1, 0, 1):
        delta = timedelta(days=day_shift)
        b_ini_shift = b_ini + delta
        b_fin_shift = b_fin + delta
        if max(a_ini, b_ini_shift) <= min(a_fin, b_fin_shift):
            return True

    return False


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


def _hora_a_time(hora_raw: Any) -> time | None:
    hora = normalizar_hora(hora_raw)
    if not hora:
        return None
    try:
        return datetime.strptime(hora, "%H:%M").time()
    except ValueError:
        return None


def _validar_modo(modo: str | None) -> str:
    modo_norm = str(modo or "real").strip().lower()
    if modo_norm not in ("real", "simulacion"):
        raise ValueError("modo debe ser 'real' o 'simulacion'")
    return modo_norm


def resolver_intervalo_restriccion(restr: dict[str, Any], modo: str = "real") -> tuple[datetime, datetime] | None:
    """Resuelve intervalo de restricción según modo de análisis."""
    modo_norm = _validar_modo(modo)

    if modo_norm == "real":
        dt_ini = normalizar_fecha_hora(restr.get("fecha_inicio"), restr.get("hora_inicio"))
        dt_fin = normalizar_fecha_hora(restr.get("fecha_fin"), restr.get("hora_fin"))
        if dt_ini is not None and dt_fin is not None:
            return normalizar_intervalo_datetime(dt_ini, dt_fin)

    hora_ini = _hora_a_time(restr.get("hora_inicio"))
    hora_fin = _hora_a_time(restr.get("hora_fin"))
    if not hora_ini or not hora_fin:
        return None

    base = date(2000, 1, 1)
    ini = datetime.combine(base, hora_ini)
    fin = datetime.combine(base, hora_fin)
    return normalizar_intervalo_datetime(ini, fin)


def _resolver_datetime_paso_real(paso: dict[str, Any]) -> datetime | None:
    for clave_fecha in ("fecha", "fecha_real", "fecha_malla", "dia"):
        dt = normalizar_fecha_hora(paso.get(clave_fecha), paso.get("hora"))
        if dt is not None:
            return dt
    return None


def resolver_intervalo_tramo(tramo: dict[str, Any], modo: str = "real") -> tuple[datetime, datetime] | None:
    """Resuelve intervalo temporal del tramo según modo."""
    modo_norm = _validar_modo(modo)

    if modo_norm == "real":
        paso_ini = tramo.get("paso_inicio") or {}
        paso_fin = tramo.get("paso_fin") or {}
        dt_ini_real = _resolver_datetime_paso_real(paso_ini)
        dt_fin_real = _resolver_datetime_paso_real(paso_fin)
        if dt_ini_real is not None and dt_fin_real is not None:
            return normalizar_intervalo_datetime(dt_ini_real, dt_fin_real)

    dt_ini = tramo.get("dt_inicio")
    dt_fin = tramo.get("dt_fin")
    if dt_ini is None or dt_fin is None:
        return None
    return normalizar_intervalo_datetime(dt_ini, dt_fin)


def _orden_valor(paso: dict[str, Any]) -> tuple[int, int]:
    orden_raw = paso.get("orden")
    try:
        return 0, int(orden_raw)
    except (TypeError, ValueError):
        return 1, 0


def _time_sort_key(paso: dict[str, Any]) -> tuple[int, int, int]:
    t = _hora_a_time(paso.get("hora"))
    if t is None:
        return 1, 99, 99
    return 0, t.hour, t.minute


def _paso_datetime_progresivo(pasos: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Construye datetimes progresivos para pasos de malla con cruce de medianoche."""
    base_day = date(2000, 1, 1)
    salida: list[dict[str, Any]] = []

    ultimo_dt: datetime | None = None
    offset_dias = 0

    for paso in pasos:
        paso_norm = dict(paso)
        hora_t = _hora_a_time(paso.get("hora"))
        if hora_t is None:
            paso_norm["_dt_prog"] = None
            salida.append(paso_norm)
            continue

        candidato = datetime.combine(base_day + timedelta(days=offset_dias), hora_t)
        if ultimo_dt is not None and candidato < ultimo_dt:
            offset_dias += 1
            candidato = datetime.combine(base_day + timedelta(days=offset_dias), hora_t)

        paso_norm["_dt_prog"] = candidato
        ultimo_dt = candidato
        salida.append(paso_norm)

    return salida


def construir_tramos_malla(mallas: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Construye tramos consecutivos por tren a partir de pasos de malla."""
    mallas_por_tren: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for paso in mallas:
        tren_id = str(paso.get("tren") or "")
        mallas_por_tren[tren_id].append(paso)

    tramos: list[dict[str, Any]] = []

    for tren_id, pasos in mallas_por_tren.items():
        pasos_ordenados = sorted(pasos, key=lambda p: (_orden_valor(p), _time_sort_key(p), normalizar_pk(p.get("pk")) or -1.0))
        pasos_con_dt = _paso_datetime_progresivo(pasos_ordenados)

        for idx in range(len(pasos_con_dt) - 1):
            paso_ini = pasos_con_dt[idx]
            paso_fin = pasos_con_dt[idx + 1]

            pk_ini = normalizar_pk(paso_ini.get("pk"))
            pk_fin = normalizar_pk(paso_fin.get("pk"))
            dt_ini = paso_ini.get("_dt_prog")
            dt_fin = paso_fin.get("_dt_prog")

            if pk_ini is None or pk_fin is None or dt_ini is None or dt_fin is None:
                continue

            dt_ini, dt_fin = normalizar_intervalo_datetime(dt_ini, dt_fin)
            linea = normalizar_linea(paso_ini.get("linea"))
            hora_ref = normalizar_hora(paso_ini.get("hora"))

            tramos.append(
                {
                    "tren": tren_id,
                    "linea": linea,
                    "pk_inicio": pk_ini,
                    "pk_fin": pk_fin,
                    "dt_inicio": dt_ini,
                    "dt_fin": dt_fin,
                    "hora_ref": hora_ref,
                    "pk_ref": pk_ini,
                    "archivo": paso_ini.get("archivo") or paso_fin.get("archivo"),
                    "paso_inicio": paso_ini,
                    "paso_fin": paso_fin,
                }
            )

    return tramos


def limpiar_conflictos(sqlite_service: Any) -> None:
    """Borra tabla conflictos antes de recalcular."""
    if _es_conexion_sqlite(sqlite_service):
        cur = sqlite_service.cursor()
        cur.execute("DELETE FROM conflictos")
        sqlite_service.commit()
        return

    sqlite_service.limpiar_conflictos()


def _llave_dedupe_conflicto(conflicto: dict[str, Any]) -> tuple[Any, ...]:
    pk = normalizar_pk(conflicto.get("pk"))
    pk_round = None if pk is None else round(pk, 3)
    hora = normalizar_hora(conflicto.get("hora"))

    return (
        conflicto.get("tren"),
        conflicto.get("linea"),
        conflicto.get("tipo_conflicto"),
        str(conflicto.get("documento_origen") or ""),
        conflicto.get("archivo"),
        hora,
        pk_round,
    )


def insertar_conflictos(sqlite_service: Any, conflictos: list[dict[str, Any]]) -> int:
    """Inserta conflictos detectados en SQLite evitando duplicados."""
    insertados = 0
    dedupe: set[tuple[Any, ...]] = set()

    for conflicto in conflictos:
        llave = _llave_dedupe_conflicto(conflicto)
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


def _detectar_conflictos_restriccion_por_tramo(
    tramo: dict[str, Any],
    restricciones: list[dict[str, Any]],
    *,
    es_tba: bool,
    modo: str = "real",
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    """Detecta conflictos TBA/TBP para un tramo de tren."""
    conflictos: list[dict[str, Any]] = []
    modo_norm = _validar_modo(modo)
    stats = {"comparadas": 0, "desc_linea": 0, "desc_pk": 0, "desc_tiempo": 0, "desc_vigencia": 0, "ok": 0}

    linea_tramo = normalizar_linea(tramo.get("linea"))
    intervalo_tramo = resolver_intervalo_tramo(tramo, modo=modo_norm)
    if not intervalo_tramo:
        stats["desc_tiempo"] += len(restricciones)
        return conflictos, stats

    dt_ini_tramo, dt_fin_tramo = intervalo_tramo

    for restr in restricciones:
        stats["comparadas"] += 1

        linea_restr = normalizar_linea(restr.get("linea"))
        if linea_tramo is not None and linea_restr is not None:
            if linea_tramo != linea_restr:
                stats["desc_linea"] += 1
                continue

        pk_ini_restr = normalizar_pk(restr.get("pk_inicio"))
        pk_fin_restr = normalizar_pk(restr.get("pk_fin"))
        if not hay_solape_pk(tramo.get("pk_inicio"), tramo.get("pk_fin"), pk_ini_restr, pk_fin_restr):
            stats["desc_pk"] += 1
            continue

        intervalo_restr = resolver_intervalo_restriccion(restr, modo=modo_norm)
        if not intervalo_restr:
            stats["desc_tiempo"] += 1
            continue

        dt_ini_restr, dt_fin_restr = intervalo_restr
        if not hay_solape_temporal(dt_ini_tramo, dt_fin_tramo, dt_ini_restr, dt_fin_restr):
            stats["desc_tiempo"] += 1
            continue
        if modo_norm == "real":
            tiene_vigencia_real = (
                normalizar_fecha_hora(restr.get("fecha_inicio"), restr.get("hora_inicio")) is not None
                and normalizar_fecha_hora(restr.get("fecha_fin"), restr.get("hora_fin")) is not None
            )
            if not tiene_vigencia_real:
                stats["desc_vigencia"] += 1
                continue

        if es_tba:
            tipo_txt = str(restr.get("tipo") or "").upper()
            tipo_conflicto = "CORTE TENSIÓN" if "TENSION" in tipo_txt or "TENSIÓN" in tipo_txt else "CORTE TOTAL"
            descripcion = (
                "Conflicto real: tren atraviesa tramo afectado durante la vigencia del expediente"
                if modo_norm == "real"
                else "Conflicto potencial: el tren solapa con la restricción por PK y horario"
            )
            accion = "Reprogramar / detener circulación"
        else:
            tipo_conflicto = "LIMITACIÓN"
            descripcion = (
                "Conflicto real: tren atraviesa tramo afectado durante la vigencia del expediente"
                if modo_norm == "real"
                else "Conflicto potencial: el tren solapa con la restricción por PK y horario"
            )
            accion = "Reducir velocidad / ajustar circulación"

        conflictos.append(
            {
                "tren": tramo.get("tren"),
                "linea": linea_tramo,
                "pk": tramo.get("pk_ref") or tramo.get("pk_inicio"),
                "hora": tramo.get("hora_ref"),
                "tipo_conflicto": tipo_conflicto,
                "descripcion": descripcion,
                "accion": accion,
                "documento_origen": str(restr.get("documento_id") or ""),
                "archivo": restr.get("archivo") or tramo.get("archivo"),
            }
        )
        stats["ok"] += 1

    return conflictos, stats


def _tramo_desde_paso(
    paso: dict[str, Any],
    paso_siguiente: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    """Compatibilidad: construye tramo unitario desde paso + siguiente."""
    if paso_siguiente is None:
        paso_siguiente = paso

    pk_ini = normalizar_pk(paso.get("pk"))
    pk_fin = normalizar_pk(paso_siguiente.get("pk"))
    if pk_ini is None or pk_fin is None:
        return None

    hora_ini_t = _hora_a_time(paso.get("hora"))
    hora_fin_t = _hora_a_time(paso_siguiente.get("hora"))
    if hora_ini_t is None or hora_fin_t is None:
        return None

    base = date(2000, 1, 1)
    dt_ini = datetime.combine(base, hora_ini_t)
    dt_fin = datetime.combine(base, hora_fin_t)
    dt_ini, dt_fin = normalizar_intervalo_datetime(dt_ini, dt_fin)

    return {
        "tren": paso.get("tren"),
        "linea": normalizar_linea(paso.get("linea")),
        "pk_inicio": pk_ini,
        "pk_fin": pk_fin,
        "dt_inicio": dt_ini,
        "dt_fin": dt_fin,
        "hora_ref": normalizar_hora(paso.get("hora")),
        "pk_ref": pk_ini,
        "archivo": paso.get("archivo") or (paso_siguiente or {}).get("archivo"),
    }


def detectar_conflictos_tba(
    paso_o_tramo: dict[str, Any],
    tba_rows: list[dict[str, Any]],
    paso_siguiente: dict[str, Any] | None = None,
    modo: str = "real",
) -> list[dict[str, Any]]:
    """Detecta conflictos TBA por tramo (compatibilidad con paso + siguiente)."""
    tramo = paso_o_tramo if "dt_inicio" in paso_o_tramo and "dt_fin" in paso_o_tramo else _tramo_desde_paso(paso_o_tramo, paso_siguiente)
    if tramo is None:
        return []

    conflictos, _ = _detectar_conflictos_restriccion_por_tramo(tramo, tba_rows, es_tba=True, modo=modo)
    return conflictos


def detectar_conflictos_tbp(
    paso_o_tramo: dict[str, Any],
    tbp_rows: list[dict[str, Any]],
    paso_siguiente: dict[str, Any] | None = None,
    modo: str = "real",
) -> list[dict[str, Any]]:
    """Detecta conflictos TBP por tramo (compatibilidad con paso + siguiente)."""
    tramo = paso_o_tramo if "dt_inicio" in paso_o_tramo and "dt_fin" in paso_o_tramo else _tramo_desde_paso(paso_o_tramo, paso_siguiente)
    if tramo is None:
        return []

    conflictos, _ = _detectar_conflictos_restriccion_por_tramo(tramo, tbp_rows, es_tba=False, modo=modo)
    return conflictos


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


def detectar_conflictos_velocidad(
    paso: dict[str, Any],
    velocidades: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Detecta conflictos de velocidad para un paso de malla."""
    conflictos: list[dict[str, Any]] = []

    pk = normalizar_pk(paso.get("pk"))
    hora = normalizar_hora(paso.get("hora"))
    linea = normalizar_linea(paso.get("linea"))

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


def calcular_conflictos(sqlite_service: Any, tren: str | None = None, modo: str = "real") -> list[dict[str, Any]]:
    """Detecta conflictos operativos y los persiste en tabla conflictos."""
    modo_norm = _validar_modo(modo)
    mallas = _obtener_rows(sqlite_service, "mallas", "tren, orden, hora", tren=tren)
    tba_rows = _obtener_rows(sqlite_service, "tba", "linea, pk_inicio, hora_inicio")
    tbp_rows = _obtener_rows(sqlite_service, "tbp", "linea, pk_inicio, hora_inicio")
    velocidades = _obtener_rows(sqlite_service, "velocidades", "linea, pk")

    print(f"[DEBUG][MODO] {modo_norm}")
    print(
        f"[DEBUG][CONFLICTOS] mallas={len(mallas)} tba={len(tba_rows)} "
        f"tbp={len(tbp_rows)} velocidades={len(velocidades)}"
    )

    limpiar_conflictos(sqlite_service)

    conflictos: list[dict[str, Any]] = []
    tramos = construir_tramos_malla(mallas)
    print(f"[DEBUG][CONFLICTOS] tramos_construidos={len(tramos)}")

    stats_tba = {"comparadas": 0, "desc_linea": 0, "desc_pk": 0, "desc_tiempo": 0, "desc_vigencia": 0, "ok": 0}
    stats_tbp = {"comparadas": 0, "desc_linea": 0, "desc_pk": 0, "desc_tiempo": 0, "desc_vigencia": 0, "ok": 0}

    for tramo in tramos:
        confl_tba, tramo_stats_tba = _detectar_conflictos_restriccion_por_tramo(tramo, tba_rows, es_tba=True, modo=modo_norm)
        confl_tbp, tramo_stats_tbp = _detectar_conflictos_restriccion_por_tramo(tramo, tbp_rows, es_tba=False, modo=modo_norm)
        conflictos.extend(confl_tba)
        conflictos.extend(confl_tbp)
        for key in stats_tba:
            stats_tba[key] += tramo_stats_tba[key]
            stats_tbp[key] += tramo_stats_tbp[key]

    for paso in mallas:
        conflictos.extend(detectar_conflictos_velocidad(paso, velocidades))

    insertados = insertar_conflictos(sqlite_service, conflictos)

    print(
        "[DEBUG][TBA] "
        f"comparadas={stats_tba['comparadas']} descartes_linea={stats_tba['desc_linea']} "
        f"descartes_pk={stats_tba['desc_pk']} descartes_tiempo={stats_tba['desc_tiempo']} "
        f"descartes_vigencia={stats_tba['desc_vigencia']} "
        f"conflictos={stats_tba['ok']}"
    )
    print(
        "[DEBUG][TBP] "
        f"comparadas={stats_tbp['comparadas']} descartes_linea={stats_tbp['desc_linea']} "
        f"descartes_pk={stats_tbp['desc_pk']} descartes_tiempo={stats_tbp['desc_tiempo']} "
        f"descartes_vigencia={stats_tbp['desc_vigencia']} "
        f"conflictos={stats_tbp['ok']}"
    )
    print(f"[DEBUG][CONFLICTOS] detectados={len(conflictos)} insertados_sin_duplicado={insertados}")

    return conflictos


def detectar_conflictos(sqlite_service: Any, tren: str | None = None, modo: str = "real") -> list[dict[str, Any]]:
    """Alias de compatibilidad con nombre previo."""
    return calcular_conflictos(sqlite_service, tren=tren, modo=modo)
