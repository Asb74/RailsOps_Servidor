"""Motor de detección de conflictos (v1).

Esta versión compara los pasos de la malla con restricciones TBA/TBP
almacenadas en SQLite y persiste los conflictos detectados.
"""

from __future__ import annotations

from datetime import datetime, time
from typing import Any


def _parse_hora(hora_str: str | None) -> time | None:
    """Intenta parsear una hora en formatos comunes (HH:MM[:SS])."""
    if not hora_str:
        return None

    valor = hora_str.strip()
    for fmt in ("%H:%M:%S", "%H:%M"):
        try:
            return datetime.strptime(valor, fmt).time()
        except ValueError:
            continue
    return None


def _parse_fecha_hora(fecha_str: str | None, hora_str: str | None) -> datetime | None:
    """Parsea fecha+hora en varios formatos de fecha habituales."""
    if not fecha_str or not hora_str:
        return None

    hora = _parse_hora(hora_str)
    if hora is None:
        return None

    fecha = fecha_str.strip()
    for fecha_fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y"):
        try:
            fecha_dt = datetime.strptime(fecha, fecha_fmt)
            return datetime.combine(fecha_dt.date(), hora)
        except ValueError:
            continue
    return None


def _pk_en_rango(pk: float | None, pk_inicio: float | None, pk_fin: float | None) -> bool:
    """Valida si un PK puntual cae dentro de un rango [inicio, fin]."""
    if pk is None or pk_inicio is None or pk_fin is None:
        return False

    bajo = min(pk_inicio, pk_fin)
    alto = max(pk_inicio, pk_fin)
    return bajo <= pk <= alto


def _hora_en_intervalo(
    hora_paso: str | None,
    fecha_ini: str | None,
    hora_ini: str | None,
    fecha_fin: str | None,
    hora_fin: str | None,
) -> bool:
    """Comprueba si la hora del paso está dentro de la ventana temporal.

    Reglas v1:
    - Si hay fecha+hora completas en ambos extremos, compara datetime.
    - Si sólo hay horas (sin fechas), compara dentro del mismo día.
    - Si falta información necesaria, devuelve False (no se inventan datos).
    """
    paso_hora = _parse_hora(hora_paso)
    if paso_hora is None:
        return False

    inicio_dt = _parse_fecha_hora(fecha_ini, hora_ini)
    fin_dt = _parse_fecha_hora(fecha_fin, hora_fin)

    if inicio_dt and fin_dt:
        # v1: sin fecha en malla, se proyecta al día de inicio de restricción.
        paso_dt = datetime.combine(inicio_dt.date(), paso_hora)
        return inicio_dt <= paso_dt <= fin_dt

    inicio_hora = _parse_hora(hora_ini)
    fin_hora = _parse_hora(hora_fin)
    if inicio_hora and fin_hora:
        bajo = min(inicio_hora, fin_hora)
        alto = max(inicio_hora, fin_hora)
        return bajo <= paso_hora <= alto

    return False


def _es_corte_total(restriccion: dict[str, Any]) -> bool:
    """Heurística básica para detectar corte total en TBA/TBP."""
    texto = " ".join(
        str(restriccion.get(campo, "") or "")
        for campo in ("tipo", "vias", "estacion_inicio", "estacion_fin")
    ).lower()

    claves = (
        "corte total",
        "vía cortada",
        "via cortada",
        "sin servicio",
        "interrupción total",
        "interrupcion total",
    )
    return any(clave in texto for clave in claves)


def _leer_tabla(
    conn: Any,
    tabla: str,
    tren: str | None = None,
    linea: str | None = None,
) -> list[dict[str, Any]]:
    """Lee una tabla con filtros opcionales por tren/línea."""
    cur = conn.cursor()

    where: list[str] = []
    params: list[Any] = []

    if linea:
        where.append("linea = ?")
        params.append(linea)

    if tabla == "mallas" and tren:
        where.append("tren = ?")
        params.append(tren)

    sql = f"SELECT * FROM {tabla}"
    if where:
        sql += " WHERE " + " AND ".join(where)

    if tabla == "mallas":
        sql += " ORDER BY tren, orden, hora"
    else:
        sql += " ORDER BY linea, pk_inicio, hora_inicio"

    cur.execute(sql, params)
    return cur.fetchall()


def detectar_conflictos(sqlite_service: Any, tren: str | None = None, linea: str | None = None) -> list[dict[str, Any]]:
    """Detecta conflictos entre mallas y restricciones TBA/TBP.

    Args:
        sqlite_service: módulo/servicio con métodos get_connection() e insertar_conflicto().
        tren: filtro opcional por identificador de tren.
        linea: filtro opcional por línea.

    Returns:
        Lista de conflictos detectados (y también persistidos en tabla `conflictos`).

    Limitaciones de esta versión (v1):
    - No usa segundos, sólo HH:MM[:SS] cuando esté disponible.
    - No interpreta periodicidad avanzada (laborables/festivos, etc.).
    - Si la malla no trae fecha, se evalúa contra la fecha de inicio de la restricción.
    - No deduplica entre ejecuciones: cada ejecución inserta nuevas filas en `conflictos`.
    - Si falta PK u hora en el paso o restricción, el conflicto se omite por diseño.
    """

    conn = sqlite_service.get_connection()
    try:
        pasos_malla = _leer_tabla(conn, "mallas", tren=tren, linea=linea)
        restricciones_tba = _leer_tabla(conn, "tba", linea=linea)
        restricciones_tbp = _leer_tabla(conn, "tbp", linea=linea)
    finally:
        conn.close()

    conflictos: list[dict[str, Any]] = []

    def registrar_conflicto(paso: dict[str, Any], restriccion: dict[str, Any], tipo_conflicto: str, accion: str) -> None:
        descripcion = (
            f"Conflicto {tipo_conflicto}: tren {paso.get('tren')} en línea {paso.get('linea')} "
            f"(PK {paso.get('pk')}, hora {paso.get('hora')}) con restricción {restriccion.get('id')}."
        )
        documento_origen = str(restriccion.get("documento_id")) if restriccion.get("documento_id") is not None else None

        conflicto = {
            "tren": paso.get("tren"),
            "linea": paso.get("linea"),
            "pk": paso.get("pk"),
            "hora": paso.get("hora"),
            "tipo_conflicto": tipo_conflicto,
            "descripcion": descripcion,
            "accion": accion,
            "documento_origen": documento_origen,
        }
        conflictos.append(conflicto)

        sqlite_service.insertar_conflicto(**conflicto)

    for paso in pasos_malla:
        paso_pk = paso.get("pk")
        paso_hora = paso.get("hora")

        # Requisito explícito: no inventar datos si falta PK u hora.
        if paso_pk is None or not paso_hora:
            continue

        linea_paso = paso.get("linea")

        # Regla 1: conflicto general con TBA/TBP por PK + intervalo horario.
        for restr in restricciones_tba + restricciones_tbp:
            if linea_paso and restr.get("linea") != linea_paso:
                continue

            if not _pk_en_rango(paso_pk, restr.get("pk_inicio"), restr.get("pk_fin")):
                continue

            if not _hora_en_intervalo(
                paso_hora,
                restr.get("fecha_inicio"),
                restr.get("hora_inicio"),
                restr.get("fecha_fin"),
                restr.get("hora_fin"),
            ):
                continue

            if _es_corte_total(restr):
                registrar_conflicto(
                    paso,
                    restr,
                    "corte_total",
                    "Detener antes de estación de entrada",
                )
            else:
                registrar_conflicto(
                    paso,
                    restr,
                    "restriccion_operativa",
                    "Recalcular horario / reducir velocidad",
                )

        # Regla 2: conflicto de velocidad con TBP que limite velocidad.
        for tbp in restricciones_tbp:
            if linea_paso and tbp.get("linea") != linea_paso:
                continue

            if tbp.get("velocidad_limitada") is None:
                continue

            if not _pk_en_rango(paso_pk, tbp.get("pk_inicio"), tbp.get("pk_fin")):
                continue

            if not _hora_en_intervalo(
                paso_hora,
                tbp.get("fecha_inicio"),
                tbp.get("hora_inicio"),
                tbp.get("fecha_fin"),
                tbp.get("hora_fin"),
            ):
                continue

            registrar_conflicto(
                paso,
                tbp,
                "conflicto_velocidad",
                "Recalcular horario / reducir velocidad",
            )

    return conflictos


if __name__ == "__main__":
    # Ejemplo de uso rápido (CLI):
    # python -m backend.core.motor_conflictos
    from backend.db import sqlite_service

    conflictos_detectados = detectar_conflictos(sqlite_service)
    print(f"Conflictos detectados: {len(conflictos_detectados)}")
    for c in conflictos_detectados[:10]:
        print(c)
