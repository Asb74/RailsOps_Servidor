"""Servicio SQLite para persistencia local de RailOps.

Este módulo define inicialización automática de base de datos,
funciones de inserción y consultas básicas.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from threading import Lock
from typing import Any

from backend.config import BASE_DIR

DB_PATH = BASE_DIR / "railops.db"
SCHEMA_PATH = Path(__file__).resolve().parent / "schema.sql"

_init_lock = Lock()
_is_initialized = False


def _resolve_db_path(db_path: str | Path | None = None) -> Path:
    return Path(db_path) if db_path else DB_PATH


def _row_factory_dict(cursor: sqlite3.Cursor, row: tuple[Any, ...]) -> dict[str, Any]:
    return {col[0]: row[idx] for idx, col in enumerate(cursor.description)}


def get_connection(db_path: str | Path | None = None) -> sqlite3.Connection:
    """Retorna una conexión SQLite con inicialización automática."""
    resolved_path = _resolve_db_path(db_path)
    _ensure_db_initialized(resolved_path)
    conn = sqlite3.connect(resolved_path)
    conn.execute("PRAGMA foreign_keys = ON")
    conn.row_factory = _row_factory_dict
    return conn


def _ensure_db_initialized(db_path: Path) -> None:
    """Inicializa la base si no existe o si aún no se aplicó esquema en este proceso."""
    global _is_initialized

    if _is_initialized and db_path.exists():
        return

    with _init_lock:
        if _is_initialized and db_path.exists():
            return

        db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(db_path)
        try:
            schema_sql = SCHEMA_PATH.read_text(encoding="utf-8")
            conn.executescript(schema_sql)
            conn.commit()
        finally:
            conn.close()

        _is_initialized = True


def init_db(db_path: str | Path | None = None) -> None:
    """Inicializa explícitamente la base de datos SQLite."""
    _ensure_db_initialized(_resolve_db_path(db_path))


def insertar_documento(
    nombre: str,
    tipo: str,
    version: str | None = None,
    gmail_id: str | None = None,
    fecha_procesado: str | None = None,
    db_path: str | Path | None = None,
) -> int:
    conn = get_connection(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO documentos (nombre, tipo, version, gmail_id, fecha_procesado)
            VALUES (?, ?, ?, ?, COALESCE(?, CURRENT_TIMESTAMP))
            """,
            (nombre, tipo, version, gmail_id, fecha_procesado),
        )
        conn.commit()
        return cur.lastrowid
    finally:
        conn.close()


def insertar_tba(
    documento_id: int,
    linea: str,
    estacion_inicio: str | None = None,
    estacion_fin: str | None = None,
    pk_inicio: float | None = None,
    pk_fin: float | None = None,
    fecha_inicio: str | None = None,
    hora_inicio: str | None = None,
    fecha_fin: str | None = None,
    hora_fin: str | None = None,
    tipo: str | None = None,
    periodicidad: str | None = None,
    vias: str | None = None,
    db_path: str | Path | None = None,
) -> int:
    conn = get_connection(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO tba (
                documento_id, linea, estacion_inicio, estacion_fin, pk_inicio, pk_fin,
                fecha_inicio, hora_inicio, fecha_fin, hora_fin, tipo, periodicidad, vias
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                documento_id,
                linea,
                estacion_inicio,
                estacion_fin,
                pk_inicio,
                pk_fin,
                fecha_inicio,
                hora_inicio,
                fecha_fin,
                hora_fin,
                tipo,
                periodicidad,
                vias,
            ),
        )
        conn.commit()
        return cur.lastrowid
    finally:
        conn.close()


def insertar_tbp(
    documento_id: int,
    linea: str,
    estacion_inicio: str | None = None,
    estacion_fin: str | None = None,
    pk_inicio: float | None = None,
    pk_fin: float | None = None,
    fecha_inicio: str | None = None,
    hora_inicio: str | None = None,
    fecha_fin: str | None = None,
    hora_fin: str | None = None,
    tipo: str | None = None,
    periodicidad: str | None = None,
    vias: str | None = None,
    velocidad_limitada: float | None = None,
    db_path: str | Path | None = None,
) -> int:
    conn = get_connection(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO tbp (
                documento_id, linea, estacion_inicio, estacion_fin, pk_inicio, pk_fin,
                fecha_inicio, hora_inicio, fecha_fin, hora_fin, tipo, periodicidad, vias,
                velocidad_limitada
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                documento_id,
                linea,
                estacion_inicio,
                estacion_fin,
                pk_inicio,
                pk_fin,
                fecha_inicio,
                hora_inicio,
                fecha_fin,
                hora_fin,
                tipo,
                periodicidad,
                vias,
                velocidad_limitada,
            ),
        )
        conn.commit()
        return cur.lastrowid
    finally:
        conn.close()


def insertar_malla(
    documento_id: int,
    tren: str,
    linea: str,
    estacion: str | None = None,
    pk: float | None = None,
    hora: str | None = None,
    orden: int | None = None,
    db_path: str | Path | None = None,
) -> int:
    conn = get_connection(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO mallas (documento_id, tren, linea, estacion, pk, hora, orden)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (documento_id, tren, linea, estacion, pk, hora, orden),
        )
        conn.commit()
        return cur.lastrowid
    finally:
        conn.close()


def insertar_velocidad(
    documento_id: int,
    linea: str,
    pk: float,
    velocidad_max: float,
    tipo_tren: str | None = None,
    db_path: str | Path | None = None,
) -> int:
    conn = get_connection(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO velocidades (documento_id, linea, pk, velocidad_max, tipo_tren)
            VALUES (?, ?, ?, ?, ?)
            """,
            (documento_id, linea, pk, velocidad_max, tipo_tren),
        )
        conn.commit()
        return cur.lastrowid
    finally:
        conn.close()


def insertar_conflicto(
    tren: str | None = None,
    linea: str | None = None,
    pk: float | None = None,
    hora: str | None = None,
    tipo_conflicto: str | None = None,
    descripcion: str | None = None,
    accion: str | None = None,
    documento_origen: str | None = None,
    fecha_detectado: str | None = None,
    db_path: str | Path | None = None,
) -> int:
    conn = get_connection(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO conflictos (
                tren, linea, pk, hora, tipo_conflicto, descripcion,
                accion, documento_origen, fecha_detectado
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, COALESCE(?, CURRENT_TIMESTAMP))
            """,
            (
                tren,
                linea,
                pk,
                hora,
                tipo_conflicto,
                descripcion,
                accion,
                documento_origen,
                fecha_detectado,
            ),
        )
        conn.commit()
        return cur.lastrowid
    finally:
        conn.close()


def listar_tba_por_linea(linea: str, db_path: str | Path | None = None) -> list[dict[str, Any]]:
    conn = get_connection(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT *
            FROM tba
            WHERE linea = ?
            ORDER BY fecha_inicio, hora_inicio, pk_inicio
            """,
            (linea,),
        )
        return cur.fetchall()
    finally:
        conn.close()


def listar_mallas_por_tren(tren: str, db_path: str | Path | None = None) -> list[dict[str, Any]]:
    conn = get_connection(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT *
            FROM mallas
            WHERE tren = ?
            ORDER BY orden, hora
            """,
            (tren,),
        )
        return cur.fetchall()
    finally:
        conn.close()


def buscar_restricciones_por_rango_pk(
    linea: str,
    pk_desde: float,
    pk_hasta: float,
    db_path: str | Path | None = None,
) -> dict[str, list[dict[str, Any]]]:
    """Busca restricciones en TBA, TBP y velocidades para un rango de PK."""
    conn = get_connection(db_path)
    try:
        cur = conn.cursor()

        cur.execute(
            """
            SELECT *
            FROM tba
            WHERE linea = ?
              AND pk_inicio <= ?
              AND pk_fin >= ?
            ORDER BY pk_inicio
            """,
            (linea, pk_hasta, pk_desde),
        )
        tba_rows = cur.fetchall()

        cur.execute(
            """
            SELECT *
            FROM tbp
            WHERE linea = ?
              AND pk_inicio <= ?
              AND pk_fin >= ?
            ORDER BY pk_inicio
            """,
            (linea, pk_hasta, pk_desde),
        )
        tbp_rows = cur.fetchall()

        cur.execute(
            """
            SELECT *
            FROM velocidades
            WHERE linea = ?
              AND pk BETWEEN ? AND ?
            ORDER BY pk
            """,
            (linea, pk_desde, pk_hasta),
        )
        velocidades_rows = cur.fetchall()

        return {
            "tba": tba_rows,
            "tbp": tbp_rows,
            "velocidades": velocidades_rows,
        }
    finally:
        conn.close()


# Alias de compatibilidad con nombre previo.
guardar_documento_local = insertar_documento
