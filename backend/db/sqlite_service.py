"""Acceso a datos SQLite.
Responsabilidad: encapsular inicialización de esquema y operaciones básicas locales.
"""

import sqlite3
from pathlib import Path

from backend.config import BASE_DIR

DB_PATH = BASE_DIR / "railops.db"
SCHEMA_PATH = Path(__file__).resolve().parent / "schema.sql"


def get_connection(db_path=None):
    path = Path(db_path) if db_path else DB_PATH
    return sqlite3.connect(path)


def init_db(db_path=None):
    conn = get_connection(db_path)
    try:
        schema_sql = SCHEMA_PATH.read_text(encoding="utf-8")
        conn.executescript(schema_sql)
        conn.commit()
    finally:
        conn.close()


def guardar_documento_local(nombre, tipo, gmail_id, db_path=None):
    conn = get_connection(db_path)
    try:
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO documentos(nombre, tipo, gmail_id, procesado) VALUES (?, ?, ?, 1)",
            (nombre, tipo, gmail_id),
        )
        conn.commit()
        return cursor.lastrowid
    finally:
        conn.close()
