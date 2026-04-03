-- Esquema SQLite de RailOps.
-- Responsabilidad: persistencia local mínima para documentos y restricciones.

CREATE TABLE IF NOT EXISTS documentos (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    nombre TEXT NOT NULL,
    tipo TEXT NOT NULL,
    gmail_id TEXT,
    procesado INTEGER DEFAULT 1,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS restricciones (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    documento_id INTEGER,
    archivo TEXT,
    payload_json TEXT NOT NULL,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);
