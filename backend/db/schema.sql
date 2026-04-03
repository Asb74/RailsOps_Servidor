PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS documentos (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    nombre TEXT NOT NULL,
    tipo TEXT NOT NULL,
    version TEXT,
    gmail_id TEXT,
    fecha_procesado TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS tba (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    documento_id INTEGER NOT NULL,
    linea TEXT NOT NULL,
    estacion_inicio TEXT,
    estacion_fin TEXT,
    pk_inicio REAL,
    pk_fin REAL,
    fecha_inicio TEXT,
    hora_inicio TEXT,
    fecha_fin TEXT,
    hora_fin TEXT,
    tipo TEXT,
    periodicidad TEXT,
    vias TEXT,
    archivo TEXT,
    FOREIGN KEY (documento_id) REFERENCES documentos(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS tbp (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    documento_id INTEGER NOT NULL,
    linea TEXT NOT NULL,
    estacion_inicio TEXT,
    estacion_fin TEXT,
    pk_inicio REAL,
    pk_fin REAL,
    fecha_inicio TEXT,
    hora_inicio TEXT,
    fecha_fin TEXT,
    hora_fin TEXT,
    tipo TEXT,
    periodicidad TEXT,
    vias TEXT,
    velocidad_limitada REAL,
    archivo TEXT,
    FOREIGN KEY (documento_id) REFERENCES documentos(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS mallas (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    documento_id INTEGER NOT NULL,
    tren TEXT NOT NULL,
    linea TEXT NOT NULL,
    estacion TEXT,
    pk REAL,
    hora TEXT,
    orden INTEGER,
    archivo TEXT,
    FOREIGN KEY (documento_id) REFERENCES documentos(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS velocidades (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    documento_id INTEGER NOT NULL,
    linea TEXT NOT NULL,
    pk REAL NOT NULL,
    velocidad_max REAL NOT NULL,
    tipo_tren TEXT,
    archivo TEXT,
    FOREIGN KEY (documento_id) REFERENCES documentos(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS conflictos (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    tren TEXT,
    linea TEXT,
    pk REAL,
    hora TEXT,
    tipo_conflicto TEXT,
    descripcion TEXT,
    accion TEXT,
    documento_origen TEXT,
    fecha_detectado TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Índices por línea
CREATE INDEX IF NOT EXISTS idx_tba_linea ON tba(linea);
CREATE INDEX IF NOT EXISTS idx_tbp_linea ON tbp(linea);
CREATE INDEX IF NOT EXISTS idx_mallas_linea ON mallas(linea);
CREATE INDEX IF NOT EXISTS idx_velocidades_linea ON velocidades(linea);
CREATE INDEX IF NOT EXISTS idx_conflictos_linea ON conflictos(linea);

-- Índices por PK
CREATE INDEX IF NOT EXISTS idx_tba_pk_inicio_fin ON tba(pk_inicio, pk_fin);
CREATE INDEX IF NOT EXISTS idx_tbp_pk_inicio_fin ON tbp(pk_inicio, pk_fin);
CREATE INDEX IF NOT EXISTS idx_mallas_pk ON mallas(pk);
CREATE INDEX IF NOT EXISTS idx_velocidades_pk ON velocidades(pk);
CREATE INDEX IF NOT EXISTS idx_conflictos_pk ON conflictos(pk);

-- Índices por tren
CREATE INDEX IF NOT EXISTS idx_mallas_tren ON mallas(tren);
CREATE INDEX IF NOT EXISTS idx_conflictos_tren ON conflictos(tren);
