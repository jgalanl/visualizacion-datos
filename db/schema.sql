CREATE TABLE IF NOT EXISTS convocatorias(
    id INTEGER PRIMARY KEY,
    codigo_bdns INTEGER,
    MRR VARCHAR(2),
    administracion TEXT,
    departamento TEXT,
    organo TEXT,
    fecha_registro TEXT,
    titulo_convocatoria TEXT,
    url_bases_regul TEXT,
    titutlo_cooficial TEXT,
    colDesconocida1 TEXT,
    colDesconocida2 INTEGER,
    colDesconocida3 INTEGER
);