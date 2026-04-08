CREATE TABLE IF NOT EXISTS specialties (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS students (
    id UUID PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    status VARCHAR(50) NOT NULL,
    specialty_id INTEGER NOT NULL REFERENCES specialties(id)
);

INSERT INTO specialties (id, name) VALUES
    (1, 'Программная инженерия'),
    (2, 'Прикладная информатика'),
    (3, 'Информационные системы и технологии'),
    (4, 'Информационная безопасность')
ON CONFLICT (id) DO NOTHING;