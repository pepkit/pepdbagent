-- PostgreSQL port of the MySQL "pep-base" database.
--
-- authors: ["Oleksandr Khoroshevskyi"]

SET client_encoding = 'LATIN1';


CREATE TABLE subsamples (
    id BIGSERIAL NOT NULL PRIMARY KEY,
    subsample json NOT NULL
);

CREATE TABLE samples(
    id BIGSERIAL NOT NULL PRIMARY KEY,
    sample json NOT NULL
);

CREATE TABLE projects (
    id BIGSERIAL NOT NULL PRIMARY KEY,
    project_name text NOT NULL,
    project_file json NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    sample_id int,
    CONSTRAINT fk_sample
        FOREIGN KEY (sample_id)
            REFERENCES samples(id),
    subsample_id int,
    CONSTRAINT fk_subsample
        FOREIGN KEY (subsample_id)
            REFERENCES subsamples(id)
);
