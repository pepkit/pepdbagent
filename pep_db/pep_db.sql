-- PostgreSQL port of the MySQL "pep-db" database.
--
-- authors: ["Oleksandr Khoroshevskyi"]

SET client_encoding = 'LATIN1';


CREATE TABLE projects (
    id BIGSERIAL NOT NULL PRIMARY KEY,
    project_name text NOT NULL,
    project_value json NOT NULL,
    description text,
    n_samples_project int
);

