-- PostgreSQL port of the MySQL "pep-db" database.
--
-- authors: ["Oleksandr Khoroshevskyi"]

SET client_encoding = 'LATIN1';


CREATE TABLE projects (
    id BIGSERIAL NOT NULL,
    namespace TEXT NOT NULL,
    name TEXT NOT NULL,
    tag TEXT NOT NULL,
    digest TEXT NOT NULL,  -- should be changed to CHARACTER
    project_value jsonb NOT NULL,
    private bool NOT NULL,
    anno_info jsonb,  -- annotation information
    CONSTRAINT id PRIMARY KEY (namespace, name, tag)
);

-- if you are changing database, don't forget to chane it in const.py file
