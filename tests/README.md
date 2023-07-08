# Test pepdbagent

### How to run tests localy:
1. Use or create empty database with next credentials:
```txt
POSTGRES_USER=postgres
POSTGRES_PASSWORD=docker
POSTGRES_DB=pep-db
POSTGRES_PORT=5432
```
Database can be created using docker file: [../pep_db/Dockerfile](../pep_db/Dockerfile)

To run docker use this tutorial [../docs/db_tutorial.md](../docs/db_tutorial.md)

2. Run pytest using this command: `pytest`
