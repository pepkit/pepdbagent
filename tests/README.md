# Test pepdbagent

### How to run tests localy:
1. Use or create empty database with next credentials:
```txt
docker run --rm -it --name bedbase \
  -e POSTGRES_USER=postgres \
  -e POSTGRES_PASSWORD=docker \
  -e POSTGRES_DB=pep-db \
  -p 5432:5432 postgres
```


2. Run pytest using this command: `pytest`
