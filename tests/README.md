# Test pepdbagent

### How to run tests localy:
1. Use or create empty database with next credentials:
```txt
docker run --rm -it --name pep-db \
  -e POSTGRES_USER=postgres \
  -e POSTGRES_PASSWORD=pass8743hf9h23f87h437 \
  -e POSTGRES_DB=pep-db \
  -p 127.0.0.1:5432:5432 postgres
```


2. Run pytest using this command: `pytest`
