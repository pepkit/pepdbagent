# pepagent + pep_db

Database and pep_db_agent for storing and processing pep projects

---
## How to create pep_db:

Complete instruction can be found here: [pep_db](pep_db)

---
## How to use pepagent
1) Create connection with DB:
```python
# 1) By providing credentials and connection information:
projectDB = PepAgent(user="postgres", password="docker",)
# 2) or By providing connection string:
projectDB = PepAgent("postgresql://postgres:docker@localhost:5432/pep-base-sql")
```

2) Add new project to the DB
```python
# initiate peppy Project
pep_project = peppy.Project("/sample_pep/subtable3/project_config.yaml")
# use upload_project function to add this project to the DB
projectDB.upload_project(pep_project, namespace = "Test")  
# additionally you can specify name of the project
```

3) Get list of available namespaces:
```python
list_of_namespaces = projectDB.get_namespaces()
print(list_of_namespaces)
```

4) Get project
```python
# Get project by id:
pr_ob = projectDB.get_project(id=3)
print(pr_ob.samples)

# Get project by registry
pr_ob = projectDB.get_project(registry='Test/subtable3')
print(pr_ob.samples)

# Get project by namespace and name
pr_ob = projectDB.get_project(namespace='Test', name='subtable3')
print(pr_ob.samples)

# Get project by digest
pr_ob = projectDB.get_project(digest='1495b8d5b586ab71c9f3a30dd265b3c3')
print(pr_ob.samples)
```

5) Get list of all available projects in the namespace
```python
# Get project by id:
projects_list = projectDB.get_project_list('Test')
print(projects_list)
```

6) Get annotation about single project or projects:
```python
# Get project by id:
projects_anno_list = projectDB.get_anno(namespace='Test')
projects_anno_list = projectDB.get_anno(id='5')
projects_anno_list = projectDB.get_anno(digest='1495b8d5b586ab71c9f3a30dd265b3c3')

```

