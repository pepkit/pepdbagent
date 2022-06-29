# pepagent + pep_db

Database and pep_db_agent for storing and processing pep projects

--
## How to use pepagent
1) Create connection with DB:
```python
projectDB = PepAgent(user="postgres", password="docker",)
```

2) Add new project to the DB
```python
# initiate peppy Project
pep_project = peppy.Project("/sample_pep/subtable3/project_config.yaml")
# use upload_project function to add this project to the DB
projectDB.upload_project(pep_project)
```

3) Get list of available projects:
```python
list_of_projects = projectDB.get_projects_list()
print(list_of_projects)
```

4) Get project
```python
# Get project by id:
pr_ob = projectDB.get_project(project_id=3)
print(pr_ob.samples)

# #Get project by name
pr_ob = projectDB.get_project(project_name='imply')
print(pr_ob.samples)
```
