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
projectDB.upload_project(pep_project, namespace = "Test", anno={"project": "annotation_dict"})  
# additionally you can specify name and tag of the project

```

3) Get list of projects in namespace:
```python
list_of_namespaces = projectDB.get_namespace(namespace="King")
print(list_of_namespaces)

```

4) Get list of available namespaces:
```python
list_of_namespaces = projectDB.get_namespaces()
print(list_of_namespaces)
# To get list with with just names of namespaces set: names=True
# otherwise you will get list with namespaces with information about all projects
```

5) Get project
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

# Get project by registry
pr_ob = projectDB.get_project(registry='Test/subtable3:this_is_tag')
print(pr_ob.samples)

# Get project by namespace and name
pr_ob = projectDB.get_project(namespace='Test', name='subtable3', tag='this_is_tag')
print(pr_ob.samples)

# Get project by digest
pr_ob = projectDB.get_project(digest='1495b8d5b586ab71c9f3a30dd265b3c3')
print(pr_ob.samples)
```

4) Get list of projects
```python
# Get projects by registry
pr_ob = projectDB.get_projects(registry='Test/subtable3')
print(pr_ob.samples)

# Get projects by list of registries
pr_ob = projectDB.get_projects(registry=['Test/subtable3', 'King/pr25'] )
print(pr_ob.samples)

# Get projects by namespace
pr_ob = projectDB.get_projects(namespace='Test')
print(pr_ob.samples)

# Get project by tag
pr_ob = projectDB.get_project(tag='this_is_tag')
print(pr_ob.samples)

```

5) Get annotation about single project or projects:

```python

# Get dictionary of annotation for 1 project by id 
projects_anno_list = projectDB.get_project_annotation(id='5')
# Get dictionary of annotation for 1 project by digest
projects_anno_list = projectDB.get_project_annotation(digest='1495b8d5b586ab71c9f3a30dd265b3c3')
# Get dictionary of annotation for 1 project by registry
projects_anno_list = projectDB.get_project_annotation(digest='Test/subtable3:this_is_tag')
# if tag is not set default tag will be set
projects_anno_list = projectDB.get_project_annotation(namespace='Test/subtable3')
```

6) Get annotations namespace or all namespaces:

```python
# Get dictionary of annotation for specific namespace
namespace_anno = projectDB.get_namespace_annotation(namespace='Test')

# Get dictiionary of annotations for all namespaces
namespace_anno_all = projectDB.get_namespace_annotation()
```
