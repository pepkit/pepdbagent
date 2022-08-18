# pepagent + pep_db

Database and PEPagent for storing and processing pep projects

---
## How to create pep_db:

Complete instruction can be found here: [pep_db](pep_db)

---
## How to use pepagent
1) Create connection with DB:
```python
# 1) By providing credentials and connection information:
projectDB = PEPagent(user="postgres", password="docker",)
# 2) or By providing connection string:
projectDB = PEPagent("postgresql://postgres:docker@localhost:5432/pep-base-sql")
```

2) Add new project to the DB
```python
# initiate peppy Project
pep_project = peppy.Project("/sample_pep/subtable3/project_config.yaml")
# use upload_project function to add this project to the DB
projectDB.upload_project(pep_project, namespace = "Test", anno={"project": "annotation_dict"})  
# additionally you can specify name and tag of the project

# update project

projectDB.update_project(pep_project, namespace = "Test", anno={"enot": "annotation_dict"})  
# additionally you can specify name and tag of the project

```

3) Get list of projects in namespace:
```python
list_of_namespaces = projectDB.get_namespace_info(namespace="King")
print(list_of_namespaces)

```

4) Get list of available namespaces:

```python
list_of_namespaces = projectDB.get_namespaces_info_by_list()
print(list_of_namespaces)
# To get list with with just names of namespaces set: names=True
# otherwise you will get list with namespaces with information about all projects
```

5) Get project

```python

# Get project by registry
pr_ob = projectDB.get_project_by_registry(registry_path='Test/subtable3')
print(pr_ob.samples)

# Get project by registry
pr_ob = projectDB.get_project_by_registry(registry_path='Test/subtable3:this_is_tag')
print(pr_ob.samples)

# Get project by namespace and name
pr_ob = projectDB.get_project(namespace='Test', name='subtable3')
print(pr_ob.samples)

# Get project by namespace and name
pr_ob = projectDB.get_project(namespace='Test', name='subtable3', tag='this_is_tag')
print(pr_ob.samples)

```

4) Get list of projects

```python
# Get projects by tag
pr_ob = projectDB.get_projects_in_namespace(tag='new_tag')
print(pr_ob.samples)

# Get projects by namespace
pr_ob = projectDB.get_projects_in_namespace(namespace='King')
print(pr_ob.samples)

# Get projects by namespace and tag
pr_ob = projectDB.get_projects_in_namespace(namespace='King', tag='taggg')
print(pr_ob.samples)

# Get projects by list of registry paths
pr_ob = projectDB.get_projects_in_list(registry_paths=['Test/subtable3:default', 'Test/subtable3:bbb'])
print(pr_ob.samples)

# Get all the projects
pr_ob = projectDB.get_project_all()
print(pr_ob.samples)

```

5) Get annotation about single project or projects:

```python

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


7) Check project existance:

```python
# by name and namespace:
projectDB.project_exists(namespace="nn", name="buu")

# by name and namespace and tag:
projectDB.project_exists(namespace="nn", name="buu", tag='dog')

# by registry path:
projectDB.project_exists_by_registry(registry_path='nn/buu/dog')

```


8) Check project status:

```python
# by name and namespace and tag:
# Get dictionary of annotation for specific namespace
projectDB.project_status(namespace="nn", name="buu", tag='dog')

# by registry path:
projectDB.project_status_by_registry(registry_path='nn/buu/dog')
```

9) Get registry paths of all the projects by digest:

```python
projectDB.get_registry_paths_by_digest(digest='sdafsgwerg243rt2gregw3qr24')
```