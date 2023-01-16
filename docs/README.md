# pepdbagent

`pepdbagent` is a Python package for uploading, updating, and retrieving PEP metadata from a Postgres database.

Generally pepdbagent consists of its core - **PEPDatabaseAgent** class that conduct 3 main parts: **Projects**, 
**Projects Annotations**, **Namespace Annotations**. These parts have different purpose and were separated
to increase readability, maintainability and simplicity of pepdbagent package. Below, you can find 
more detailed information about them.

## PEPDatabaseAgent
PEPDatabaseAgent is core function, it helps set connection to database (by using **BaseConnection** class)
and provide user with 3 main modules: `project`,  `annotation`, `namespace`.

Example:
Set connection to database
```python

import pepdbagent
# 1) By providing credentials and connection information:
agent = pepdbagent.PEPDatabaseAgent(user="postgres", password="docker", )
# 2) or By providing connection string:
agent = pepdbagent.PEPDatabaseAgent("postgresql://postgres:docker@localhost:5432/pep-db")
```

## Project
Project is a module that has 3 main purposes:
- Submitting/Editing projects (+editing metadata)
- Retrieving projects
- Deleting projects

Example:

```python
import peppy
prj_obj = peppy.Project("prj_path")

# submit a project
agent.project.submit(prj_obj, namespace, name, tag)

# addit project/metadata
update_dict = {"is_private"=True}
agent.project.edit(update_dict, namespace, name, tag)

# retrieve a project
agent.project.get(namespace, name, tag)

# delete a project
agent.project.delete(namespace, name, tag)
```

## Annotation 
Annotation helps retrieve metadata of PEPs. Additionally, it provides project search functionality

Example:
```python
# Get annotation of one project:
agent.annotation.get(namespace, name, tag)

# Get annotations of all projects from db:
agent.annotation.get()

# Get annotations of all projects within namespace:
agent.annotation.get(namespace='namespace')

# Query project within namespace or all database
agent.annotation.get(query='query')

# Get annotation of projects from list
agent.annotation.get_by_rp(["list/of:proj", "by/registry:path"])

# Additionally, to get annotations from private projects you should provide 
# admin or list of admins.
```


# Namespace
Namespace module helps retrieve information about namespaces and provide search functionality.

Example:
```python
# Get info about namespace by searching them in query
agent.namespace.get(query='Namespace')

# The same way as in annotation you should provide admin list, to have 
# information about private projects added to information about namespaces
agent.namespace.get(query='geo', admin=['databio', 'geo', 'ncbi'])
```