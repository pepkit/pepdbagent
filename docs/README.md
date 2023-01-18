# pepdbagent

`pepdbagent` is a Python package for uploading, updating, and retrieving [PEP](http://pep.databio.org/en/latest/) metadata from a Postgres database.

The pepdbagent provides a core class called **PEPDatabaseAgent**. This class has 3 modules, divided 
to increase readability, maintainability, and user experience of pepdbagent, which are: **Projects**, 
**Project Annotations**, and **Namespace Annotations**.  Below, we describe each module in detail:

## PEPDatabaseAgent
PEPDatabaseAgent is the primary class that you will use. It connects to the database (using **BaseConnection** class).

Example: Instiantiate a PEPDatabaseAgent object and connect to database:
```python

import pepdbagent
# 1) By providing credentials and connection information:
agent = pepdbagent.PEPDatabaseAgent(user="postgres", password="docker", )
# 2) or By providing connection string:
agent = pepdbagent.PEPDatabaseAgent(dsn="postgresql://postgres:docker@localhost:5432/pep-db")
```

This `agent` object will provide 3 sub-modules, corresponding to the 3 main entity types stored in PEPhub: `agent.project`,  `agent.annotation`, and `agent.namespace`.

## Project

The `.project` module is used to create, update, retrieve, and delete projects.

Example:

```python
import peppy
prj_obj = peppy.Project("/path/to/project_config.yaml")

# submit a project
agent.project.submit(prj_obj, namespace, name, tag)

# edit project/metadata
update_dict = {"is_private"=True}
agent.project.edit(update_dict, namespace, name, tag)

# retrieve a project
agent.project.get(namespace, name, tag)

# delete a project
agent.project.delete(namespace, name, tag)
```

## Annotation

The `.annotation` module provides an interface to PEP annotations. PEP annotations refers to the information *about* the PEPs (or, the PEP metadata). It also provides project search functionality.

Example:
```python
# Get annotation of one project:
agent.annotation.get(namespace, name, tag)

# Get annotations of all projects from db:
agent.annotation.get()

# Get annotations of all projects within a given namespace:
agent.annotation.get(namespace='namespace')

# Search for a project with partial string matching, either within namespace or entire database
# This returns a list of projects
agent.annotation.get(query='query')
agent.annotation.get(query='query', namespace='namespace')

# Get annotation of multiple projects given a list of registry paths
agent.annotation.get_by_rp(["namespace1/project1:tag1", "namespace2/project2:tag2"])

# Additionally, to get annotations from private projects you should provide 
# admin or list of admins.
```


# Namespace
The `.namespace` module retrieves information about namespaces and provides search functionality.

Example:
```python
# Get info about namespace by searching them in query
agent.namespace.get(query='Namespace')

# The same way as in annotation you should provide admin list, to have 
# information about private projects added to information about namespaces
agent.namespace.get(query='geo', admin=['databio', 'geo', 'ncbi'])
```
