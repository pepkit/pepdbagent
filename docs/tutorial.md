# pepdbagent

pepdbagent is a package for uploading, updating and retrieving Projects and it'd metadata from database.

### Main functionalities:
- Uploading and updating peppy projects and metadata.
- Retrieving projects from database.
- Deleting projects.
- Retrieving metadata about projects and namespaces.
- Searching projects and namespaces inside database.

## Brief tutorial

### Creating connection between database and agent:
```python
# 1) By providing credentials and connection information:
con = Connection(user="postgres", password="docker",)
# 2) or By providing connection string:
con = Connection("postgresql://postgres:docker@localhost:5432/pep-db")
```

### Uploading, updating, deleting records.

1) **Uploading projects**
```python
# initiate peppy Project
import peppy
pep_project = peppy.Project("/sample_pep/subtable3/project_config.yaml")
# use upload_project function to add this project to the DB
con.upload_project(pep_project, 
                   namespace = "test", 
                   name = "test", 
                   tag = "test", 
                   description = "ocean dream", 
                   is_private=True)
```

2) **Re-uploading existing project**:
```python
# To re-upload existing project you can use upload function with argument overwrite set True
con.upload_project(..., 
                   overwrite=True)
```

3) **Updating individual items** in the record. 
There is possibility to update separately: [`project`,`is_private`, `tag`,`name`].

```python
# Example 1
update_dict = {
    "is_private": True,
    "tag": "new_tag",
}
con.update_item(namespace="test",
                name="test", 
                tag="test",
                update_dict=update_dict)

# Example 2
p_roj = peppy.Project(...)
update_dict = {
    "project": p_roj,
    "is_private": True,
}
con.update_item(namespace="test",
                name="test", 
                tag="test",
                update_dict=update_dict)
```

4) **Deleting record**
```python
con.delete_project(namespace="test",
                   name="test",
                   tag="test")
```


### Retrieving projects
1)
```python
# Get project by namespace and name
proj = con.get_project(namespace='test', name='test', tag='tag')

# Get project by registry path
proj = con.get_project(registry_path='Test/subtable3:this_is_tag')
```
2) **Get raw project**
```python
con.get_raw_project(namespace="test",
                   name="test",
                   tag="test")
```

### Retrieving project and namespace annotations
1) **Get annotation of one project**
```python
con.get_project_annotation(namespace="test",
                           name="test",
                           tag="test")
```
Return of this method is pydantic model, that include all metadata of the project (meta metadata)

2) **Get namespace annotations**
```python

con.get_namespace_info(snamespace='test', 
                       user= "test")
```
Return is a dictionary with a schema: {
            namespace,
            n_samples,
            n_projects,
            projects:(id, name, tag, digest, description, n_samples)}

### **Search**
Search methods can also work as annotation getters
1) Search namespaces by name

```python

con.search.namespace(search_str="some_string", 
                     admin_list=['admin1','namespace1'],
                     limit=100,
                     offset=50,
                     )
```
Example return:
```str
number_of_results=1 limit=100 offset=0 results=[NamespaceSearchResultModel(namespace='admin1', number_of_projects=3, number_of_samples=449)]
```

2) Search project inside namespace
```python

con.search.project(namespace="test_namespace",
                   admin=True,  # True if user is admin of the namespace
                   search_str="some_string", 
                     limit=100,
                     offset=0,
                     )
```
Example return:
```str
namespace='Khoroshevskyi' number_of_results=1 limit=100 offset=0 results=[ProjectSearchResultModel(namespace='Khoroshevskyi', name='test', tag='f1', is_private=True, number_of_samples=16, description='descr test', last_update=None, submission_date='2023-01-05', digest='042db0e7d79f92f4180e3f92b8372162')]
```
To get all possible projects: don't provide any searching strings


### Additional functions.

Check if project exists in database
```python
con.project_exists(namespace="test", name="test")
```
