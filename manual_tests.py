# file for manual local tests
import peppy
import os
import pepdbagent
from peppy import Project


DATA_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "tests",
    "data",
)


def get_path_to_example_file(namespace, project_name):
    return os.path.join(DATA_PATH, namespace, project_name, "project_config.yaml")


def list_of_available_peps():
    pep_namespaces = os.listdir(DATA_PATH)
    projects = {}
    for np in pep_namespaces:
        pep_name = os.listdir(os.path.join(DATA_PATH, np))
        projects[np] = {p: get_path_to_example_file(np, p) for p in pep_name}
    return projects


def upload_sample_pep_to_db(connection: pepdbagent.PEPDatabaseAgent):
    list_of_peps = list_of_available_peps()
    for namespace, item in list_of_peps.items():
        if namespace == "private_test":
            private = True
        else:
            private = False
        for name, path in item.items():
            prj = peppy.Project(path)
            connection.project.create(
                namespace=namespace,
                name=name,
                tag="default",
                is_private=private,
                project=prj,
                overwrite=True,
                pep_schema="random_schema_name",
            )

    return None


# populate database with few peps:
con = pepdbagent.PEPDatabaseAgent(dsn="postgresql://postgres:docker@localhost:5432/pep-db", echo=False)
upload_sample_pep_to_db(con)


###############
# # Upload

prj = peppy.Project(
    "/home/bnt4me/virginia/repos/pepdbagent/tests/data/namespace1/basic/project_config.yaml"
)
fgf = prj.to_dict()
rr = prj.to_dict(extended=True)
con.project.create(project=prj, namespace="dog_namespace", name="testttt", tag="test1", overwrite=True)


pr = con.project.get(namespace="dog_namespace", name="testttt", tag="test1", raw=True)
pr
con.project.exists(namespace="dog_namespace", name="testttt", tag="test1")


prj_raw = con.project.get(namespace="dog_namespace", name="testttt", tag="test1", raw=True)

print(prj_raw)


###############
# Annotation

dd_list = con.annotation.get_by_rp(
    [
        "dog_namespace/gse_yaml:default",
        "dog_namespace/gse_yaml:default",
        "dog_namespace/testttt:f1",
    ],
    admin="dog_namespace",
)
dd_list_private = con.annotation.get_by_rp(
    [
        "dog_namespace/gse_yaml:default",
        "dog_namespace/gse_yaml:default",
        "dog_namespace/testttt:f1",
    ]
)

dd_search = con.annotation.get(namespace="dog_namespace")
dd_search_pr = con.annotation.get(namespace="dog_namespace", admin="dog_namespace")
dd_search_pr_namespace = con.annotation.get(
    query="s", admin=["dog_namespace", "test_11"]
)

dd_all = con.annotation.get(admin=["dog_namespace", "test_11"])


print(dd_list)
print(dd_list_private)

print(dd_search)
print(dd_search_pr)
print(dd_search_pr_namespace)

print(dd_all)

################
# Namespace

ff = con.namespace.get("dog_namespace", admin="dog_namespace")

print(ff)

con.project.update(update_dict={"is_private": False}, namespace="dog_namespace", name="testttt", tag="test1")

dell = con.project.delete(namespace="dog_namespace", name="testttt", tag="test1")


from pydantic import BaseModel