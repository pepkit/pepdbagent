# file for manual local tests
import peppy

import pepdbagent
from peppy import Project


con = pepdbagent.PEPDatabaseAgent(dsn="postgresql://postgres:docker@localhost:5432/pep-db", echo=True)
###############
# # Upload
prj = peppy.Project(
    "/home/bnt4me/virginia/repos/pepdbagent/sample_pep/basic/project_config.yaml"
)
con.project.create(project=prj, namespace="Khoroshevskyi", name="dupa", tag="test1", overwrite=True)

con.project.exists(namespace="Khoroshevskyi", name="dupa", tag="test1")
con.project.update(update_dict={"is_private": False}, namespace="Khoroshevskyi", name="dupa", tag="test1")
# # Project

# prj_dow = con.project.get(namespace="Khoroshevskyi", name="dupa", tag="test1")


exit(1)
print(prj_dow.name)

prj_raw = con.project.get(namespace="Khoroshfevskyi", name="dupa", tag="test1", raw=True)

print(prj_raw)


###############
# Annotation

dd_list = con.adialectnnotation.get_by_rp(
    [
        "Khoroshevskyi/gse_yaml:default",
        "Khoroshevskyi/gse_yaml:default",
        "Khoroshevskyi/dupa:f1",
    ],
    admin="Khoroshevskyi",
)
dd_list_private = con.annotation.get_by_rp(
    [
        "Khoroshevskyi/gse_yaml:default",
        "Khoroshevskyi/gse_yaml:default",
        "Khoroshevskyi/dupa:f1",
    ]
)

dd_search = con.annotation.get(namespace="Khoroshevskyi")
dd_search_pr = con.annotation.get(namespace="Khoroshevskyi", admin="Khoroshevskyi")
dd_search_pr_namespace = con.annotation.get(
    query="s", admin=["Khoroshevskyi", "test_11"]
)

dd_all = con.annotation.get(admin=["Khoroshevskyi", "test_11"])


print(dd_list)
print(dd_list_private)

print(dd_search)
print(dd_search_pr)
print(dd_search_pr_namespace)

print(dd_all)

################
# Namespace

ff = con.namespace.get("Khoroshevskyi", admin="Khoroshevskyi")

print(ff)


ff = con.project.get_by_rp("Khoroshevskyi/gse_yaml:default")

print(ff)

dell = con.project.delete(namespace="Khoroshevskyi", name="dupa", tag="test1")

con.project.update(update_dict={"is_private": False}, namespace="Khoroshevskyi", name="dupa", tag="test1")
