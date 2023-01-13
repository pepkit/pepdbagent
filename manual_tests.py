# file for manual local tests

import pepdbagent
from peppy import Project
from pydantic import BaseModel


con = pepdbagent.PEPDatabaseAgent(
    user="postgres",
    password="docker",
)

dd1 = con.annotation._get_single_annotation("Khoroshevskyi", "dupa", "f1")
dd2 = con.annotation._get_single_annotation("Khoroshevskyi", "gse_yaml", "default")

dd_list = con.annotation.get_by_rp(["Khoroshevskyi/gse_yaml:default", "Khoroshevskyi/gse_yaml:default", "Khoroshevskyi/dupa:f1"], admin="Khoroshevskyi")
dd_list_private = con.annotation.get_by_rp(["Khoroshevskyi/gse_yaml:default", "Khoroshevskyi/gse_yaml:default", "Khoroshevskyi/dupa:f1"])

dd_search = con.annotation.get(namespace="Khoroshevskyi")
dd_search_pr = con.annotation.get(namespace="Khoroshevskyi", admin="Khoroshevskyi")
dd_search_pr_namespace = con.annotation.get(query="s", admin=["Khoroshevskyi","test_11"])

print(dd1)
print(dd2)

print(dd_list)
print(dd_list_private)

print(dd_search)
print(dd_search_pr)
print(dd_search_pr_namespace)


ff = con.namespace.get("Khoroshevskyi", admin="Khoroshevskyi")
