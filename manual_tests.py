# file for manual local tests

import pepdbagent
from peppy import Project


con = pepdbagent.Connection(
    user="postgres",
    password="docker",
)

proj = Project(
    "/home/bnt4me/virginia/repos/pepdbagent/sample_pep/subtable1/project_config.yaml"
)

con.upload_project(
    namespace="test_11",
    name="sub",
    tag="f1",
    project=proj,
    is_private=True,
    overwrite=True,
)


# gf = con.get_project(namespace="test", name='sub', tag='f')
#
#
# print(gf)
#
#
# gf_annot = con.get_project_annotation(namespace="test", name='sub', tag='f')
#
# print(gf_annot)


gf = con.get_project(namespace="test_11", name="new_name", tag="balck")


gf.name = "new_name"
gf.description = "funny jou"

ff = con.update_item(
    namespace="test_11",
    name="new_name",
    tag="balck",
    update_dict={"project": gf, "is_private": True, "tag": "balck", "name": "boo"},
)


ann = con.get_project_annotation(namespace="test_11", name="new_name", tag="balck")

print(ann.json())

# res = con.search.project(namespace='kk',search_str='d', admin=True)
res = con.search.namespace("i", admin_nsp=("kk",))
print(res)
