# file for manual local tests

import sqlmodel
import pepdbagent
from pydantic import BaseModel
from typing import Optional


# class Annot(BaseModel):
#     name: str
#     surname: str
#
# class Model(BaseModel):
#     person: str
#     guest: Optional[str]
#     annot: Annot
#
#
#
# ff = Model(**fff)
# print(ff.dict(exclude_none=True))
con = pepdbagent.Connection(user='postgres',
                      password='docker',
                      )

con.get_project(namespace="new", name="GSE220436", tag='raw')

dd = con.search.project(namespace="kk", search_str='de', admin=False)

print(dd)


