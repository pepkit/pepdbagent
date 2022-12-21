# file for manual local tests

import pepdbagent
from peppy import Project


con = pepdbagent.Connection(user='postgres',
                            password='docker',
                            )

# proj = Project('/home/bnt4me/virginia/repos/pepdbagent/sample_pep/subtable1/project_config.yaml')

# con.updload_project(namespace="test", name='sub', tag='f', project=proj, is_private=True)


ff = con.update_item(namespace="test", name="sub", tag='f', update_dict={'private': True,
                                                                                'annot': {'description': "this is my description",
                                                                                          'something': 'else'},
                                                                                'digest': "dfdfdf",

                                                                                })

# res = con.search.project(namespace='kk',search_str='d', admin=True)
res = con.search.namespace('i', admin_nsp=('kk',))
print(res)
