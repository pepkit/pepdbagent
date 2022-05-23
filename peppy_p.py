import peppy
from peppy import Project
import json
import pandas as pd

from attmap import PathExAttMap


proj = Project("/home/bnt4me/Virginia/pephub_db/sample_pep/amendments1/project_config.yaml")
samples = p.sample_table

proj_dict = dict(proj)


def convert_to_dict(project_value):
    """
    Transformation of the project to dict format
    """
    if isinstance(project_value, list):
        new_list = []
        for item_value in project_value:
            new_list.append(convert_to_dict(item_value))
        return new_list

    elif isinstance(project_value, dict):
        new_dict = {}
        for key, value in project_value.items():
            if key != "_project":
                new_dict[key] = convert_to_dict(value)
                print(key)
        return new_dict

    elif isinstance(project_value, PathExAttMap):
        new_dict = PathExAttMap.to_dict(project_value)
        return convert_to_dict(new_dict)
        # return new_dict

    elif isinstance(project_value, peppy.Sample):
        new_dict = PathExAttMap.to_dict(project_value)
        #new_dict = dict(project_value)
        print(new_dict)
        return new_dict

    elif isinstance(project_value, pd.DataFrame):
        return project_value.to_dict()

    else:
        return project_value


project_dict = convert_to_dict(proj_dict)
