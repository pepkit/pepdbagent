# file with pydantic models
import datetime
from typing import List, Optional

import peppy
from pydantic import BaseModel, Extra, Field, validator


class AnnotationModel(BaseModel):
    """
    Project Annotation model. All meta metadata
    """

    namespace: Optional[str]
    name: Optional[str]
    tag: Optional[str]
    is_private: Optional[bool]
    number_of_samples: Optional[int]
    description: Optional[str]
    last_update_date: Optional[str]
    submission_date: Optional[str]
    digest: Optional[str]
    pep_schema: Optional[str]

    class Config:
        allow_population_by_field_name = True
        validate_assignment = True

    @validator("is_private")
    def is_private_should_be_bool(cls, v):
        if not isinstance(v, bool):
            return False
        else:
            return v


class AnnotationList(BaseModel):
    """
    Annotation return model.
    """

    count: int
    limit: int
    offset: int
    results: List[AnnotationModel]


class Namespace(BaseModel):
    """
    Model of single namespace search result
    """

    namespace: str
    number_of_projects: int
    number_of_samples: int


class NamespaceList(BaseModel):
    """
    Model of combined namespace search results
    """

    count: int
    limit: int
    offset: int
    results: List[Namespace]


class UpdateItems(BaseModel):
    """
    Model used for updating individual items in db
    """

    project_value: Optional[peppy.Project] = Field(alias="project")
    tag: Optional[str]
    is_private: Optional[bool]
    name: Optional[str]
    pep_schema: Optional[str]

    class Config:
        arbitrary_types_allowed = True

    #     extra = Extra.forbid


class UpdateModel(BaseModel):
    """
    !! is Used only by pepdbagent. Don't use it outside
    Model used for updating individual items and creating sql string in the code
    """

    project_value: Optional[dict]
    name: Optional[str] = None
    tag: Optional[str] = None
    private: Optional[bool] = Field(alias="is_private")
    digest: Optional[str]
    last_update_date: Optional[datetime.datetime]
    number_of_samples: Optional[int]
    pep_schema: Optional[str]

    @validator("tag", "name")
    def value_must_not_be_empty(cls, v):
        if "" == v:
            return None
        return v

    @validator("tag", "name")
    def value_must_be_lowercase(cls, v):
        if v:
            return v.lower()
        return v

    class Config:
        extra = Extra.forbid
