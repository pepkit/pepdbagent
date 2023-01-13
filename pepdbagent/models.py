# file pydantic models
import datetime
from typing import List, Optional

from pydantic import BaseModel, Field, validator, Extra
import peppy


class Model(BaseModel):
    """
    Configurations for BaseModel
    """

    class Config:
        allow_population_by_field_name = True


class Annotation(BaseModel):
    """
    Project Annotations model. All meta metadata
    """

    namespace: Optional[str]
    name: Optional[str]
    tag: Optional[str]
    is_private: Optional[bool]
    number_of_samples: Optional[int] = Field(alias="n_samples")
    description: Optional[str]
    last_update_date: Optional[str]
    submission_date: Optional[str]
    digest: Optional[str]

    class Config:
        allow_population_by_field_name = True
        validate_assignment = True

    @validator("is_private")
    def is_private_should_be_bool(cls, v):
        if not isinstance(v, bool):
            return False
        else:
            return v


class ProjectSearchResultModel(Annotation):
    pass


class ProjectModel(Annotation):
    pass


class NamespaceSearchResultModel(Model):
    """
    Model of single namespace search result
    """

    namespace: str
    number_of_projects: int
    number_of_samples: int


class NamespaceSearchModel(Model):
    """
    Model of combined namespace search results
    """

    number_of_results: int
    limit: int
    offset: int
    results: List[NamespaceSearchResultModel]


class ProjectSearchModel(Model):
    """
    Model of combined project search results
    """

    namespace: str
    number_of_results: int
    limit: int
    offset: int
    results: List[ProjectSearchResultModel]


class RawPEPModel(BaseModel):
    """
    Model of raw PEP projects
    """

    name: str
    description: str
    _config: dict
    _sample_dict: dict
    _subsample_dict: Optional[list] = None

    class Config:
        extra = Extra.forbid


class UpdateItems(BaseModel):
    """
    Model used for updating individual items in db
    """

    project_value: Optional[peppy.Project] = Field(alias="project")
    tag: Optional[str]
    is_private: Optional[bool]
    name: Optional[str]

    # do not update
    # anno_info: Optional[Annotation] = Field(alias="annot")

    # class Config:
    #     extra = Extra.forbid


class UpdateModel(BaseModel):
    """
    !! is Used only by pepdbagent. Don't use it outside
    Model used for updating individual items and creating sql string in the code
    """

    project_value: Optional[dict]
    name: Optional[str]
    tag: Optional[str]
    private: Optional[bool] = Field(alias="is_private")
    digest: Optional[str]
    last_update_date: Optional[datetime.datetime]
    number_of_samples: Optional[int]

    class Config:
        extra = Extra.forbid


class UploadResponse(Model):
    """
    Response model in upload or update methods
    """

    registry_path: str
    log_stage: str
    status: str
    info: str


class NamespaceModel(Model):
    """
    Namespace model with project annotations
    """

    number_of_projects: int = Field(alias="n_projects")
    number_of_samples: int = Field(alias="n_samples")
    namespace: str
    projects: List[ProjectModel]


class NamespacesResponseModel(Model):
    namespaces: Optional[List[NamespaceModel]]

######################################################################


class AnnotationReturnModel(BaseModel):
    count: int
    limit: int
    offset: int
    result: List[Annotation]


class NamespaceResultModel(BaseModel):
    """
    Model of single namespace search result
    """

    namespace: str
    number_of_projects: int
    number_of_samples: int


class NamespaceReturnModel(BaseModel):
    """
    Model of combined namespace search results
    """

    number_of_results: int
    limit: int
    offset: int
    results: List[NamespaceResultModel]
