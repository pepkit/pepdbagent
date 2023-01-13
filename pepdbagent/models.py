# file with pydantic models
import datetime
from typing import List, Optional

from pydantic import BaseModel, Field, validator, Extra
import peppy


class AnnotationModel(BaseModel):
    """
    Project Annotations model. All meta metadata
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

    class Config:
        allow_population_by_field_name = True
        validate_assignment = True

    @validator("is_private")
    def is_private_should_be_bool(cls, v):
        if not isinstance(v, bool):
            return False
        else:
            return v


class AnnotationReturnModel(BaseModel):
    """
    Annotation return model.
    """
    count: int
    limit: int
    offset: int
    result: List[AnnotationModel]


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


class UpdateItems(BaseModel):
    """
    Model used for updating individual items in db
    """

    project_value: Optional[peppy.Project] = Field(alias="project")
    tag: Optional[str]
    is_private: Optional[bool]
    name: Optional[str]

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


class UploadResponse(BaseModel):
    """
    Response model in upload or update methods
    """

    registry_path: str
    log_stage: str
    status: str
    info: str
