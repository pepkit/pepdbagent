# file with pydantic models
import datetime
from typing import List, Optional, Union

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

    name: Optional[str]
    description: Optional[str]
    tag: Optional[str]
    is_private: Optional[bool]
    pep_schema: Optional[str]
    digest: Optional[str]
    config: Optional[dict]
    samples: Optional[List[dict]]
    subsamples: Optional[List[List[dict]]]
    description: Optional[str]

    class Config:
        arbitrary_types_allowed = True
        extra = Extra.forbid

    @property
    def number_of_samples(self) -> Union[int, None]:
        if self.samples:
            return len(self.samples)
        return None


class UpdateModel(BaseModel):
    """
    !! is Used only by pepdbagent. Don't use it outside
    Model used for updating individual items and creating sql string in the code
    """

    config: Optional[dict]
    name: Optional[str] = None
    tag: Optional[str] = None
    private: Optional[bool] = Field(alias="is_private")
    digest: Optional[str]
    number_of_samples: Optional[int]
    pep_schema: Optional[str]
    description: Optional[str] = ""
    # last_update_date: Optional[datetime.datetime] = datetime.datetime.now(datetime.timezone.utc)

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

    @validator("tag", "name")
    def value_should_not_contain_question(cls, v):
        if "?" in v:
            return ValueError("Question mark (?) is prohibited in name and tag.")
        return v

    class Config:
        extra = Extra.forbid
        allow_population_by_field_name = True
