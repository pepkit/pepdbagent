# file with pydantic models
import datetime
from typing import Dict, List, Optional, Union

from peppy.const import CONFIG_KEY, SAMPLE_RAW_DICT_KEY, SUBSAMPLE_RAW_LIST_KEY
from pydantic import BaseModel, ConfigDict, Field, field_validator

from pepdbagent.const import DEFAULT_TAG


class ProjectDict(BaseModel):
    """
    Project dict (raw) model
    """

    config: dict = Field(alias=CONFIG_KEY)
    subsample_list: Optional[Union[list, None]] = Field(alias=SUBSAMPLE_RAW_LIST_KEY)
    sample_dict: list = Field(alias=SAMPLE_RAW_DICT_KEY)

    model_config = ConfigDict(populate_by_name=True, extra="forbid")


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
    pop: Optional[bool] = False
    stars_number: Optional[int] = 0
    forked_from: Optional[Union[str, None]] = None

    model_config = ConfigDict(
        validate_assignment=True,
        populate_by_name=True,
    )

    @field_validator("is_private")
    def is_private_should_be_bool(cls, v):
        if not isinstance(v, bool):
            return False
        else:
            return v


class PaginationResult(BaseModel):
    page: int = 0
    page_size: int = 10
    total: int


class AnnotationList(BaseModel):
    """
    Annotation return model.
    """

    count: int
    limit: int
    offset: int
    results: List[Union[AnnotationModel, None]]


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

    name: Optional[str] = None
    description: Optional[str] = None
    tag: Optional[str] = None
    is_private: Optional[bool] = None
    pep_schema: Optional[str] = None
    digest: Optional[str] = None
    config: Optional[dict] = None
    samples: Optional[List[dict]] = None
    subsamples: Optional[List[List[dict]]] = None
    pop: Optional[bool] = None
    schema_id: Optional[int] = None

    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        extra="forbid",
    )

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

    config: Optional[dict] = None
    name: Optional[str] = None
    tag: Optional[str] = None
    private: Optional[bool] = Field(alias="is_private", default=None)
    digest: Optional[str] = None
    number_of_samples: Optional[int] = None
    pep_schema: Optional[str] = None
    description: Optional[str] = ""
    # last_update_date: Optional[datetime.datetime] = datetime.datetime.now(datetime.timezone.utc)
    pop: Optional[bool] = False

    @field_validator("tag", "name")
    def value_must_not_be_empty(cls, v):
        if "" == v:
            return None
        return v

    @field_validator("tag", "name")
    def value_must_be_lowercase(cls, v):
        if v:
            return v.lower()
        return v

    @field_validator("tag", "name")
    def value_should_not_contain_question(cls, v):
        if "?" in v:
            return ValueError("Question mark (?) is prohibited in name and tag.")
        return v

    model_config = ConfigDict(populate_by_name=True, extra="forbid")


class NamespaceInfo(BaseModel):
    """
    Model with information about namespace
    """

    namespace_name: str
    contact_url: Optional[str] = None
    number_of_projects: int
    number_of_schemas: int


class ListOfNamespaceInfo(BaseModel):
    """
    Namespace information response model
    """

    pagination: PaginationResult
    results: List[NamespaceInfo]


class ProjectRegistryPath(BaseModel):
    """
    Project Namespace
    """

    namespace: str
    name: str
    tag: str = DEFAULT_TAG


class ViewAnnotation(BaseModel):
    """
    Project views model
    """

    name: str
    description: Optional[str] = None
    number_of_samples: int = 0


class ProjectViews(BaseModel):
    """
    View annotation model
    """

    namespace: str
    name: str
    tag: str = DEFAULT_TAG
    views: List[ViewAnnotation] = []


class CreateViewDictModel(BaseModel):
    """
    View creation dict model
    """

    project_namespace: str
    project_name: str
    project_tag: str
    sample_list: List[str]


class RegistryPath(BaseModel):
    namespace: str
    name: str
    tag: Optional[str] = "default"


class NamespaceStats(BaseModel):
    """
    Namespace stats model
    """

    namespace: Union[str, None] = None
    projects_updated: Dict[str, int] = None
    projects_created: Dict[str, int] = None


class HistoryChangeModel(BaseModel):
    """
    Model for history change
    """

    change_id: int
    change_date: datetime.datetime
    user: str


class HistoryAnnotationModel(BaseModel):
    """
    History annotation model
    """

    namespace: str
    name: str
    tag: str = DEFAULT_TAG
    history: List[HistoryChangeModel]


class SchemaVersionAnnotation(BaseModel):
    """
    Schema version annotation model
    """

    namespace: str
    schema_name: str
    version: str
    contributors: Optional[str] = ""
    release_notes: Optional[str] = ""
    tags: Dict[str, Union[str, None]] = {}
    release_date: datetime.datetime
    last_update_date: datetime.datetime


class SchemaRecordAnnotation(BaseModel):
    """
    Schema annotation model
    """

    namespace: str
    schema_name: str
    description: Optional[str] = ""
    maintainers: str = ""
    lifecycle_stage: str = ""
    latest_released_version: str
    private: bool = False
    last_update_date: datetime.datetime


class SchemaSearchResult(BaseModel):
    """
    Schema search result model
    """

    pagination: PaginationResult
    results: List[SchemaRecordAnnotation]


class SchemaVersionSearchResult(BaseModel):
    """
    Schema version search result model
    """

    pagination: PaginationResult
    results: List[SchemaVersionAnnotation]


class UpdateSchemaRecordFields(BaseModel):
    maintainers: Optional[str] = None
    lifecycle_stage: Optional[str] = None
    private: Optional[bool] = False
    name: Optional[str] = None
    description: Optional[str] = None


class UpdateSchemaVersionFields(BaseModel):
    contributors: Optional[str] = None
    schema_value: Optional[dict] = None
    release_notes: Optional[str] = None


class TarNamespaceModel(BaseModel):
    """
    Namespace archive model
    """

    identifier: int = None
    namespace: str
    file_path: str
    creation_date: datetime.datetime = None
    number_of_projects: int = 0
    file_size: int = 0


class TarNamespaceModelReturn(BaseModel):
    """
    Namespace archive search model
    """

    count: int
    results: List[TarNamespaceModel]
