# file with pydantic models
import datetime

from peprs.const import CONFIG_KEY, SAMPLE_RAW_DICT_KEY, SUBSAMPLE_RAW_DICT_KEY
from pydantic import BaseModel, ConfigDict, Field, field_validator

from pepdbagent.const import DEFAULT_TAG


class ProjectDict(BaseModel):
    """
    Project dict (raw) model
    """

    config: dict = Field(alias=CONFIG_KEY)
    subsample_list: list | None = Field(alias=SUBSAMPLE_RAW_DICT_KEY)
    sample_dict: list = Field(alias=SAMPLE_RAW_DICT_KEY)

    model_config = ConfigDict(populate_by_name=True, extra="forbid")


class AnnotationModel(BaseModel):
    """
    Project Annotation model. All meta metadata
    """

    namespace: str | None = None
    name: str | None = None
    tag: str | None = None
    is_private: bool | None = None
    number_of_samples: int | None = None
    description: str | None = None
    last_update_date: str | None = None
    submission_date: str | None = None
    digest: str | None = None
    pep_schema: str | None = None
    pop: bool | None = False
    stars_number: int | None = 0
    forked_from: str | None = None

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
    results: list[AnnotationModel | None]


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
    results: list[Namespace]


class UpdateItems(BaseModel):
    """
    Model used for updating individual items in db
    """

    name: str | None = None
    description: str | None = None
    tag: str | None = None
    is_private: bool | None = None
    pep_schema: str | None = None
    digest: str | None = None
    config: dict | None = None
    samples: list[dict] | None = None
    subsamples: list[list[dict]] | None = None
    pop: bool | None = None
    schema_id: int | None = None

    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        extra="forbid",
    )

    @property
    def number_of_samples(self) -> int | None:
        if self.samples:
            return len(self.samples)
        return None


class UpdateModel(BaseModel):
    """
    !! is Used only by pepdbagent. Don't use it outside
    Model used for updating individual items and creating sql string in the code
    """

    config: dict | None = None
    name: str | None = None
    tag: str | None = None
    private: bool | None = Field(alias="is_private", default=None)
    digest: str | None = None
    number_of_samples: int | None = None
    pep_schema: str | None = None
    description: str | None = ""
    # last_update_date: datetime.datetime | None = datetime.datetime.now(datetime.timezone.utc)
    pop: bool | None = False

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
    contact_url: str | None = None
    number_of_projects: int
    number_of_schemas: int


class ListOfNamespaceInfo(BaseModel):
    """
    Namespace information response model
    """

    pagination: PaginationResult
    results: list[NamespaceInfo]


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
    description: str | None = None
    number_of_samples: int = 0


class ProjectViews(BaseModel):
    """
    View annotation model
    """

    namespace: str
    name: str
    tag: str = DEFAULT_TAG
    views: list[ViewAnnotation] = []


class CreateViewDictModel(BaseModel):
    """
    View creation dict model
    """

    project_namespace: str
    project_name: str
    project_tag: str
    sample_list: list[str]


class RegistryPath(BaseModel):
    namespace: str
    name: str
    tag: str | None = "default"


class NamespaceStats(BaseModel):
    """
    Namespace stats model
    """

    namespace: str | None = None
    projects_updated: dict[str, int] | None = None
    projects_created: dict[str, int] | None = None


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
    history: list[HistoryChangeModel]


class SchemaVersionAnnotation(BaseModel):
    """
    Schema version annotation model
    """

    namespace: str
    schema_name: str
    version: str
    contributors: str | None = ""
    release_notes: str | None = ""
    tags: dict[str, str | None] = {}
    release_date: datetime.datetime
    last_update_date: datetime.datetime


class SchemaRecordAnnotation(BaseModel):
    """
    Schema annotation model
    """

    namespace: str
    schema_name: str
    description: str | None = ""
    maintainers: str | None = ""
    lifecycle_stage: str | None = ""
    latest_released_version: str | None = None
    private: bool | None = False
    last_update_date: datetime.datetime


class SchemaSearchResult(BaseModel):
    """
    Schema search result model
    """

    pagination: PaginationResult
    results: list[SchemaRecordAnnotation]


class SchemaVersionSearchResult(BaseModel):
    """
    Schema version search result model
    """

    pagination: PaginationResult
    results: list[SchemaVersionAnnotation]


class UpdateSchemaRecordFields(BaseModel):
    maintainers: str | None = None
    lifecycle_stage: str | None = None
    private: bool | None = False
    name: str | None = None
    description: str | None = None


class UpdateSchemaVersionFields(BaseModel):
    contributors: str | None = None
    schema_value: dict | None = None
    release_notes: str | None = None


class TarNamespaceModel(BaseModel):
    """
    Namespace archive model
    """

    identifier: int | None = None
    namespace: str
    file_path: str
    creation_date: datetime.datetime | None = None
    number_of_projects: int = 0
    file_size: int = 0


class TarNamespaceModelReturn(BaseModel):
    """
    Namespace archive search model
    """

    count: int
    results: list[TarNamespaceModel]
