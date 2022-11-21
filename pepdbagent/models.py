from typing import List, Optional

from pydantic import BaseModel, Field


class Model(BaseModel):
    class Config:
        allow_population_by_field_name = True


class ProjectModel(Model):
    description: Optional[str]
    digest: str
    id: int
    number_of_samples: int = Field(alias="n_samples")
    name: str
    tag: str
    is_private: bool


class NamespaceModel(Model):
    number_of_projects: int = Field(alias="n_projects")
    number_of_samples: int = Field(alias="n_samples")
    namespace: str
    projects: List[ProjectModel]


class NamespacesResponseModel(Model):
    namespaces: Optional[List[NamespaceModel]]


class UploadResponse(Model):
    registry_path: str
    log_stage: str
    status: str
    info: str
