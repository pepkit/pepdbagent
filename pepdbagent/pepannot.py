from pydantic import BaseModel, Field
from typing import Optional


class Annotation(BaseModel):
    is_private: Optional[bool] = False
    number_of_samples: Optional[int] = Field(alias="n_samples")
    description: Optional[str]
    last_update: Optional[str]

    class Config:
        allow_population_by_field_name = True
        validate_assignment = True
