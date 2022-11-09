from pydantic import BaseModel, Field, validator
from typing import Optional


class Annotation(BaseModel):
    status: Optional[str]
    number_of_samples: Optional[int] = Field(alias="n_samples")
    is_private: Optional[bool]
    description: Optional[str]
    last_update: Optional[str]

    class Config:
        allow_population_by_field_name = True
        validate_assignment = True

    @validator("is_private", pre=True, always=True)
    def set_name(cls, is_private):
        if is_private is None:
            return False
        return is_private
