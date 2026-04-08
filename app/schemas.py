from uuid import UUID

from pydantic import BaseModel, ConfigDict


class StudentBase(BaseModel):
    name: str
    status: str
    specialty_id: int


class StudentCreate(StudentBase):
    pass


class StudentUpdate(StudentBase):
    pass


class StudentResponse(StudentBase):
    id: UUID

    model_config = ConfigDict(from_attributes=True)