from pydantic import BaseModel


class SpecialtyReportItem(BaseModel):
    specialty_id: int
    specialty_name: str
    active_students_count: int


class StudentsReportResponse(BaseModel):
    total_students: int
    active_students: int
    expelled_students: int
    by_specialty: list[SpecialtyReportItem]


class ReportFileResponse(BaseModel):
    bucket: str
    object_name: str
    content_type: str