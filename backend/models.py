from pydantic import BaseModel
from datetime import date
from typing import Optional

class SalesRecord(BaseModel):
    employee_id: str
    branch: Optional[str]
    role: Optional[str]
    vehicle_model: Optional[str]
    vehicle_type: str
    quantity: int
    sale_date: date


class SalesUploadResponse(BaseModel):
    status: str
    file_name: str
    uploaded_by: str
    total_rows: int
    valid_rows: int
    invalid_rows: int
    message: str

class RuleUploadResponse(BaseModel):
    status: str                    # SUCCESS / FAILED
    file_name: str
    uploaded_by: str

    total_rows: int
    valid_rows: int
    invalid_rows: int

    upload_id: Optional[int] = None

    message: str
