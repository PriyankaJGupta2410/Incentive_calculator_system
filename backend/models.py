from pydantic import BaseModel, Field
from datetime import date
from typing import Optional

class SalesRowSchema(BaseModel):
    Employee_ID: str = Field(..., min_length=1)
    Branch: str
    Role: str
    Vehicle_Model: str
    Quantity: int = Field(..., gt=0)
    Sale_Date: date
    Vehicle_Type: str

class RuleRowSchema(BaseModel):
    Rule_ID: str
    Role: str
    Vehicle_Type: str
    Min_Units: int
    Max_Units: int
    Incentive_Amount_INR: float
    Bonus_Per_Unit_INR: float
    Valid_From: date
    Valid_To: date

class IncentiveCalculationRequest(BaseModel):
    period: str = Field(example="2025-09")

class CalculationResultSchema(BaseModel):
    employee_id: str
    period_month: str
    total_incentive: float
    breakdown_json: str
    status: str

    class Config:
        from_attributes = True


class CalculationStatsSchema(BaseModel):
    total_incentive: float
    total_salespeople: int
    avg_incentive: float
    top_performer: Optional[str]