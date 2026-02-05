import shutil
from pydantic import ValidationError
from fastapi import APIRouter, UploadFile, File, Form,HTTPException
import os
import pandas as pd
from dotenv import load_dotenv
from models import SalesRowSchema,RuleRowSchema
from database import db,conn

load_dotenv()
data_ingestion_router = APIRouter()

##########################DIRECTORY TO SAVE UPLOADED FILES ##########################
UPLOAD_DIRECTORY = "uploads"
os.makedirs(UPLOAD_DIRECTORY, exist_ok=True)
############################ API ROUTES FOR DATA INGESTION #########################
@data_ingestion_router.post("/upload_sales_data")
async def upload_sales_data(file: UploadFile = File(...)):
    try:
        file_path = os.path.join(UPLOAD_DIRECTORY, file.filename)
        with open(file_path, "wb") as f:
            shutil.copyfileobj(file.file, f)

        df = pd.read_csv(file_path)

        REQUIRED_SALES_COLUMNS = ["Employee_ID", "Branch", "Role", "Vehicle_Model", "Quantity", "Sale_Date", "Vehicle_Type"]

        missing_cols = [col for col in REQUIRED_SALES_COLUMNS if col not in df.columns]
        if missing_cols:
            raise HTTPException(
                status_code=400,
                detail=f"Missing columns: {missing_cols}"
            )

        success_count = 0
        failed_rows = []

        for index, row in df.iterrows():
            try:
                # 🔹 Convert CSV row to dict
                row_dict = row.to_dict()

                # 🔹 Pydantic validation
                sales_data = SalesRowSchema(**row_dict)

                # Insert salesperson
                db.execute(
                    "SELECT id FROM salespeople WHERE id = %s",
                    (sales_data.Employee_ID,)
                )
                if not db.fetchone():
                    db.execute(
                        """
                        INSERT INTO salespeople (id, branch, role)
                        VALUES (%s, %s, %s)
                        """,
                        (
                            sales_data.Employee_ID,
                            sales_data.Branch,
                            sales_data.Role
                        )
                    )

                # Insert sales record
                db.execute(
                    """
                    INSERT INTO sales_records
                    (employee_id, vehicle_model, quantity, sale_date, vehicle_type)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (
                        sales_data.Employee_ID,
                        sales_data.Vehicle_Model,
                        sales_data.Quantity,
                        sales_data.Sale_Date,
                        sales_data.Vehicle_Type
                    )
                )

                success_count += 1

            except ValidationError as ve:
                failed_rows.append({
                    "row": index + 1,
                    "error": ve.errors()
                })
                continue

        conn.commit()

        return {
            "status": "success",
            "processed": success_count,
            "failed": len(failed_rows),
            "failed_rows": failed_rows
        }

    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@data_ingestion_router.post("/upload_structured_rule")
async def upload_structured_rule(file: UploadFile = File(...)):
    try:
        # -----------------------------
        # 1. Save uploaded file
        # -----------------------------
        file_path = os.path.join(UPLOAD_DIRECTORY, file.filename)
        with open(file_path, "wb") as f:
            shutil.copyfileobj(file.file, f)

        # -----------------------------
        # 2. Read CSV
        # -----------------------------
        df = pd.read_csv(file_path)

        # -----------------------------
        # 3. Validate required columns
        # -----------------------------
        REQUIRED_RULE_COLUMNS = [
            "Rule_ID",
            "Role",
            "Vehicle_Type",
            "Min_Units",
            "Max_Units",
            "Incentive_Amount_INR",
            "Bonus_Per_Unit_INR",
            "Valid_From",
            "Valid_To"
        ]

        missing_cols = [col for col in REQUIRED_RULE_COLUMNS if col not in df.columns]
        if missing_cols:
            raise HTTPException(
                status_code=400,
                detail=f"Missing columns: {missing_cols}"
            )

        success_count = 0
        failed_rows = []

        # -----------------------------
        # 4. Process rows
        # -----------------------------
        for index, row in df.iterrows():
            try:
                row_dict = row.to_dict()

                # Pydantic validation
                rule_data = RuleRowSchema(**row_dict)

                # Check if rule already exists
                db.execute(
                    "SELECT id FROM incentive_rules WHERE id = %s",
                    (rule_data.Rule_ID,)
                )
                if db.fetchone():
                    continue  # Skip existing rule

                # Insert rule
                db.execute(
                    """
                    INSERT INTO incentive_rules
                    (
                        id,
                        role,
                        vehicle_type,
                        min_units,
                        max_units,
                        incentive_amount,
                        bonus_per_unit,
                        valid_from,
                        valid_to,
                        rule_type
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        rule_data.Rule_ID,
                        rule_data.Role,
                        rule_data.Vehicle_Type,
                        rule_data.Min_Units,
                        rule_data.Max_Units,
                        rule_data.Incentive_Amount_INR,
                        rule_data.Bonus_Per_Unit_INR,
                        rule_data.Valid_From,
                        rule_data.Valid_To,
                        "Structured"
                    )
                )

                success_count += 1

            except ValidationError as ve:
                failed_rows.append({
                    "row": index + 1,
                    "error": ve.errors()
                })
                continue

            except Exception as e:
                failed_rows.append({
                    "row": index + 1,
                    "error": str(e)
                })
                continue

        # -----------------------------
        # 5. Commit DB
        # -----------------------------
        conn.commit()

        return {
            "status": "success",
            "processed": success_count,
            "failed": len(failed_rows),
            "failed_rows": failed_rows
        }

    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))