from fastapi import APIRouter, UploadFile, File, Form,HTTPException
from datetime import datetime
import csv
import io
import os
import json
from dotenv import load_dotenv
from models import SalesUploadResponse,RuleUploadResponse
from database import db,conn

load_dotenv()
data_ingestion_router = APIRouter()

##########################DIRECTORY TO SAVE UPLOADED FILES ##########################
UPLOAD_DIRECTORY = "uploads"
os.makedirs(UPLOAD_DIRECTORY, exist_ok=True)
############################ API ROUTES FOR DATA INGESTION #########################
def parse_date(date_str):
    if not date_str:
        return None

    date_str = str(date_str).strip()  # 🔥 IMPORTANT

    formats = [
        "%d-%m-%Y",
        "%d/%m/%Y",
        "%Y-%m-%d",
    ]

    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt).date()
        except ValueError:
            continue

    return None

def structured_parse_date(date_str):
    if not date_str:
        return None

    # Remove spaces, BOM, hidden chars
    date_str = str(date_str).strip().replace("\ufeff", "")

    formats = [
        "%d-%m-%Y",  # 01-09-2025
        "%d/%m/%Y",  # 01/09/2025
        "%Y-%m-%d",  # 2025-09-01
    ]

    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt).date()
        except ValueError:
            continue

    return None


@data_ingestion_router.post("/upload_sales_data",response_model=SalesUploadResponse)
async def upload_sales_data(
    file: UploadFile = File(...),
    uploaded_by: str = Form(...)
):
    content = await file.read()

    # Save raw CSV
    file_path = os.path.join(UPLOAD_DIRECTORY, file.filename)
    with open(file_path, "wb") as f:
        f.write(content)

    reader = csv.DictReader(io.StringIO(content.decode("utf-8")))

    total_rows = 0
    valid_rows = 0
    invalid_rows = 0

    for row_number, row in enumerate(reader, start=1):
        total_rows += 1

        employee_id = row.get("Employee_ID")
        vehicle_type = row.get("Vehicle_Type")
        quantity = row.get("Quantity")
        sale_date = row.get("Sale_Date", "").strip()

        # ---------- Validation ----------
        if not employee_id or not vehicle_type or not quantity:
            invalid_rows += 1
            db.execute(
                "INSERT INTO sales_upload_errors (csv_row_number, error_message, raw_data) VALUES (%s,%s,%s)",
                (row_number, "Missing required fields", json.dumps(row))
            )
            continue

        try:
            quantity = int(quantity)
            if quantity <= 0:
                raise ValueError
        except:
            invalid_rows += 1
            db.execute(
                "INSERT INTO sales_upload_errors (csv_row_number , error_message, raw_data) VALUES (%s,%s,%s)",
                (row_number, "Invalid quantity", json.dumps(row))
            )
            continue

        parsed_date = parse_date(sale_date)
        if not parsed_date:
            invalid_rows += 1
            db.execute(
                "INSERT INTO sales_upload_errors (csv_row_number, error_message, raw_data) VALUES (%s,%s,%s)",
                (row_number, "Invalid date format (DD-MM-YYYY)", json.dumps(row))
            )
            continue

        # ---------- Insert Valid Record ----------
        db.execute("""
            INSERT INTO sales_transactions
            (employee_id, branch, role, vehicle_model, vehicle_type, quantity, sale_date)
            VALUES (%s,%s,%s,%s,%s,%s,%s)
        """, (
            employee_id,
            row.get("Branch"),
            row.get("Role"),
            row.get("Vehicle_Model"),
            vehicle_type,
            quantity,
            parsed_date
        ))

        valid_rows += 1

    # ---------- Upload Log ----------
    db.execute("""
        INSERT INTO sales_upload_logs
        (file_name, uploaded_by, total_rows, valid_rows, invalid_rows)
        VALUES (%s,%s,%s,%s,%s)
    """, (
        file.filename,
        uploaded_by,
        total_rows,
        valid_rows,
        invalid_rows
    ))

    conn.commit()

    return SalesUploadResponse(
        status="SUCCESS",
        file_name=file.filename,
        uploaded_by=uploaded_by,
        total_rows=total_rows,
        valid_rows=valid_rows,
        invalid_rows=invalid_rows,
        message="Sales data ingested successfully"
    )

@data_ingestion_router.post("/upload_structured_rule")
async def upload_structured_rule(
    file: UploadFile = File(...),
    uploaded_by: str = Form(...)
):
    if not file.filename.endswith(".csv"):
        raise HTTPException(status_code=400, detail="Only CSV files allowed")

    content = await file.read()

    # ---- Save raw CSV ----
    file_path = os.path.join(UPLOAD_DIRECTORY, file.filename)
    with open(file_path, "wb") as f:
        f.write(content)

    reader = csv.DictReader(io.StringIO(content.decode("utf-8")))

    total_rows = valid_rows = invalid_rows = 0

    # ---- Create upload record ----
    db.execute(
        """
        INSERT INTO incentive_rule_uploads
        (file_name, uploaded_by, total_rows, valid_rows, invalid_rows)
        VALUES (%s,%s,0,0,0)
        """,
        (file.filename, uploaded_by)
    )
    upload_id = db.lastrowid
    conn.commit()

    for csv_row_number, raw_row in enumerate(reader, start=2):
        total_rows += 1

        try:
            # ---- INTERNAL CSV FIELD MAPPING ----
            rule_id = raw_row.get("Rule_ID", "").strip()
            role = raw_row.get("Role", "").strip()
            vehicle_type = raw_row.get("Vehicle_Type", "").strip()

            min_qty = int(raw_row.get("Min_Units", 0))
            max_qty = int(raw_row.get("Max_Units", 0))

            base_amount = float(raw_row.get("Incentive_Amount_INR", 0))
            per_unit_amount = float(raw_row.get("Bonus_Per_Unit_INR", 0))

            valid_from = parse_date(raw_row.get("Valid_From", ""))
            valid_to = parse_date(raw_row.get("Valid_To", ""))

            # ---- Auto priority: higher slab = higher priority ----
            priority = max_qty

            # ---- Validations ----
            if not rule_id or not role or not vehicle_type:
                raise ValueError("Missing required text fields")

            if min_qty <= 0 or max_qty <= 0:
                raise ValueError("Invalid quantity values")

            if min_qty > max_qty:
                raise ValueError("min_qty cannot be greater than max_qty")

            if not valid_from or not valid_to:
                raise ValueError("Invalid date format")

            if valid_from > valid_to:
                raise ValueError("Invalid date range")

            # ---- Save version snapshot ----
            db.execute(
                """
                INSERT INTO incentive_rule_versions
                (rule_id, rule_snapshot, upload_id)
                VALUES (%s,%s,%s)
                """,
                (rule_id, json.dumps(raw_row), upload_id)
            )

            # ---- Insert rule ----
            db.execute(
                """
                INSERT INTO incentive_rules
                (rule_id, role, vehicle_type,
                 min_qty, max_qty,
                 base_amount, per_unit_amount,
                 valid_from, valid_to,
                 priority, upload_id)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                (
                    rule_id, role, vehicle_type,
                    min_qty, max_qty,
                    base_amount, per_unit_amount,
                    valid_from, valid_to,
                    priority, upload_id
                )
            )

            valid_rows += 1

        except Exception as e:
            invalid_rows += 1

    # ---- Update upload summary ----
    db.execute(
        """
        UPDATE incentive_rule_uploads
        SET total_rows=%s,
            valid_rows=%s,
            invalid_rows=%s
        WHERE id=%s
        """,
        (total_rows, valid_rows, invalid_rows, upload_id)
    )

    conn.commit()

    return {
        "status": "SUCCESS",
        "file_name": file.filename,
        "uploaded_by": uploaded_by,
        "total_rows": total_rows,
        "valid_rows": valid_rows,
        "invalid_rows": invalid_rows,
        "message": "Structured incentive rules uploaded successfully"
    }