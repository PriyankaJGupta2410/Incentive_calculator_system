from fastapi import APIRouter, UploadFile, File, Form,HTTPException
import os
import pandas as pd
from dotenv import load_dotenv
from database import db,conn
from datetime import date, datetime
from models import IncentiveCalculationRequest
import json
import calendar


load_dotenv()
calculator_router = APIRouter()

############################ API ROUTES FOR CALCULATOR #########################
@calculator_router.post("/calculate-incentives")
def calculate_incentives_api(payload: IncentiveCalculationRequest):
    try:
        # ------------------------------------
        # 1. Parse period
        # ------------------------------------
        dt = datetime.strptime(payload.period, "%Y-%m")
        start_date = dt.date().replace(day=1)
        last_day = calendar.monthrange(start_date.year, start_date.month)[1]
        end_date = start_date.replace(day=last_day)

        # ------------------------------------
        # 2. Fetch sales data
        # ------------------------------------
        db.execute(
            """
            SELECT employee_id, vehicle_type, quantity, sale_date
            FROM sales_records
            WHERE sale_date BETWEEN %s AND %s
            """,
            (start_date, end_date)
        )
        sales = db.fetchall()

        if not sales:
            return {
                "status": "success",
                "message": "No sales found for given period"
            }

        # ------------------------------------
        # 3. Group sales by employee
        # ------------------------------------
        sales_by_employee = {}
        for s in sales:
            sales_by_employee.setdefault(s["employee_id"], []).append(s)

        # ------------------------------------
        # 4. Load salespeople master
        # ------------------------------------
        db.execute("SELECT id, branch, role FROM salespeople")
        emp_map = {e["id"]: e for e in db.fetchall()}

        # ------------------------------------
        # 5. Branch totals & rankings
        # ------------------------------------
        branch_totals = {}
        branch_rankings = {}

        for emp_id, emp_sales in sales_by_employee.items():
            emp = emp_map.get(emp_id)
            if not emp:
                continue

            total_units = sum(s["quantity"] for s in emp_sales)
            branch = emp["branch"]

            branch_totals[branch] = branch_totals.get(branch, 0) + total_units
            branch_rankings.setdefault(branch, []).append((emp_id, total_units))

        for branch in branch_rankings:
            branch_rankings[branch].sort(key=lambda x: x[1], reverse=True)

        # ------------------------------------
        # 6. Top 10% performers (global)
        # ------------------------------------
        all_perf = []
        for emp_id, emp_sales in sales_by_employee.items():
            total_units = sum(s["quantity"] for s in emp_sales)
            all_perf.append((emp_id, total_units))

        all_perf.sort(key=lambda x: x[1], reverse=True)
        top_n = max(1, int(len(all_perf) * 0.1))
        top_10_ids = {e[0] for e in all_perf[:top_n]}

        # ------------------------------------
        # 7. Load structured incentive rules
        # ------------------------------------
        db.execute(
            """
            SELECT *
            FROM incentive_rules
            WHERE rule_type='Structured'
            AND valid_from <= %s
            AND valid_to >= %s
            """,
            (end_date, start_date)
        )
        rules = db.fetchall()

        processed = 0

        # ------------------------------------
        # 8. Per employee calculation
        # ------------------------------------
        for emp_id, emp_sales in sales_by_employee.items():
            emp = emp_map.get(emp_id)
            if not emp:
                continue

            role = emp["role"]
            branch = emp["branch"]

            sales_counts = {}
            sales_days = set()
            product_mix = set()

            for s in emp_sales:
                sales_counts[s["vehicle_type"]] = (
                    sales_counts.get(s["vehicle_type"], 0) + s["quantity"]
                )
                sales_days.add(s["sale_date"])
                product_mix.add(s["vehicle_type"])

            structured_total = 0
            applied_rules = []

            # ---------- Structured slab calculation ----------
            for vehicle_type, count in sales_counts.items():
                slabs = [
                    r for r in rules
                    if r["role"] == role
                    and r["vehicle_type"] == vehicle_type
                    and r["min_units"] <= count
                    and (r["max_units"] is None or count <= r["max_units"])
                ]

                slabs.sort(key=lambda x: x["min_units"], reverse=True)

                if slabs:
                    r = slabs[0]
                    amount = (
                        r["incentive_amount"]
                        + (count - r["min_units"]) * r["bonus_per_unit"]
                    )
                    structured_total += amount
                    applied_rules.append({
                        "rule_id": r["id"],
                        "type": "Structured Slab",
                        "vehicle_type": vehicle_type,
                        "amount": round(amount, 2)
                    })

            total_incentive = structured_total

            # ---------- Branch milestone slabs ----------
            branch_units = branch_totals.get(branch, 0)

            if branch_units >= 400:
                total_incentive += 10000
                applied_rules.append({"type": "Branch Milestone", "amount": 10000})
            elif branch_units >= 300:
                total_incentive += 6000
                applied_rules.append({"type": "Branch Milestone", "amount": 6000})
            elif branch_units >= 200:
                total_incentive += 3000
                applied_rules.append({"type": "Branch Milestone", "amount": 3000})

            # ---------- Consistency bonus ----------
            if len(sales_days) >= 20:
                total_incentive += 4000
                applied_rules.append({"type": "Consistency Bonus", "amount": 4000})

            # ---------- Cross-sell bonus ----------
            if len(product_mix) >= 3:
                total_incentive += 3000
                applied_rules.append({"type": "Cross Sell Bonus", "amount": 3000})

            # ---------- Branch rank bonus ----------
            rank_bonus = {0: 15000, 1: 10000, 2: 5000}
            for i, (eid, _) in enumerate(branch_rankings.get(branch, [])):
                if eid == emp_id and i in rank_bonus:
                    total_incentive += rank_bonus[i]
                    applied_rules.append({
                        "type": "Branch Rank Bonus",
                        "rank": i + 1,
                        "amount": rank_bonus[i]
                    })
                    break

            # ---------- Top 10% performer bonus ----------
            if emp_id in top_10_ids:
                bonus = total_incentive * 0.5
                total_incentive += bonus
                applied_rules.append({
                    "type": "Top 10 Percent Bonus",
                    "amount": round(bonus, 2)
                })

            # ---------- Save result ----------
            db.execute(
                """
                DELETE FROM calculation_results
                WHERE employee_id=%s AND period_month=%s
                """,
                (emp_id, payload.period)
            )

            db.execute(
                """
                INSERT INTO calculation_results
                (employee_id, period_month, total_incentive,
                 breakdown_json, status, calculated_at)
                VALUES (%s,%s,%s,%s,%s,%s)
                """,
                (
                    emp_id,
                    payload.period,
                    round(total_incentive, 2),
                    json.dumps(applied_rules),
                    "Success",
                    date.today()
                )
            )

            processed += 1

        conn.commit()

        return {
            "status": "success",
            "period": payload.period,
            "processed_salespeople": processed
        }

    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
