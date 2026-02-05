from fastapi import APIRouter, Depends, HTTPException
from typing import List
from database import db, conn
from models import CalculationResultSchema,CalculationStatsSchema
from dotenv import load_dotenv

load_dotenv()

results_router = APIRouter()

############################ API ROUTES FOR RESULTS #########################
@results_router.get("/GETresults", response_model=List[CalculationResultSchema])
def GETresults(skip: int = 0, limit: int = 100):
    try:
        query = """
       SELECT employee_id, period_month, total_incentive,
                   breakdown_json, status
        FROM calculation_results
        LIMIT %s OFFSET %s
        """
        db.execute(query, (limit, skip))
        results = db.fetchall()
        return results

    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
    
@results_router.get("/GETsalespeople", response_model=List[str])
def GETsalespeople():
    try:
        query = "SELECT id FROM salespeople"
        db.execute(query)
        salespeople = [row["id"] for row in db.fetchall()]
        return salespeople

    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
    
@results_router.get("/GETdashboard_stats", response_model=CalculationStatsSchema)
def GETdashboard_stats():
    try:
        # Total Incentive
        db.execute("""
            SELECT employee_id, total_incentive
            FROM calculation_results
        """)
        results = db.fetchall()

        total_salespeople = len(results)
        total_incentive = sum(r["total_incentive"] for r in results)
        avg_incentive = (
            total_incentive / total_salespeople
            if total_salespeople > 0 else 0
        )

        top_performer = None
        if results:
            top = max(results, key=lambda x: x["total_incentive"])
            top_performer = top["employee_id"]

        return {
            "total_incentive": total_incentive,
            "total_salespeople": total_salespeople,
            "avg_incentive": avg_incentive,
            "top_performer": top_performer
        }

    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))