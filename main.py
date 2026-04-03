# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.18.1
#   kernelspec:
#     display_name: Python 3 (ipykernel) (Local)
#     language: python
#     name: conda-base-py
# ---

# %%
# main.py
from typing import List, Optional

from fastapi import FastAPI
from pydantic import BaseModel

import planner_core as pc  # this is your file from Step 1

app = FastAPI()


class DegreePlannerRequest(BaseModel):
    """
    This defines the JSON body we expect from callers (later Vertex AI).
    """
    program_code: str              # "MSLOD" or "HOL-EMBA"
    start_term_code: str           # e.g. "SP26"
    certs: Optional[List[str]] = None   # e.g. ["OC", "TL"]
    half_time: bool = False
    max_terms: int = 20
    target_credits: Optional[int] = None
    return_rows: bool = True       # if True, return flattened rows; else full plan


@app.get("/")
def health_check():
    """
    Simple health endpoint to see if the service is up.
    """
    return {"status": "ok"}


@app.post("/plan")
def generate_plan(body: DegreePlannerRequest):
    """
    Main endpoint: builds a plan using your planner_core logic.
    """

    # 1) Decide target credits if not provided
    if body.target_credits is None:
        if body.program_code == "MSLOD":
            target = 36
        elif body.program_code == "HOL-EMBA":
            target = 53
        else:
            target = 36
    else:
        target = body.target_credits

    certs = body.certs or []

    # 2) Run your planner
    plan = pc.run_planner(
        program_code=body.program_code,
        start_term_code=body.start_term_code,
        certs=certs,
        half_time=body.half_time,
        max_terms=body.max_terms,
        target_credits=target,
    )

    # 3) Add tuition info
    plan = pc.enrich_plan_with_tuition(plan)

    # 4) Either return the "rows" (nice for UI) or full raw plan
    if body.return_rows:
        rows = pc.plan_to_table_rows(plan)
        return {
            "program_code": plan["program_code"],
            "certificates": plan["certificates"],
            "start_term_code": plan["start_term_code"],
            "half_time": plan["half_time"],
            "total_credits": plan["total_credits"],
            "total_tuition": plan["total_tuition"],
            "tuition_per_credit": plan["tuition_per_credit"],
            "rows": rows,
        }
    else:
        return plan
