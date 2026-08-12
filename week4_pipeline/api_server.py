"""
Week 4 - Mock REST API service (FastAPI)
=========================================
Serves the fashion product catalog over HTTP so the batch pipeline in this
same week performs a *real* network extraction (localhost), exactly as it
would against any production REST API - pagination, query params, and JSON
responses included.

Run standalone:  uvicorn week4_pipeline.api_server:app --port 8000
"""
from pathlib import Path
from typing import Optional

import pandas as pd
from fastapi import FastAPI, Query

BASE = Path(__file__).resolve().parent.parent
CLEAN_CSV = BASE / "week1_eda" / "outputs" / "cleaned_data.csv"

app = FastAPI(title="Fashion Catalog API", version="1.0")
_df = None


def get_df():
    global _df
    if _df is None:
        _df = pd.read_csv(CLEAN_CSV)
    return _df


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/products")
def list_products(
    limit: int = Query(100, le=5000),
    offset: int = 0,
    masterCategory: Optional[str] = None,
    gender: Optional[str] = None,
):
    df = get_df()
    if masterCategory:
        df = df[df["masterCategory"] == masterCategory]
    if gender:
        df = df[df["gender"] == gender]
    page = df.iloc[offset: offset + limit]
    return page.to_dict(orient="records")


@app.get("/products/{product_id}")
def get_product(product_id: int):
    df = get_df()
    row = df[df["id"] == product_id]
    if row.empty:
        return {"error": "not found"}
    return row.to_dict(orient="records")[0]


@app.get("/categories")
def categories():
    df = get_df()
    return sorted(df["masterCategory"].unique().tolist())
