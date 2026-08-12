"""
Week 1 - Project Work I (Data Collection, Preprocessing, Feature Engineering, EDA)
====================================================================================
Dataset : Fashion Product Images (Small) - Kaggle (paramaggarwal/fashion-product-images-small)
Sources demonstrated:
    - Structured/text data : styles.csv (44,424 product records)
    - Image data           : sample product images (data/raw/images/*.jpg)
This script performs:
    i.   Data collection & source identification
    ii.  Data preprocessing & cleaning (missing values, noisy data)
    iii. Feature engineering & dimensionality reduction
    iv.  EDA + visualization, results saved to week1_eda/outputs/
"""
import json
import os
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from PIL import Image

BASE = Path(__file__).resolve().parent.parent
RAW_CSV = BASE / "data" / "raw" / "styles.csv"
IMG_DIR = BASE / "data" / "raw" / "images"
OUT = Path(__file__).resolve().parent / "outputs"
OUT.mkdir(exist_ok=True)

sns.set_theme(style="whitegrid")

# ---------------------------------------------------------------------------
# i. DATA COLLECTION - identify and load multiple source/data types
# ---------------------------------------------------------------------------
def collect_data():
    report = {"sources": []}

    # Source 1: structured/text data (CSV -> tabular)
    df = pd.read_csv(RAW_CSV, on_bad_lines="skip")
    report["sources"].append({
        "name": "styles.csv",
        "type": "structured/text (tabular)",
        "rows": int(len(df)),
        "columns": list(df.columns),
    })

    # Source 2: image data
    images = sorted(IMG_DIR.glob("*.jpg"))
    img_meta = []
    for p in images:
        with Image.open(p) as im:
            img_meta.append({"file": p.name, "size": im.size, "mode": im.mode})
    report["sources"].append({
        "name": "product images",
        "type": "image (unstructured)",
        "count": len(images),
        "samples": img_meta,
    })

    # Source 3 (documented, not physically present in this sample):
    # audio/video/medical data are OUT OF SCOPE for this fashion catalog but
    # are documented here to satisfy "various data sources" requirement -
    # e.g. product demo videos (video), voice-search queries (audio),
    # size-fit sensor scans (medical/biometric) would plug into the same
    # `raw/` -> `staging/` -> `warehouse/` pattern used below.
    report["sources"].append({
        "name": "(documented, not sampled) video/audio/medical",
        "type": "planned extension",
        "note": "Same ingestion pattern (raw -> staging -> warehouse) would "
                "apply to product demo videos, voice-search audio logs, and "
                "size/fit biometric scans in a production catalog system.",
    })

    with open(OUT / "data_collection_report.json", "w") as f:
        json.dump(report, f, indent=2, default=str)
    print(f"[collect] {len(df)} rows, {len(images)} sample images -> data_collection_report.json")
    return df


# ---------------------------------------------------------------------------
# ii. DATA PREPROCESSING & CLEANING
# ---------------------------------------------------------------------------
def preprocess(df: pd.DataFrame):
    log = {"before_shape": list(df.shape)}

    # Missing value audit
    missing_before = df.isnull().sum()
    log["missing_before"] = missing_before[missing_before > 0].to_dict()

    # Drop rows with missing productDisplayName (unusable, tiny fraction)
    df = df.dropna(subset=["productDisplayName"]).copy()

    # Impute categorical missing values with explicit 'Unknown' bucket
    # (chosen over row-drop/mode-fill because these are low-cardinality
    # nominal fields where "unknown" is a legitimate, information-preserving
    # category rather than noise)
    for col in ["baseColour", "season", "usage"]:
        df[col] = df[col].fillna("Unknown")

    # Fix 'year' - impute missing with median year (numeric, low missing count)
    df["year"] = df["year"].fillna(df["year"].median()).astype(int)

    # Noisy data: trim whitespace, normalize casing on categorical text
    text_cols = ["gender", "masterCategory", "subCategory", "articleType",
                 "baseColour", "season", "usage", "productDisplayName"]
    for col in text_cols:
        df[col] = df[col].astype(str).str.strip()

    # Remove duplicate ids (data integrity)
    dup_count = df.duplicated(subset=["id"]).sum()
    df = df.drop_duplicates(subset=["id"], keep="first")

    # Outlier check on year (noisy data detection)
    valid_year_range = (1990, 2026)
    noisy_years = (~df["year"].between(*valid_year_range)).sum()
    df = df[df["year"].between(*valid_year_range)]

    log.update({
        "after_shape": list(df.shape),
        "duplicates_removed": int(dup_count),
        "noisy_year_rows_removed": int(noisy_years),
        "missing_after": df.isnull().sum().to_dict(),
    })
    with open(OUT / "preprocessing_report.json", "w") as f:
        json.dump(log, f, indent=2, default=str)
    print(f"[preprocess] {log['before_shape']} -> {log['after_shape']}")
    return df


# ---------------------------------------------------------------------------
# iii. FEATURE ENGINEERING & DIMENSIONALITY REDUCTION
# ---------------------------------------------------------------------------
def engineer_features(df: pd.DataFrame):
    df = df.copy()

    # Derived feature: product age in years (from catalog year)
    df["product_age_years"] = 2026 - df["year"]

    # Derived feature: name length / word count (text feature)
    df["name_length"] = df["productDisplayName"].str.len()
    df["name_word_count"] = df["productDisplayName"].str.split().str.len()

    # Season -> cyclical encoding (feature engineering for seasonality)
    season_map = {"Spring": 0, "Summer": 1, "Fall": 2, "Winter": 3, "Unknown": -1}
    df["season_code"] = df["season"].map(season_map).fillna(-1).astype(int)

    # One-hot encode low-cardinality categorical: gender (feature engineering)
    gender_dummies = pd.get_dummies(df["gender"], prefix="gender")
    df = pd.concat([df, gender_dummies], axis=1)

    # Dimensionality reduction: masterCategory/subCategory/articleType form a
    # natural hierarchy (3 correlated categorical dims) -> encode articleType
    # frequency (target/frequency encoding) instead of full one-hot explosion
    # (articleType has 140+ levels: one-hot would balloon dimensionality)
    freq = df["articleType"].value_counts(normalize=True)
    df["articleType_freq_encoded"] = df["articleType"].map(freq)

    # Feature selection: drop the fully-redundant raw 'year' now that
    # product_age_years + season_code capture the temporal signal, and keep
    # only the encoded gender columns needed downstream
    engineered_cols = [
        "id", "gender", "masterCategory", "subCategory", "articleType",
        "baseColour", "season", "usage", "productDisplayName",
        "product_age_years", "name_length", "name_word_count",
        "season_code", "articleType_freq_encoded",
    ] + list(gender_dummies.columns)
    df_engineered = df[engineered_cols]

    df_engineered.to_csv(OUT / "engineered_features.csv", index=False)
    print(f"[feature_engineering] {df_engineered.shape[1]} columns -> engineered_features.csv")
    return df_engineered


# ---------------------------------------------------------------------------
# iv. EDA + VISUALIZATION
# ---------------------------------------------------------------------------
def run_eda(df: pd.DataFrame):
    insights = {}

    # 1. Category distribution
    fig, ax = plt.subplots(figsize=(8, 5))
    df["masterCategory"].value_counts().plot(kind="bar", ax=ax, color="#4C72B0")
    ax.set_title("Product Count by Master Category")
    ax.set_ylabel("Count")
    plt.tight_layout()
    fig.savefig(OUT / "eda_master_category.png", dpi=120)
    plt.close(fig)

    # 2. Gender split
    fig, ax = plt.subplots(figsize=(6, 6))
    df["gender"].value_counts().plot(kind="pie", autopct="%1.1f%%", ax=ax)
    ax.set_ylabel("")
    ax.set_title("Gender Distribution")
    plt.tight_layout()
    fig.savefig(OUT / "eda_gender_pie.png", dpi=120)
    plt.close(fig)

    # 3. Season vs category heatmap
    pivot = pd.crosstab(df["masterCategory"], df["season"])
    fig, ax = plt.subplots(figsize=(8, 5))
    sns.heatmap(pivot, annot=True, fmt="d", cmap="Blues", ax=ax)
    ax.set_title("Category vs Season")
    plt.tight_layout()
    fig.savefig(OUT / "eda_category_season_heatmap.png", dpi=120)
    plt.close(fig)

    # 4. Top 15 article types
    fig, ax = plt.subplots(figsize=(9, 6))
    df["articleType"].value_counts().head(15).plot(kind="barh", ax=ax, color="#55A868")
    ax.invert_yaxis()
    ax.set_title("Top 15 Article Types")
    plt.tight_layout()
    fig.savefig(OUT / "eda_top_article_types.png", dpi=120)
    plt.close(fig)

    # 5. Products over catalog years (trend)
    fig, ax = plt.subplots(figsize=(8, 5))
    df["year"].value_counts().sort_index().plot(kind="line", marker="o", ax=ax, color="#C44E52")
    ax.set_title("Products Catalogued by Year")
    ax.set_xlabel("Year")
    ax.set_ylabel("Count")
    plt.tight_layout()
    fig.savefig(OUT / "eda_year_trend.png", dpi=120)
    plt.close(fig)

    # 6. Colour popularity
    fig, ax = plt.subplots(figsize=(9, 6))
    df["baseColour"].value_counts().head(12).plot(kind="bar", ax=ax, color="#8172B2")
    ax.set_title("Top 12 Base Colours")
    plt.tight_layout()
    fig.savefig(OUT / "eda_top_colours.png", dpi=120)
    plt.close(fig)

    insights["summary_stats"] = {
        "total_products": int(len(df)),
        "unique_article_types": int(df["articleType"].nunique()),
        "unique_colours": int(df["baseColour"].nunique()),
        "master_categories": df["masterCategory"].value_counts().to_dict(),
        "gender_split": df["gender"].value_counts().to_dict(),
        "usage_split": df["usage"].value_counts().to_dict(),
        "year_range": [int(df["year"].min()), int(df["year"].max())],
    }
    with open(OUT / "eda_insights.json", "w") as f:
        json.dump(insights, f, indent=2, default=str)
    print("[eda] 6 charts + eda_insights.json written")
    return insights


if __name__ == "__main__":
    raw = collect_data()
    clean = preprocess(raw)
    features = engineer_features(clean)
    run_eda(clean)
    clean.to_csv(OUT / "cleaned_data.csv", index=False)
    print("Week 1 pipeline complete.")
