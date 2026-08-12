# Power BI — Power Query (M) Scripts

Paste these into **Power BI Desktop → Home → Transform Data → Advanced Editor**
(create a new blank query first, then replace its contents with the script below).

Update `PROJECT_PATH` at the top of each script to wherever you unzip this project,
e.g. `"C:\Users\Mouli\delab\"` (Windows) — keep the trailing backslash.

---

## 1. Star schema dimension & fact tables (static import)

For each of these five tables, create one query pointing at its CSV:

| Query name | Source file |
|---|---|
| `fact_sales` | `powerbi/data/fact_sales.csv` |
| `dim_product` | `powerbi/data/dim_product.csv` |
| `dim_customer` | `powerbi/data/dim_customer.csv` |
| `dim_store` | `powerbi/data/dim_store.csv` |
| `dim_time` | `powerbi/data/dim_time.csv` |

Template M script (repeat per table, changing the file name and typed-column list):

```m
let
    ProjectPath = "C:\Users\<you>\delab\powerbi\data\",
    Source = Csv.Document(File.Contents(ProjectPath & "fact_sales.csv"),
        [Delimiter=",", Columns=9, Encoding=65001, QuoteStyle=QuoteStyle.None]),
    PromotedHeaders = Table.PromoteHeaders(Source, [PromoteAllScalars=true]),
    ChangedType = Table.TransformColumnTypes(PromotedHeaders,{
        {"order_item_id", Int64.Type}, {"order_id", Int64.Type},
        {"product_id", Int64.Type}, {"customer_id", Int64.Type},
        {"store_id", Int64.Type}, {"time_id", Int64.Type},
        {"quantity", Int64.Type}, {"unit_price", type number},
        {"revenue", type number}
    })
in
    ChangedType
```

After importing all five, go to **Model view** and draw the relationships (all
one-to-many, single direction, fact table on the "many" side):

```
dim_product[product_id]  1 ──< many  fact_sales[product_id]
dim_customer[customer_id] 1 ──< many  fact_sales[customer_id]
dim_store[store_id]       1 ──< many  fact_sales[store_id]
dim_time[time_id]         1 ──< many  fact_sales[time_id]
```

This gives you the classic star layout for the Week 3 dashboard page, and you can
drop a **Table** visual straight onto the canvas bound to any of these queries to
literally show the star-schema tables with live data, exactly as asked.

---

## 2. Auto-ETL-on-new-data: the "Folder" pattern (this is the key ask)

Instead of pointing at one static CSV, point Power BI at a **folder**. Every time
you refresh (manually, or on Power BI Service's scheduled refresh), it re-scans
the folder, picks up any *new* files dropped there, and re-runs the same cleaning
transformation on them automatically — this is the closest true equivalent to
"give it new data and it re-runs EDA/ETL" inside Power BI itself (Power BI has no
native statistical-EDA engine, so the actual profiling/chart-generation is done
by the Python pipeline — see the Airflow section for the automatic trigger on the
Python side. This M query is the BI-side counterpart: automatic re-ingestion +
re-transformation of whatever lands in the folder).

**Setup:** drop new/updated `styles.csv`-shaped files into `powerbi/data/incoming/`.

```m
let
    IncomingFolder = "C:\Users\<you>\delab\powerbi\data\incoming\",

    // 1. Pick up EVERY csv currently in the folder — this list grows automatically
    //    as new files are added, with zero query changes needed
    Source = Folder.Files(IncomingFolder),
    FilterCsv = Table.SelectRows(Source, each Text.EndsWith([Name], ".csv")),

    // 2. Parse + transform each file identically (this IS the "ETL" step —
    //    every new file goes through the same cleaning logic automatically)
    AddParsed = Table.AddColumn(FilterCsv, "ParsedData", each
        let
            raw = Csv.Document([Content], [Delimiter=",", Encoding=65001]),
            headered = Table.PromoteHeaders(raw, [PromoteAllScalars=true]),
            // --- cleaning steps mirror week1_eda/pipeline_week1.py ---
            trimmed = Table.TransformColumns(headered, {
                {"gender", Text.Trim}, {"masterCategory", Text.Trim},
                {"subCategory", Text.Trim}, {"articleType", Text.Trim},
                {"baseColour", Text.Trim}, {"season", Text.Trim},
                {"usage", Text.Trim}, {"productDisplayName", Text.Trim}
            }),
            filledColour = Table.ReplaceValue(trimmed, null, "Unknown",
                Replacer.ReplaceValue, {"baseColour"}),
            filledSeason = Table.ReplaceValue(filledColour, null, "Unknown",
                Replacer.ReplaceValue, {"season"}),
            filledUsage = Table.ReplaceValue(filledSeason, null, "Unknown",
                Replacer.ReplaceValue, {"usage"}),
            dedup = Table.Distinct(filledUsage, {"id"})
        in
            dedup
    ),

    // 3. Expand every parsed file into one unified table (union of all batches,
    //    old + new, so historical data is preserved while new drops are appended)
    Expanded = Table.ExpandTableColumn(AddParsed, "ParsedData",
        {"id","gender","masterCategory","subCategory","articleType","baseColour",
         "season","year","usage","productDisplayName"}),

    // 4. Final typed output
    ChangedType = Table.TransformColumnTypes(Expanded, {
        {"id", Int64.Type}, {"year", Int64.Type}
    }),

    // 5. Track ingestion metadata (so you can filter "what's new since last refresh"
    //    in a visual, or audit which file a row came from)
    WithSource = Table.AddColumn(ChangedType, "source_file",
        each [Name], type text)
in
    WithSource
```

Set this query to load into the model as `catalog_incoming`, then in **Power BI
Service**, configure **Scheduled Refresh** (Dataset settings → Refresh) so this
folder is re-scanned on whatever cadence you want (hourly/daily) — genuinely
automatic, no manual re-import needed when new data shows up.

> **Note on true EDA automation:** Power BI itself doesn't run statistical
> profiling (missing-value %, outlier detection, distribution plots) as part of
> refresh — that requires actual computation (pandas/matplotlib), which is what
> `week1_eda/pipeline_week1.py` does. The Airflow DAG (`orchestration/airflow/dags/`)
> is what makes *that* side fully automatic: it watches the same `incoming/`
> folder and re-runs the whole Week 1-5 Python pipeline the moment a new file
> lands, regenerating the CSVs this Power BI folder-query reads. Point both at
> the same folder and you get end-to-end automation: new file → Airflow re-runs
> EDA/ETL → Power BI refresh picks up the regenerated outputs.

---

## 3. Aggregated category rollup (for Week 2 ETL page)

```m
let
    Source = Csv.Document(File.Contents(
        "C:\Users\<you>\delab\week2_etl\outputs\aggregated_by_category.csv"),
        [Delimiter=",", Encoding=65001]),
    Headers = Table.PromoteHeaders(Source, [PromoteAllScalars=true]),
    Typed = Table.TransformColumnTypes(Headers,{
        {"masterCategory", type text}, {"product_count", Int64.Type},
        {"avg_stock", type number}, {"avg_rating", type number},
        {"total_reviews", Int64.Type}
    })
in
    Typed
```
