# Power BI — Build Guide

This turns the exported data + M scripts + DAX into the actual `.pbix` dashboard,
in Power BI Desktop (Windows/Mac), with button-based navigation between pages
mirroring the 5-week structure.

## 1. Import the data

1. Open Power BI Desktop → **Get Data → Text/CSV** (or **Folder** for the
   auto-refresh query) and bring in each table listed in
   `POWER_QUERY_M_SCRIPTS.md` §1. Use the Advanced Editor to paste the M
   script directly instead of clicking through the UI — faster and avoids
   typos in the column-type list.
2. Add the `catalog_incoming` folder query from §2 for the auto-ETL pane.
3. **Home → Transform Data → Close & Apply.**

## 2. Build the model (star schema)

1. Switch to **Model view**.
2. Drag `dim_product[product_id]` onto `fact_sales[product_id]` to create the
   relationship; repeat for `dim_customer`, `dim_store`, `dim_time`. Power BI
   should auto-detect these as 1-to-many with a single filter direction
   (fact → dimensions) — leave it at the default (don't enable bidirectional
   filtering, it's not needed here and hurts performance at this row count).
3. Right-click `dim_time` → **Mark as Date Table** → column `order_date`.
4. Arrange the 5 tables visually into an actual star shape (fact in the
   middle, dimensions around it) — this is literally your Week 3 ERD,
   rendered live inside the tool.

## 3. Add the DAX measures

Paste each block from `DAX_MEASURES.md` as a new measure. Group them into a
display folder (right-click measure → **Display Folder** → `_Measures`) so
the field list stays clean.

## 4. Build the report pages

Create one page per week, matching the dashboard structure you already had in
Streamlit:

| Page | Visuals |
|---|---|
| **Overview** | Card visuals for `[Total Revenue]`, `[Total Products]`, `[Total Transactions]`; a nav button bar (see §5) |
| **Week 1 – EDA** | Import the 6 PNGs from `week1_eda/outputs/` as Image visuals, or rebuild as native bar/pie charts against `dim_product` for interactivity |
| **Week 2 – ETL** | Table visual bound to the `aggregated_by_category` query; card visuals for extraction/load row counts (paste static numbers from `week2_etl/outputs/week2_summary.json`, or import that JSON as a table via **Get Data → JSON**) |
| **Week 3 – Schema** | **Table visuals showing `dim_product`, `fact_sales`, `dim_time`, etc. directly with live data** (drag the whole query onto the canvas as a Table visual — this satisfies "show the table also with data for the star schema"); a matrix visual for the `GROUP BY CUBE` output (import `week3_schema/outputs/cube_full_grouping_sets.csv`) |
| **Week 4 – Batch Pipeline** | Card visuals for rows loaded/verified from `week4_pipeline/outputs/week4_summary.json` |
| **Week 5 – Resilience** | Card visuals for idempotency/atomicity/backfill proof numbers (import the JSON reports) |

## 5. Button-based navigation (bookmarks)

This is the standard Power BI pattern for a "dashboard with buttons":

1. **View → Bookmarks pane** and **Selection pane** (both under the View ribbon).
2. Go to each page, get it looking the way you want, then in the Bookmarks
   pane click **Add** — name it to match the page (`bm_Overview`, `bm_Week1`,
   `bm_Week2`, …).
   - For a genuine single-page-app feel: instead of 6 separate report pages,
     build **one page** with 6 groups of visuals stacked on top of each
     other, use the **Selection pane** to hide/show each group per bookmark
     (checkbox next to each visual group), and tick **Data**, **Display**,
     and **Current Page** off / **Selected visuals** on when creating each
     bookmark so it only toggles visibility, not filters.
   - Simpler alternative: keep 6 separate pages and just bookmark
     "Page 1 view", "Page 2 view", etc. — bookmarks still work across pages.
3. Insert a **Button** (Insert ribbon → Buttons → Blank) for each week. Set:
   - **Action → Type → Bookmark**, then pick the matching bookmark.
   - Style the button text ("Week 1", "Week 2", …) and place identical
     copies of the 5-button row on every page/bookmark state so navigation
     is always visible (select all 5 buttons + the row background → **Format
     → Selection Pane → group them → copy/paste onto every page**).
4. Test: **View → Reading View**, click each button, confirm it jumps/toggles
   correctly. Bookmark buttons work in Reading View and once published to the
   Power BI Service — they don't require Edit mode.

## 6. Publish & schedule refresh (closes the "new data → auto ETL" loop)

1. **Home → Publish** to your Power BI workspace.
2. On the published dataset → **Settings → Scheduled Refresh** → set a daily
   (or hourly) cadence. Combined with the Airflow DAG re-generating the CSVs
   whenever new data lands in `powerbi/data/incoming/` and
   `data/raw/styles.csv`, this gives you the full loop:

   ```
   New data dropped -> Airflow FileSensor detects it -> Airflow re-runs
   Week 1-5 Python pipeline -> CSVs in week*/outputs/ + powerbi/data/
   regenerate -> Power BI scheduled refresh picks up the new files ->
   dashboard updates with zero manual steps
   ```

   (If you're on Power BI Desktop only, without a Pro/Service license, just
   hit **Refresh** manually after the Airflow DAG run — same result, one
   extra click.)
