# Power BI — DAX Measures

Create these under **Modeling → New Measure** with `fact_sales` selected as the
home table (unless noted otherwise). Organize them into a display folder called
`_Measures` for a clean field list.

## Core KPIs

```dax
Total Revenue = SUM(fact_sales[revenue])

Total Units Sold = SUM(fact_sales[quantity])

Total Transactions = COUNTROWS(fact_sales)

Average Order Value =
DIVIDE([Total Revenue], DISTINCTCOUNT(fact_sales[order_id]), 0)

Total Products =
DISTINCTCOUNT(dim_product[product_id])

Total Customers =
DISTINCTCOUNT(dim_customer[customer_id])
```

## Time intelligence (requires `dim_time` marked as a Date table)

```dax
Revenue QoQ % =
VAR CurrentQ = [Total Revenue]
VAR PrevQ =
    CALCULATE(
        [Total Revenue],
        DATEADD(dim_time[order_date], -1, QUARTER)
    )
RETURN DIVIDE(CurrentQ - PrevQ, PrevQ, 0)

Revenue YTD =
TOTALYTD([Total Revenue], dim_time[order_date])

Revenue Same Period Last Year =
CALCULATE([Total Revenue], SAMEPERIODLASTYEAR(dim_time[order_date]))
```

> To enable time intelligence: select `dim_time` table → **Mark as Date Table**
> → choose `order_date` as the date column. Also set `dim_time[order_date]`
> data type to **Date** (it imports as text from CSV by default — fix in Power
> Query with `Table.TransformColumnTypes(..., {{"order_date", type date}})`).

## Category / product analysis

```dax
Revenue Rank by Category =
RANKX(ALL(dim_product[category_name]), [Total Revenue], , DESC)

Top Category =
CALCULATE(
    VALUES(dim_product[category_name]),
    TOPN(1, ALL(dim_product[category_name]), [Total Revenue])
)

% of Total Revenue =
DIVIDE([Total Revenue], CALCULATE([Total Revenue], ALL(dim_product)))
```

## Data-quality measures (for a "Week 1/5 quality scorecard" visual)

Pull these directly as columns from `week1_eda/outputs/preprocessing_report.json`
and `week5_resilience/outputs/week5_summary.json` if you import them as a small
manual table (Enter Data), or hardcode as measures for the demo:

```dax
Rows After Cleaning = COUNTROWS(dim_product)

Duplicate Rows Removed = 7        -- from preprocessing_report.json duplicates_removed
Noisy Rows Removed = 0            -- from preprocessing_report.json noisy_year_rows_removed
```

## Button/navigation helper measure (used with bookmarks — see BUILD_GUIDE.md)

```dax
Selected Page Label =
SWITCH(
    TRUE(),
    ISFILTERED('_NavState'[Page]), SELECTEDVALUE('_NavState'[Page]),
    "Overview"
)
```
