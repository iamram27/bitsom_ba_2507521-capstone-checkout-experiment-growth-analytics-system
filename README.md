# bitsom_ba_2507521-capstone-checkout-experiment-growth-analytics-system
End-to-end e-commerce A/B test analysis — ETL pipeline,  funnel diagnostics, statistical significance testing,  segment deep dive (HTE-lite), and 30-day revenue impact  estimation. Built with Python, Pandas, SciPy &amp; Jupyter.

 Project Overview
This project analyzes the impact of a redesigned checkout experience (Variant B) in an e-commerce environment.
The goal was to determine whether Variant B should be rolled out globally by evaluating:

Conversion impact
Revenue per session impact
Funnel leakages
Segment-level performance
30-day projected business impact

The project includes:
Data engineering (ETL pipeline)
Funnel analytics
A/B testing with statistical validation
Heterogeneous treatment effects (HTE-lite)
Business impact modeling
Interactive Power BI dashboard
Strategic decision memo

Business Objective :
Improve checkout conversion while maintaining or increasing revenue per session and margin.
Primary KPI (North Star)
Revenue per Session (RPS)
Supporting KPIs
Conversion Rate
Checkout → Purchase Conversion
Average Order Value (AOV)
Time-to-Purchase
Drop-off rates at each funnel step

Tools & Technologies Used
Programming & Data Processing
Python 3.13

Used for:
Data cleaning
Feature engineering
Funnel metric creation
Statistical testing
Impact modeling

Libraries Used:
pandas
Data transformation
Aggregations
Joins
Time-series grouping
Funnel metrics
numpy
Numerical computations
Lift calculations
Scenario modeling
matplotlib
KPI trend visualization
scipy.stats

Z-test for conversion significance
python-pptx

Final executive presentation automation:

Data Engineering
Custom ETL pipeline (etl_pipeline.py) was built to:
Load raw CSV datasets
Clean timestamp columns
Engineer funnel flags
Calculate time-to-step metrics
Aggregate session-level revenue
Generate analytics-ready fact tables

Business Intelligence:

Power BI Desktop:
Used for:
KPI dashboards
Funnel visualization
Segment explorer
A/B lift charts
Impact modeling
Sensitivity analysis
Interactive features:
Slicers (device, channel, segment, variant)
Drill-downs
Dynamic measures (DAX)
What-if parameters


Checkout_Capstone/
│
├── data_raw/
│   ├── sessions.csv
│   ├── events.csv
│   ├── orders.csv
│   ├── users.csv
│
├── data/
│   ├── fact_sessions.csv
│   ├── fact_orders.csv
│   ├── dim_users_enriched.csv
│
├── etl_pipeline.py
├── analysis.ipynb
├── dashboard.pbix
├── README.md

Setup Instructions :

1 : Clone Repository
git clone <repo-url>
cd Checkout_Capstone

2 : Install Dependencies
pip install pandas numpy matplotlib scipy python-pptx

3️ : Folder Structure Setup
Create:
data_raw/
data/

Place raw CSV files inside data_raw.

How to Run ETL End-to-End
From project root directory:
py etl_pipeline.py

What ETL Does

Step-by-step:
Loads raw datasets
Cleans timestamps
Creates funnel flags:
has_product_view
has_add_to_cart
has_begin_checkout
has_payment_attempt
has_purchase

Calculates:
time_to_cart_sec
time_to_checkout_sec
time_to_purchase_sec
session_duration_sec
Aggregates revenue per session
Merges user attributes
Outputs curated datasets

Data Outputs Generated
After ETL runs, the following files are created inside /data:

fact_sessions.csv
Session-level dataset containing:
session_id
user_id
device
channel
variant
funnel flags
time-to-step metrics
session_duration_sec

fact_orders.csv
Order-level dataset:
order_id
session_id
net_amount
total_items
payment_method

dim_users_enriched.csv
User-level attributes:
user_id
segment
city_tier
lifetime_revenue
user_value_band

How to Open Dashboard
Install Power BI Desktop
Open dashboard.pbix
Ensure data folder path matches your local machine
Refresh data if needed

Dashboard contains 5 pages:
Executive Summary
Funnel & Drop-offs
Segment Explorer
Experiment Deep Dive


Z-Test for Proportions
Used to test significance of conversion difference.
Assumptions:
Independent samples
Large sample size
Binary outcome

Impact Modeling:
30-day projection based on:
Observed lift
Average daily sessions
Revenue per session
Sensitivity analysis:
Best case (+25%)
Base case
Worst case (-25%)

Key Insights:
Variant B improves conversion by +18.7%
Largest funnel leakage at Product → Cart (69.4%)
Mobile paid social underperforms
Premium & Web users benefit most
Projected incremental revenue: ₹2.88 Lakhs (Base Case)












