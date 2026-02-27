import pandas as pd
import numpy as np

RAW_PATH = "data_raw/"
OUT_PATH = "data/"

print("ETL started...")


users = pd.read_csv(RAW_PATH + "users.csv")
sessions = pd.read_csv(RAW_PATH + "sessions.csv")
print("Sessions columns:", sessions.columns.tolist())
events = pd.read_csv(RAW_PATH + "events.csv")
print("Events columns:", events.columns.tolist())
orders = pd.read_csv(RAW_PATH + "orders.csv")
order_items = pd.read_csv(RAW_PATH + "order_items.csv")
products = pd.read_json(RAW_PATH + "products.json")
campaigns = pd.read_csv(RAW_PATH + "campaigns.csv")

print("All files loaded")

def clean_columns(df):
    df.columns = df.columns.str.lower().str.strip()
    return df

def clean_strings(df):
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].str.lower().str.strip()
    return df

# apply
users = clean_strings(clean_columns(users)).drop_duplicates()
sessions = clean_strings(clean_columns(sessions)).drop_duplicates()
events = clean_strings(clean_columns(events)).drop_duplicates()
events = events.rename(columns={"event_type": "event_name"})
orders = clean_strings(clean_columns(orders)).drop_duplicates()
order_items = clean_strings(clean_columns(order_items)).drop_duplicates()
products = clean_strings(clean_columns(products)).drop_duplicates()
campaigns = clean_strings(clean_columns(campaigns)).drop_duplicates()

# handle missing values
sessions["device"] = sessions["device"].fillna("unknown")
sessions["channel"] = sessions["channel"].fillna("unknown")

# convert timestamps
# convert timestamps
sessions = sessions.rename(columns={"session_start_ts": "start_ts"})
sessions["start_ts"] = pd.to_datetime(sessions["start_ts"], errors="coerce")
events["event_ts"] = pd.to_datetime(events["event_ts"], errors="coerce")
orders["order_ts"] = pd.to_datetime(orders["order_ts"], errors="coerce")
print("Basic cleaning completed")


funnel = (
    events
    .pivot_table(
        index="session_id",
        columns="event_name",
        values="event_ts",
        aggfunc="min"
    )
    .reset_index()
)

funnel["has_product_view"] = funnel["product_view"].notna().astype(int)
funnel["has_add_to_cart"] = funnel["add_to_cart"].notna().astype(int)
funnel["has_begin_checkout"] = funnel["begin_checkout"].notna().astype(int)
funnel["has_payment_attempt"] = funnel["payment_attempt"].notna().astype(int)
funnel["has_purchase"] = funnel["purchase"].notna().astype(int)

print("Funnel flags created")
print(funnel.head())

print(funnel[["has_product_view","has_add_to_cart",
              "has_begin_checkout","has_payment_attempt",
              "has_purchase"]].sum())


funnel["time_to_cart_sec"] = (
    funnel["add_to_cart"] - funnel["product_view"]
).dt.total_seconds()

funnel["time_to_checkout_sec"] = (
    funnel["begin_checkout"] - funnel["product_view"]
).dt.total_seconds()

funnel["time_to_purchase_sec"] = (
    funnel["purchase"] - funnel["product_view"]
).dt.total_seconds()

print("Time-to-step metrics created")


session_duration = (
    events
    .groupby("session_id")["event_ts"]
    .agg(lambda x: (x.max() - x.min()).total_seconds())
    .reset_index(name="session_duration_sec")
)

print("Session duration calculated")


# merge sessions + funnel + session_duration
fact_sessions = (
    sessions
    .merge(funnel, on="session_id", how="left")
    .merge(session_duration, on="session_id", how="left")
)

# replace null flags with 0
flag_cols = [
    "has_product_view",
    "has_add_to_cart",
    "has_begin_checkout",
    "has_payment_attempt",
    "has_purchase"
]

for col in flag_cols:
    fact_sessions[col] = fact_sessions[col].fillna(0).astype(int)

fact_sessions.to_csv(OUT_PATH + "fact_sessions.csv", index=False)

print("fact_sessions.csv created")


basket_summary = (
    order_items
    .groupby("order_id")
    .agg(
        total_items=("quantity", "sum"),
        distinct_products=("product_id", "nunique")
    )
    .reset_index()
)

order_items_enriched = order_items.merge(
    products,
    on="product_id",
    how="left"
)

order_items_enriched["item_cost"] = (
    order_items_enriched["cost"] *
    order_items_enriched["quantity"]
)

cost_rating = (
    order_items_enriched
    .groupby("order_id")
    .agg(
        total_cost=("item_cost", "sum"),
        avg_product_rating=("rating", "mean")
    )
    .reset_index()
)

top_category = (
    order_items_enriched
    .groupby(["order_id", "category"])
    .size()
    .reset_index(name="cnt")
    .sort_values(["order_id", "cnt"], ascending=[True, False])
    .drop_duplicates("order_id")
    [["order_id", "category"]]
    .rename(columns={"category": "top_category"})
)

fact_orders = (
    orders
    .merge(basket_summary, on="order_id", how="left")
    .merge(cost_rating, on="order_id", how="left")
    .merge(top_category, on="order_id", how="left")
)

fact_orders["margin_proxy"] = (
    fact_orders["net_amount"] -
    fact_orders["total_cost"]
)

fact_orders.to_csv(OUT_PATH + "fact_orders.csv", index=False)

print("fact_orders.csv created")

# -------------------------
# STEP 9: BUILD DIM_USERS_ENRICHED
# -------------------------

lifetime_sessions = (
    fact_sessions
    .groupby("user_id")["session_id"]
    .nunique()
    .reset_index(name="lifetime_sessions")
)
lifetime_orders = (
    fact_orders
    .groupby("user_id")["order_id"]
    .nunique()
    .reset_index(name="lifetime_orders")
)

order_dates = (
    fact_orders
    .groupby("user_id")["order_ts"]
    .agg(
        first_order_date="min",
        last_order_date="max"
    )
    .reset_index()
)

dim_users = (
    users
    .merge(lifetime_sessions, on="user_id", how="left")
    .merge(lifetime_orders, on="user_id", how="left")
    .merge(order_dates, on="user_id", how="left")
)

dim_users["lifetime_sessions"] = dim_users["lifetime_sessions"].fillna(0)
dim_users["lifetime_orders"] = dim_users["lifetime_orders"].fillna(0)

dim_users["repeat_rate_flag"] = (
    dim_users["lifetime_orders"] >= 2
).astype(int)

user_revenue = (
    fact_orders
    .groupby("user_id")["net_amount"]
    .sum()
    .reset_index(name="lifetime_revenue")
)

dim_users = dim_users.merge(user_revenue, on="user_id", how="left")
dim_users["lifetime_revenue"] = dim_users["lifetime_revenue"].fillna(0)

median_revenue = dim_users["lifetime_revenue"].median()

def assign_value_band(x):
    if x == 0:
        return "low"
    elif x <= median_revenue:
        return "medium"
    else:
        return "high"

dim_users["user_value_band"] = dim_users["lifetime_revenue"].apply(assign_value_band)

dim_users.to_csv(OUT_PATH + "dim_users_enriched.csv", index=False)

print("dim_users_enriched.csv created")