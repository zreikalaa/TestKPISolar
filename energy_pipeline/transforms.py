from pyspark.sql import DataFrame, functions as F

# --- joins ---
def join_inverter_static(inv: DataFrame, st: DataFrame) -> DataFrame:
    return inv.join(st, on="logical_device_mrid", how="left").drop(st.logical_device_mrid)

def join_with_events(df: DataFrame, events: DataFrame) -> DataFrame:
    cond = (
        (df.logical_device_mrid == events.logical_device_mrid) &  
        (df.ts_start > events.ts_start) &
        (df.ts_start < events.ts_end)
    )
    return (
        df.join(events, cond, "left")
          .drop(events.ts_start, events.ts_end, events.logical_device_mrid)
    )

def join_with_site_ref(df: DataFrame, ref: DataFrame) -> DataFrame:
    cond = (df.project_code == ref.project_code) & (F.to_date(df.ts_start) == ref.ts_start)
    return df.join(ref, cond, "left").drop(ref.ts_start, ref.project_code)

# --- business logic ---
def compute_potential_production(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "potential_production",
        F.col("specific_yield_ac") * F.col("ac_max_power") * F.lit(1/6.0)
    )

def keep_storage_dc(df: DataFrame) -> DataFrame:
    return df.filter((F.col("inverter_function_type") == "Storage") & (F.col("storage_inverter_type") == "DC-Coupled"))

def add_year_month(df: DataFrame) -> DataFrame:
    return df.withColumn("year_month", F.date_format("ts_start", "yyyy-MM"))
