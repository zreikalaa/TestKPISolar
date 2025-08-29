from typing import Dict
from pyspark.sql import DataFrame
from io_energy import get_spark, read_csv
import transforms as T
from pyspark.sql import DataFrame, functions as F


def run(input_paths: Dict[str, str], output_path: str, mode: str = "overwrite") -> DataFrame:
    """
    input_paths keys:
      - inverter_yields
      - static_inverter_info
      - sldc_events
      - site_median_reference
    """
    spark = get_spark()

    inv  = read_csv(spark, input_paths["inverter_yields"])
    st   = read_csv(spark, input_paths["static_inverter_info"])
    ev   = read_csv(spark, input_paths["sldc_events"])
    ref  = read_csv(spark, input_paths["site_median_reference"])

    
    df = T.join_inverter_static(inv, st)
    df = T.join_with_events(df, ev)
    df = T.join_with_site_ref(df, ref)
    df = T.compute_potential_production(df)
    df = T.keep_storage_dc(df)
    df = T.add_year_month(df)

    df.show(2, vertical=True, truncate=False)
    
    (df.write.mode(mode)
       .partitionBy("project_code", "year_month")
       .parquet(output_path))
    
    df_sites = (
    df.groupBy("project_code")
      .agg(F.sum("potential_production").alias("total_production"))
      .orderBy("total_production", ascending=True)
      .limit(5)
    )
    df_sites.show(5, vertical=True, truncate=False)
    
    return df
    
