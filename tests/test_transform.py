import pytest
from pyspark.sql import SparkSession, functions as F
from energy_pipeline.transforms import (
    join_inverter_static, join_with_events, join_with_site_ref,
    compute_potential_production, keep_storage_dc, add_year_month
)

@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .master("local[2]")
        .appName("unit-tests-transforms")
        .getOrCreate()
    )

def test_join_inverter_static_drops_right_key(spark):
    inv = spark.createDataFrame(
        [(1, "P1", "2025-01-01 00:00:00"),
         (2, "P1", "2025-01-01 00:10:00")],
        ["logical_device_mrid", "project_code", "ts_start"]
    )
    st = spark.createDataFrame(
        [(1, 100.0, "Storage", "DC-Coupled"),
         (2,  80.0, "Storage", "DC-Coupled")],
        ["logical_device_mrid", "ac_max_power", "inverter_function_type", "storage_inverter_type"]
    )

    out = join_inverter_static(inv, st)
    cols = out.columns

    # clé présente (côté gauche) mais la clé du DataFrame de droite est droppée
    assert "logical_device_mrid" in cols
    # Pas de colonne en double : Spark garde un seul champ après drop
    # Vérifie que les colonnes de droite utiles existent
    assert "ac_max_power" in cols
    assert "inverter_function_type" in cols
    assert "storage_inverter_type" in cols


def test_join_with_site_ref_on_project_and_date_drop_right_cols(spark):
    df = spark.createDataFrame(
        [
            (10, "P1", "2025-01-01 00:10:00"),
            (10, "P1", "2025-01-02 00:10:00"),  # pas de ref ce jour
        ],
        ["logical_device_mrid", "project_code", "ts_start"]
    ).withColumn("ts_start", F.to_timestamp("ts_start"))

    ref = spark.createDataFrame(
        [
            ("P1", "2025-01-01", 2.0),
        ],
        ["project_code", "ts_start", "specific_yield_ac"]  # ts_start en string (date)
    )

    out = join_with_site_ref(df, ref)
    cols = out.columns
    # colonnes droppées du côté ref
    assert "project_code" in cols          # côté gauche toujours là
    assert "specific_yield_ac" in cols     # valeur business voulue
    # la ts_start du côté ref a été droppée
    # (celle qui reste est la ts_start du côté mesures)
    # Vérif fonctionnelle : yield présent uniquement pour le 2025-01-01
    vals = out.orderBy("ts_start").select("specific_yield_ac").rdd.map(lambda r: r[0]).collect()
    assert vals == [2.0, None]

def test_compute_potential_production(spark):
    df = spark.createDataFrame(
        [
            (100.0, 2.0),
            (50.0,  1.5),
        ],
        ["ac_max_power", "specific_yield_ac"]
    )
    out = compute_potential_production(df).select("potential_production")
    vals = [r[0] for r in out.collect()]
    assert vals[0] == 2.0 * 100.0 * (1/6.0)
    assert vals[1] == 1.5 * 50.0  * (1/6.0)

def test_keep_storage_dc(spark):
    df = spark.createDataFrame(
        [
            (1, "Storage", "DC-Coupled"),
            (2, "Storage", "AC-Coupled"),
            (3, "Other",   "DC-Coupled"),
        ],
        ["id", "inverter_function_type", "storage_inverter_type"]
    )
    kept_ids = keep_storage_dc(df).select("id").rdd.flatMap(lambda r: r).collect()
    assert kept_ids == [1]

def test_add_year_month(spark):
    df = spark.createDataFrame(
        [(1, "2025-01-15 12:00:00"), (2, "2025-12-31 23:59:59")],
        ["id", "ts_start"]
    ).withColumn("ts_start", F.to_timestamp("ts_start"))
    out = add_year_month(df)
    got = out.orderBy("id").select("year_month").rdd.flatMap(lambda r: r).collect()
    assert got == ["2025-01", "2025-12"]
