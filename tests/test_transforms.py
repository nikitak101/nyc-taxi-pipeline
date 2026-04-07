import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pytest
from pyspark.sql import SparkSession
from src.pipeline import transform
from src.quality import null_check


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local").appName("Test").getOrCreate()


# ✅ Test 1: tip percentage logic
def test_tip_pct_calculation(spark):
    data = [(1, 10.0, 2.0, 1, 1.0)]
    cols = ["VendorID", "fare_amount", "tip_amount", "PULocationID", "trip_distance"]

    df = spark.createDataFrame(data, cols)

    result = transform(df)
    row = result.collect()[0]

    # Expected tip_pct = (2 / 10) * 100 = 20
    assert row["tip_pct"] == 20.0


# ✅ Test 2: null check should raise error
def test_null_check_raises_error(spark):
    data = [(1, None, 1)]  # fare_amount is NULL
    cols = ["VendorID", "fare_amount", "PULocationID"]

    df = spark.createDataFrame(data, cols)

    with pytest.raises(ValueError):
        null_check(df, ["fare_amount"])



#run using pytest tests/ -v