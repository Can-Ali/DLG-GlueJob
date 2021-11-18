from pytest_bdd import scenarios, given, when, then, parsers
from awsglue.context import GlueContext
from pyspark.context import SparkContext

# sc = SparkContext()
# glueContext = GlueContext(sc)
# spark = glueContext.spark_session

target_file_path = "file_path"
column_list = ['operation', 'processeddate', 'changedate', 'changedate_year', 'changedate_month', 'changedate_day']

scenarios("../features/e2e.feature")


@given(parsers.cfparse("the glue job run successfully"))
def run_gluejob():
    """
    Make sure Glue job is successfully running
    """
    print("\nGlue Job run successfully!")


@when(parsers.cfparse("I validate data is created in parquet format"))
def validate_data_in_parquet_format(glue_context):
    """
    Validating date iin parquet format
    :param glue_context: @pytest.fixture
    """
    global df
    spark = glue_context[0]
    df = spark.read.parquet(target_file_path)
    assert df.count() > 0


@then(parsers.cfparse("I validate data table columns match the columns given"))
def validate_columns():
    """
    Validating columns are in the dataframe
    """
    assert all(column in column_list for column in df.columns)
    # assert len(df.columns) == len(column_list) -> If we know all the columns, we can compare the sizes
