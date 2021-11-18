import datetime
import pytest
from pytest_bdd import scenarios, given, when, then, parsers
from gluejob.Glue_Job_SAS_To_Parquet import create_glue_context, list_folders, read_sas_table, add_audit_cols, \
    write_to_parquet


expected_list = ['table1', 'table2', 'table3']

scenarios("../features/unit_tests.feature")


@pytest.fixture
def glue_context():
    return create_glue_context()


@given(parsers.cfparse('I get folder list by using these parameters "<client>" "<bucket_name>" "<prefix>"'))
def get_folder_list(client, bucket_name, prefix):
    """
    Getting list of folders by using list_folders method
    :param client: boto3 client
    :param bucket_name: S3 bucket from which the directories are required
    :param prefix: Prefix of the input S3 bucket
    """
    global actual_list
    actual_list = list_folders(client, bucket_name, prefix)


@then(parsers.cfparse('I validate folder list has expected tables'))
def validate_tables():
    """
    Validating folder names are matching
    """
    assert all(table_name in actual_list for table_name in expected_list)
    # assert len(actual_list) == len(expected_list) -> If we know all the columns, we can compare the sizes


@given(parsers.cfparse('I read "{filepath}"'))
def read_file_in_sas_format(glue_context, filepath):
    """
    Reading a SAS table using spark and converts it to Spark Datafrane
    :param glue_context: @pytest.fixture
    :param filepath
    """
    global df
    spark = glue_context[0]
    df = read_sas_table(spark, filepath)


@when(parsers.cfparse('I add columns changed'))
def add_columns():
    """
    Adds audit columns to the dataframe
    """
    global df
    df = add_audit_cols(df, datetime.now())


@then(parsers.cfparse('I convert data to parquet format as "{writemode}" partition_cols as "{partition_cols}" target_path as "{target_path}"'))
def write_data_in_parquet_as(writemode, partition_cols, target_path):
    """
    Writing data in parquet format
    :param writemode
    :param partition_cols
    :param target_path
    """
    write_to_parquet(df, writemode, partition_cols, target_path)


@given(parsers.cfparse('I read data in parquet format from "{target_filepath}"'))
def read_file_in_parquet_format(glue_context, target_filepath):
    """
    Reading dataframe in parquet format
    :param glue_context: @pytest.fixture
    :param target_filepath
    """
    global df_parquet
    spark = glue_context[0]
    df_parquet = spark.read.parquet(target_filepath)


@then(parsers.cfparse('I validate I have records'))
def validate_records():
    """
    Validating dataframe in parquet format has data
    """
    assert df_parquet.count() > 0
