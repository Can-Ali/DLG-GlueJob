Feature: Test transform SAS data to parquet format

  Scenario: Validate data transformation
    Given the glue job run successfully
    When I validate data is created in parquet format
    Then I validate data table columns match the columns given




