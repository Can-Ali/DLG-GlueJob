Feature: Validate functions of the app

  Scenario Outline: Validate data table
    Given I get folder list by using these parameters "<client>" "<bucket_name>" "<prefix>"
    Then I validate folder list has expected tables
    Examples:
      | client    | bucket_name | prefix |
      | s3_client | bucket_name | /      |

  Scenario: Validate read table function
    Given I read sas table from "filepath"
    Then I validate data has records

  Scenario: Validate column names changed
    Given I read "filepath"
    When I add columns changed
    Then I validate data table columns match the columns given

  Scenario: Validate after writing data to target path
    Given I read "filepath"
    When I add columns changed
    Then I convert data to parquet format as "overwrite" partition_cols as "processeddate" target_path as "target_path"
    Then I read data in parquet format from "target_filepath"
    Then I validate data table columns match the columns given
    Then I validate I have records