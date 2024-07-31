/*
    Welcome to your first dbt model in the internal layer!

    The following code creates a table named as contributor in dataset `get_help_my_data_product_v1_internal` in BigQuery.

    In contrast to traditional SQL, dbt offers a streamlined approach for table creation.
    Instead of using explicit commands, e.g. `CREATE TABLE ...` or `CREATE OR REPLACE VIEW AS ...`,
    a table or view can be created directly within a `dbt` model (a `.sql` file in the `dbt/models` folder`) using a `SELECT` statement.
    The name of the resulting table or view is automatically derived from the file name itself.

    The code {{ config(materialized='table') }} in dbt sets the materialization type to 'table' for the current model.
    By default, dbt assumes it to be a 'view'.
    To understand the differences between views and tables, please refer to the dbt official docs: https://docs.getdbt.com/docs/build/materializations
*/


{{ config(materialized='table', alias=var('branch')) }}

SELECT 1 id, 'John Doe' name, 'john.doe@gmail.com' email, '123 Fake Street, Sydney, NSW, 2000' address UNION ALL
SELECT 2 id, 'Jane Smith' name, 'jane.smith@gmail.com' email, '123 Fake Street, Melbourne, VIC, 3000' address  UNION ALL
SELECT 3 id, 'David Johnson' name, 'david.johnson@gmail.com' email, '123 Fake Street, Adelaide, SA, 5000' address
