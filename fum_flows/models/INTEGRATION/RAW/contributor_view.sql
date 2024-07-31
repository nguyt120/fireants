/*
    Welcome to your first dbt model in the consumption layer!

    The following code snippet creates a view named as `contributor_view` of dataset `get_help_my_data_product_v1` in BigQuery.
    BigQuery views are read-only.
    This view is designed to mirror the contents of the "contributor" table located within the internal layer. A BigQuery view is a virtual table that does not store data directly.
    Instead, it acts as a saved SQL query or a logical representation of data derived from one or more tables.

    By utilizing the ref function, the view table dynamically references and reflects the data from the underlying "contributor" table.
    This means that any changes made to the "contributor" table in the internal layer will be automatically reflected in the "contributor_view" view table.

    This approach ensures that the "contributor_view" table always provides an up-to-date and consistent representation of the data in the internal layer.
    It allows your data product consumers to conveniently query and work with the "contributor" data from the consumption layer without the need to directly access the internal layer.

*/

select * from {{ref('contributor')}}
