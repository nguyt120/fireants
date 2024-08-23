{% macro insert_overwrite_fum_flows(relation) -%}
    {{
        config(
            materialized="incremental",
            on_schema_change="sync_all_columns",
            partition_by={
                "field": "transaction_date",
                "data_type": "date",
                "granularity": "day",
            },
            incremental_strategy="insert_overwrite",
            unique_key=["key_hash"]
        )
    }}

    {%- set partition_key = config.get("unique_key") -%}
    {%- set order_key = "last_updated_date DESC" -%}
    
    {% if is_incremental() %}
        {% set filter = get_filter(relation) %}
    {% endif %}

    {%- if is_incremental() -%}
        {# load the delta into a temporary table #}
        {%- set tmp_table_name = this.identifier ~ "__delta_tmp" -%}
        {%- set tmp_table_exists, tmp_relation = dbt.get_or_create_relation(
            database=this.database,
            schema=this.schema,
            identifier=tmp_table_name,
            type="table",
        ) -%}
        {%- set delta_query = read_from_(relation, filter) -%}
        {%- do run_query(create_table_as(True, tmp_relation, delta_query)) -%}
    {%- endif %}

    WITH
        {% if is_incremental() %}
            new_trxns as (select * from {{ tmp_relation }}),
            existing_trxns_to_compare as (
                select * except(inserted_datetime)
                from {{ this }}
                where transaction_date in (select distinct transaction_date from {{ tmp_relation }})
            ),
            combined as (
                select * from new_trxns
                union all
                select * from existing_trxns_to_compare
            ),
            trxns_to_upsert as (
                {{ remove_duplicates("combined", partition_key, order_key) }}
            )
        {% else %}
            new_trxns as ({{ read_from_(relation, None) }}),
            trxns_to_upsert as (
                {{ remove_duplicates("new_trxns", partition_key, order_key) }}
            )
        {% endif %}

    SELECT *, current_datetime() as inserted_datetime
    FROM trxns_to_upsert
{%- endmacro %}


{% macro remove_duplicates(relation, partition_key, order_key) -%}
    select *
    from {{ relation }}
    qualify
        row_number() over (
            partition by {{ partition_key | join(", ") }} order by {{ order_key }}
        )
        = 1
{%- endmacro %}


{% macro read_from_(relation, ft) %}
    select *
    from {{ relation }}
    {% if ft %}
    where {{ ft }}
    {% endif %}
{% endmacro %}


{% macro get_filter(relation) %}
    {% if is_incremental() %}
        {%- set sql_statement = 'select Max(transaction_date) from {}'.format(this) -%}   
        {%- set result = run_query(sql_statement).columns[0].values()[0] -%}
        transaction_date in ( select distinct transaction_date from {{ relation }} where last_updated_date >= date('{{ result }}'))
    {% endif %}
{% endmacro %}