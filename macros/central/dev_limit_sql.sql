{#
This macro limits the select to 10 rows to speed up local build times, if the dev limit is applied.
To disable this limit and build the entire model locally, add "--var 'dev_limit_enabled: false'" at the end of the CL build in dbt.

#}

{% macro dev_limit_sql(dev_limit_enabled) -%}


  {%- if var('dev_limit_enabled', default=true) -%}

    limit 10

  {%- else -%}

  -- do nothing and build full table

  {%- endif -%}

{%- endmacro %}