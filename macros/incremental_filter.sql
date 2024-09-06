{% macro incremental_filter(date_column) %}
  {% if is_incremental() %}
    AND {{ date_column }} >= (
      SELECT MAX({{ date_column }})
      FROM {{ this }}
    )
  {% endif %}
{% endmacro %}