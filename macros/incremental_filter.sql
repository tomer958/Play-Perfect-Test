{% macro incremental_filter(timestamp_field, comparison_field) %}
  {% if is_incremental() %}
    where {{ timestamp_field }} > (select max({{ comparison_field }}) from {{ this }})
  {% endif %}
{% endmacro %}