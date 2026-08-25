{% macro cents_to_amount(column_name) %}

    ({{ column_name }} / 100.0)

{% endmacro %}