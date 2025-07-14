{% macro calculate_engagement_score(watch_duration, completion_rate, is_binge) %}
    {%- set base_score = 50 -%}
    {%- set duration_bonus = watch_duration * 0.1 -%}
    {%- set completion_bonus = completion_rate * 0.3 -%}
    {%- set binge_bonus = is_binge * 10 -%}
    
    LEAST(100, {{ base_score }} + {{ duration_bonus }} + {{ completion_bonus }} + {{ binge_bonus }})
{% endmacro %} 