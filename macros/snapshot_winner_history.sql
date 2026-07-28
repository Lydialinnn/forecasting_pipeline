{#
  snapshot_winner_history — appends the just-built forecast_model_winner to
  winner_history, tagged with the evaluation date + holdout setting.
  Called by run_pipeline.sh after each EVALUATION run (the winner table itself
  is overwritten every run, so this preserves the fold history for judging
  model consistency across evaluation cycles).
  Same-day rerun replaces that day's snapshot (no duplicates).

  Usage: dbt run-operation snapshot_winner_history --args '{holdout_weeks: 10}'
#}

{% macro snapshot_winner_history(holdout_weeks) %}

{% set create_sql %}
    create table if not exists {{ target.schema }}.winner_history as
    select
        current_date() as evaluation_run_date,
        cast(null as int64) as holdout_weeks,
        t.*
    from {{ target.schema }}.forecast_model_winner t
    where false
{% endset %}
{% do run_query(create_sql) %}

{% set delete_sql %}
    delete from {{ target.schema }}.winner_history
    where evaluation_run_date = current_date()
{% endset %}
{% do run_query(delete_sql) %}

{% set insert_sql %}
    insert into {{ target.schema }}.winner_history
    select current_date(), {{ holdout_weeks }}, t.*
    from {{ target.schema }}.forecast_model_winner t
{% endset %}
{% do run_query(insert_sql) %}

{{ log("winner_history: snapshot appended (holdout_weeks=" ~ holdout_weeks ~ ")", info=True) }}

{% endmacro %}
