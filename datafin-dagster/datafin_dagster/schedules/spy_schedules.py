

from dagster import schedule, RunRequest


from ..jobs.spy_open_aggs import polygon_spy_open_minute_to_s3_job

@schedule(
    job = polygon_spy_open_minute_to_s3_job,
    cron_schedule = '50 9 * * *',
    execution_timezone ='America/New_York'
)
def get_polygon_spy_open_schedule():
    """
    comment here
    """
    return [RunRequest(run_key=None)]








#think about below
"""
@schedule(
    job=full_market_minute_agg_job,
    cron_schedule='0 4 * * *',
    execution_timezone='America/New_York'
)
def daily_initially_whole_market_schedule(context: ScheduleEvaluationContext):
    `Simple version with boolean check`
    scheduled_date = context.scheduled_execution_time.date()
    
    # Single condition check
    should_run = (
        scheduled_date.weekday() < 5 and  # Not weekend
        not is_market_holiday(scheduled_date) and  # Not holiday
        your_other_criteria(scheduled_date)  # Your custom logic
    )
    
    if should_run:
        return RunRequest()
    else:
        context.log.info(f"Skipping execution for {scheduled_date}")
        return []


"""