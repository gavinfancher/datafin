from dagster import (
    schedule,
    RunRequest,
    ScheduleEvaluationContext
)


from ..jobs.spy_second_aggs_job import spy_second_agg_job

@schedule(
    job=spy_second_agg_job,
    cron_schedule="* * * * *",
    execution_timezone="America/New_York"
)
def spy_every_second(context: ScheduleEvaluationContext):
    """test 1"""
    
    return RunRequest(
        run_key=f"morning_spy_{context.scheduled_execution_time.strftime('%Y%m%d')}",
        tags={
            "schedule": "morning",
            "market_session": "open",
            "date": context.scheduled_execution_time.strftime('%Y-%m-%d')
        }
    )
