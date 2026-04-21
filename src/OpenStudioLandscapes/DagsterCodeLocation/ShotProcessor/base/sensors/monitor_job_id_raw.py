from dagster import (
    asset_sensor,
    AssetKey,
    RunRequest,
    DefaultSensorStatus,
)

from OpenStudioLandscapes.DagsterCodeLocation.JobProcessor.dagster_job_processor.assets.read_yaml import (
    # ASSET_HEADER_JOB_PROCESSOR,
    ASSET_HEADER_JOB_PROCESSOR_DEADLINE,
)


asset_to_watch = AssetKey([*ASSET_HEADER_JOB_PROCESSOR_DEADLINE["key_prefix"], "job_id_raw"])

# Trigger `my_job` when the `daily_sales_data` asset is materialized
#
# Resources:
# - https://docs.dagster.io/guides/automate/asset-sensors#cross-job-and-cross-code-location-dependencies
@asset_sensor(
    asset_key=asset_to_watch,
    job_name="materialize_shot_processor_sub_jobs",
    minimum_interval_seconds=15,
    default_status=DefaultSensorStatus.STOPPED,
)
def trigger_shot_processor_sub_jobs():
    return RunRequest()
