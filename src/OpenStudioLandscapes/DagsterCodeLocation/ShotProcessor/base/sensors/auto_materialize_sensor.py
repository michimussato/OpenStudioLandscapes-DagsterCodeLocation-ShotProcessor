from dagster import (
    DefaultSensorStatus,
    AssetSelection,
    AutomationConditionSensorDefinition,
    # AssetKey,
)

# from OpenStudioLandscapes.DagsterCodeLocation.JobProcessor.dagster_job_processor.assets.read_yaml import (
#     # ASSET_HEADER_JOB_PROCESSOR,
#     ASSET_HEADER_JOB_PROCESSOR_DEADLINE,
# )

shot_processor_auto_materialize_sensor = AutomationConditionSensorDefinition(
    "ShotProcessor_AutoMaterializeSensor",
    target=AssetSelection.all(),
    minimum_interval_seconds=15,
    default_status=DefaultSensorStatus.STOPPED,
)
