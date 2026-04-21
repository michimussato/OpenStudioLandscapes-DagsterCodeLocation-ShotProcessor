from dagster import (
    DefaultSensorStatus,
    AssetSelection,
    AutomationConditionSensorDefinition,
    # AssetKey,
)

from OpenStudioLandscapes.DagsterCodeLocation.ShotProcessor.base.assets import CONFIG_OIIO
from OpenStudioLandscapes.DagsterCodeLocation.ShotProcessor.definitions import assets_internal

# from OpenStudioLandscapes.DagsterCodeLocation.JobProcessor.dagster_job_processor.assets.read_yaml import (
#     # ASSET_HEADER_JOB_PROCESSOR,
#     ASSET_HEADER_JOB_PROCESSOR_DEADLINE,
# )

shot_processor_auto_materialize_sensor = AutomationConditionSensorDefinition(
    "ShotProcessor_AutoMaterializeSensor",
    target=AssetSelection.all(
        include_sources=False,
    ),  # - AssetSelection.assets(
    #     CONFIG_OIIO,
    # ),
    # target=AssetSelection.assets(
    #     *assets_internal,
    # ), # - AssetSelection.assets(
    # #     CONFIG_OIIO,
    # # ),
    minimum_interval_seconds=10,
    default_status=DefaultSensorStatus.RUNNING,
)
