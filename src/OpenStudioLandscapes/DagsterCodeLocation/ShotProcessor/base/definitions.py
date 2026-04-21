from typing import List

from dagster import (
    Definitions,
    load_assets_from_modules,
    # AutoMaterializePolicy,
    # AutoMaterializeRule,
)


# Assets
import OpenStudioLandscapes.DagsterCodeLocation.ShotProcessor.base.assets
assets_base = load_assets_from_modules(
    modules=[OpenStudioLandscapes.DagsterCodeLocation.ShotProcessor.base.assets],
    # auto_materialize_policy=AutoMaterializePolicy.lazy().with_rules(
    #         AutoMaterializeRule.materialize_on_parent_updated(),
    # ),
)


# Sensors
from OpenStudioLandscapes.DagsterCodeLocation.ShotProcessor.base.sensors.monitor_job_id_raw import trigger_shot_processor_sub_jobs
# from OpenStudioLandscapes.DagsterCodeLocation.ShotProcessor.base.sensors.auto_materialize_sensor import shot_processor_auto_materialize_sensor
sensors_base = [
    trigger_shot_processor_sub_jobs,
    # shot_processor_auto_materialize_sensor,
]


# Jobs
from OpenStudioLandscapes.DagsterCodeLocation.ShotProcessor.base.jobs.materialize_downstream import materialize_downstream_job
jobs_base = [
    materialize_downstream_job,
]

defs = Definitions(
    assets=[
        *assets_base,
        # *assets_external,
    ],
    sensors=[
        *sensors_base,
    ],
    jobs=[
        *jobs_base,
    ],
)
