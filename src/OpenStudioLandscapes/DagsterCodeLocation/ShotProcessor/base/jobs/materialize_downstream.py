from dagster import (
    define_asset_job,
)

from OpenStudioLandscapes.DagsterCodeLocation.ShotProcessor.base.assets import CONFIG_OIIO

materialize_downstream_job = define_asset_job(
    name="materialize_shot_processor_sub_jobs",
    description="Materialize downstream shot processor sub jobs "
                "after the main render job has been submitted to "
                "the render farm successfully.",
    selection=[
        CONFIG_OIIO,
    ],
)
