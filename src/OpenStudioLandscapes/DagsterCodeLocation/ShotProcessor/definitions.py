from dagster import (
    Definitions,
)

from OpenStudioLandscapes.DagsterCodeLocation.ShotProcessor.base.definitions import assets_base
from OpenStudioLandscapes.DagsterCodeLocation.ShotProcessor.jobs.text_overlay.definitions import assets_base as assets_text_overlay
from OpenStudioLandscapes.DagsterCodeLocation.ShotProcessor.jobs.handle_overlay.definitions import assets_base as assets_handle_overlay
from OpenStudioLandscapes.DagsterCodeLocation.ShotProcessor.jobs.exr_with_custom_metadata.definitions import assets_base as assets_exr_with_custom_metadata
from OpenStudioLandscapes.DagsterCodeLocation.ShotProcessor.jobs.exr_to_png.definitions import assets_base as assets_exr_to_png
from OpenStudioLandscapes.DagsterCodeLocation.ShotProcessor.jobs.png_to_mov.definitions import assets_base as assets_png_to_mov
from OpenStudioLandscapes.DagsterCodeLocation.ShotProcessor.jobs.mov_to_kitsu.definitions import assets_base as assets_mov_to_kitsu

from OpenStudioLandscapes.DagsterCodeLocation.JobProcessor.dagster_job_processor.assets.read_yaml import (
    batch_name,
    job_title_str,
    read_job_yaml,
    render_output_filename,
    version,
    render_output_directory,
    CONFIG,
    frames,
    output_format,
    submit_request_raw,
    fps,
)

# from OpenStudioLandscapes.DagsterCodeLocation.ShotProcessor.jobs.mov_to_kitsu.resources import KitsuResource

# from OpenStudioLandscapes.DagsterCodeLocation.JobProcessor.dagster_job_processor.assets.read_yaml import (
#     # output_format,
#     # CONFIG,
#     # render_output_directory,
# )

assets_internal = []
assets_internal.extend(assets_base)
assets_internal.extend(assets_exr_to_png)
assets_internal.extend(assets_exr_with_custom_metadata)
assets_internal.extend(assets_handle_overlay)
assets_internal.extend(assets_png_to_mov)
assets_internal.extend(assets_text_overlay)
assets_internal.extend(assets_mov_to_kitsu)

# Assets
assets_external = []

# base
assets_external.extend(CONFIG.specs)

# JobProcessor
assets_external.extend(read_job_yaml.specs)
assets_external.extend(version.specs)
assets_external.extend(render_output_filename.specs)
assets_external.extend(job_title_str.specs)
assets_external.extend(batch_name.specs)
assets_external.extend(output_format.specs)
assets_external.extend(CONFIG.specs)
assets_external.extend(frames.specs)
assets_external.extend(render_output_directory.specs)
assets_external.extend(submit_request_raw.specs)
assets_external.extend(fps.specs)

# all_sensors = [
#     shot_processor_auto_materialize_sensor,
# ]


# Sensors
from OpenStudioLandscapes.DagsterCodeLocation.ShotProcessor.base.definitions import sensors_base
from OpenStudioLandscapes.DagsterCodeLocation.ShotProcessor.sensors.auto_materialize_sensor import shot_processor_auto_materialize_sensor

# Jobs
from OpenStudioLandscapes.DagsterCodeLocation.ShotProcessor.base.definitions import jobs_base

# Resources
from OpenStudioLandscapes.DagsterCodeLocation.ShotProcessor.jobs.mov_to_kitsu.definitions import resources as resources_mov_to_kitsu


defs = Definitions(
    assets=[
        *assets_internal,
        *assets_external,
    ],
    sensors=[
        *sensors_base,  # Testing if this sensor is not needed
        shot_processor_auto_materialize_sensor,
    ],
    jobs=[
        *jobs_base,
    ],
    resources={
        **resources_mov_to_kitsu,
    },
)
