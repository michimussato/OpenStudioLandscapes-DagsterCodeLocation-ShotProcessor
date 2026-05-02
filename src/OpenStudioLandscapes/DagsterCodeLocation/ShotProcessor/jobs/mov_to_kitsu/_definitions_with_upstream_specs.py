from dagster import (
    Definitions,
)

from OpenStudioLandscapes.DagsterCodeLocation.ShotProcessor.jobs.mov_to_kitsu.definitions import assets_base
from OpenStudioLandscapes.DagsterCodeLocation.ShotProcessor.jobs.png_to_mov.assets import (
    submit_request_png_to_mov,
)

from OpenStudioLandscapes.DagsterCodeLocation.JobProcessor.dagster_job_processor.assets.read_yaml import (
    batch_name,
    job_title_str,
    calc_frames,
    read_job_yaml,
    render_output_filename,
    version,
    render_output_directory,
    CONFIG,
    submit_request_raw,
)

from OpenStudioLandscapes.DagsterCodeLocation.ShotProcessor.base.assets import (
    CONFIG_OIIO,
)


# Assets
assets_external = []

# base
assets_external.extend(CONFIG_OIIO.specs)

# JobProcessor
assets_external.extend(batch_name.specs)
assets_external.extend(job_title_str.specs)
assets_external.extend(calc_frames.specs)
assets_external.extend(read_job_yaml.specs)
assets_external.extend(render_output_filename.specs)
assets_external.extend(render_output_directory.specs)
assets_external.extend(version.specs)
assets_external.extend(CONFIG.specs)
assets_external.extend(submit_request_raw.specs)

# Parent Job
assets_external.extend(submit_request_png_to_mov.specs)


# Resources
from OpenStudioLandscapes.DagsterCodeLocation.ShotProcessor.jobs.mov_to_kitsu.definitions import resources

defs = Definitions(
    assets=[
        *assets_base,
        *assets_external,
    ],
    resources=resources,
)
