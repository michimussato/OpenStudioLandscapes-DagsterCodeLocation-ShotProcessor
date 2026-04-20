from dagster import (
    Definitions,
)

from OpenStudioLandscapes.DagsterCodeLocation.ShotProcessor.jobs.png_to_mov.definitions import assets_base
from OpenStudioLandscapes.DagsterCodeLocation.ShotProcessor.jobs.exr_to_png.assets import (
    submit_request_exr_to_png,
)

from OpenStudioLandscapes.DagsterCodeLocation.JobProcessor.dagster_job_processor.assets.read_yaml import (
    batch_name,
    job_title_str,
    frames,
    read_job_yaml,
    render_output_filename,
    version,
    render_output_directory,
    CONFIG,
    submit_request_raw,
    fps,
)

from OpenStudioLandscapes.DagsterCodeLocation.ShotProcessor.base.assets import (
    CONFIG_OIIO,
)

assets_external = []

# base
assets_external.extend(CONFIG_OIIO.specs)

# JobProcessor
assets_external.extend(batch_name.specs)
assets_external.extend(job_title_str.specs)
assets_external.extend(frames.specs)
assets_external.extend(read_job_yaml.specs)
assets_external.extend(render_output_filename.specs)
assets_external.extend(render_output_directory.specs)
assets_external.extend(version.specs)
assets_external.extend(CONFIG.specs)
assets_external.extend(submit_request_raw.specs)
assets_external.extend(fps.specs)

# Parent Job
assets_external.extend(submit_request_exr_to_png.specs)

defs = Definitions(
    assets=[
        *assets_base,
        *assets_external,
    ],
)
