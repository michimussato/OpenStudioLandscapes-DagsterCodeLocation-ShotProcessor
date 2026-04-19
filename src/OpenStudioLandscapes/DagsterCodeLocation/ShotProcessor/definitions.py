from dagster import (
    Definitions,
)

from OpenStudioLandscapes.DagsterCodeLocation.ShotProcessor.base.definitions import assets_base
from OpenStudioLandscapes.DagsterCodeLocation.ShotProcessor.jobs.text_overlay.definitions import assets_base as assets_text_overlay
from OpenStudioLandscapes.DagsterCodeLocation.ShotProcessor.jobs.handle_overlay.definitions import assets_base as assets_handle_overlay
from OpenStudioLandscapes.DagsterCodeLocation.ShotProcessor.jobs.exr_with_custom_metadata.definitions import assets_base as assets_exr_with_custom_metadata

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
)

# from OpenStudioLandscapes.DagsterCodeLocation.JobProcessor.dagster_job_processor.assets.read_yaml import (
#     # output_format,
#     # CONFIG,
#     # render_output_directory,
# )

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


defs = Definitions(
    assets=[
        *assets_base,
        *assets_text_overlay,
        *assets_handle_overlay,
        *assets_exr_with_custom_metadata,
        *assets_external,
    ],
)
