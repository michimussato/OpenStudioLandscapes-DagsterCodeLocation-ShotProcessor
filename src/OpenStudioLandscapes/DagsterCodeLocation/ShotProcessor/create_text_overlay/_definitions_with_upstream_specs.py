from dagster import (
    Definitions,
)

from OpenStudioLandscapes.DagsterCodeLocation.ShotProcessor.create_text_overlay.definitions import assets_base

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

defs = Definitions(
    assets=[
        *assets_base,
        *assets_external,
    ],
)
