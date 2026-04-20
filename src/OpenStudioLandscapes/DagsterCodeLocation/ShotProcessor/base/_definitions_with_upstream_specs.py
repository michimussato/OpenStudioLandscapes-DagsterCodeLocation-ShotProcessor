from dagster import (
    Definitions,
)

from OpenStudioLandscapes.DagsterCodeLocation.ShotProcessor.base.definitions import assets_base

from OpenStudioLandscapes.DagsterCodeLocation.JobProcessor.dagster_job_processor.assets.read_yaml import (
    output_format,
    CONFIG,
    render_output_directory,
    # submit_request_raw,
)

assets_external = []
assets_external.extend(output_format.specs)
assets_external.extend(CONFIG.specs)
assets_external.extend(render_output_directory.specs)
# assets_external.extend(submit_request_raw.specs)


defs = Definitions(
    assets=[
        *assets_base,
        *assets_external,
    ],
)
