from dagster import (
    Definitions,
)

from OpenStudioLandscapes.DagsterCodeLocation.ShotProcessor.base.definitions import assets_base

from OpenStudioLandscapes.DagsterCodeLocation.JobProcessor.dagster_job_processor.assets.read_yaml import (
    output_format,
    CONFIG,
    render_output_directory,
)

assets_external = []
assets_external.extend(output_format.specs)
assets_external.extend(CONFIG.specs)
assets_external.extend(render_output_directory.specs)


defs = Definitions(
    assets=[
        *assets_base,
        *assets_external,
    ],
)
