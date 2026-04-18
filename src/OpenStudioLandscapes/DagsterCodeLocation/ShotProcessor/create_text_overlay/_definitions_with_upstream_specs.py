from dagster import (
    Definitions,
)

from OpenStudioLandscapes.DagsterCodeLocation.ShotProcessor.create_text_overlay.definitions import assets_base

from OpenStudioLandscapes.DagsterCodeLocation.JobProcessor.dagster_job_processor.assets.read_yaml import (
    batch_name,
    job_title_str,
    read_job_yaml,
    render_output_filename,
    version,
    render_output_directory,
    CONFIG,
)

from OpenStudioLandscapes.DagsterCodeLocation.ShotProcessor.base.assets import (
    CONFIG_OIIO,
)
# from OpenStudioLandscapes.DagsterCodeLocation.ShotProcessor.png_to_mov.definitions import render_output_directory

# from OpenStudioLandscapes.DagsterCodeLocation.ShotProcessor.external_assets import assets_external

assets_external = []
assets_external.extend(batch_name.specs)
assets_external.extend(job_title_str.specs)
assets_external.extend(read_job_yaml.specs)
assets_external.extend(render_output_filename.specs)
assets_external.extend(render_output_directory.specs)
assets_external.extend(version.specs)
assets_external.extend(CONFIG_OIIO.specs)
assets_external.extend(CONFIG.specs)


defs = Definitions(
    assets=[
        *assets_base,
        *assets_external,
    ],
)
