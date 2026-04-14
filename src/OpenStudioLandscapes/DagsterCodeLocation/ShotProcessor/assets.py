import json
import os
import pathlib
import re
from typing import Generator, Any, Dict, List

import yaml

from dagster import (
    AssetIn,
    AssetKey,
    AssetOut,
    asset,
    multi_asset,
    AssetMaterialization,
    AssetExecutionContext,
    Output,
    MetadataValue,
)

from OpenStudioLandscapes.DagsterCodeLocation.ShotProcessor.config.models import ConfigOIIO

from OpenStudioLandscapes.DagsterCodeLocation.JobProcessor.dagster_job_processor.config.models import DefaultConstants
from OpenStudioLandscapes.DagsterCodeLocation.JobProcessor.dagster_job_processor.assets.read_yaml import ASSET_HEADER_JOB_PROCESSOR, ASSET_HEADER_JOB_PROCESSOR_PREPROCESSOR_KITSU
from OpenStudioLandscapes.DagsterCodeLocation.JobProcessor.dagster_job_processor.assets.submit_jobs import ASSET_HEADER_JOB_SUBMITTER_DEADLINE


from OpenStudioLandscapes.DagsterCodeLocation.ShotProcessor.api import _process_image, _create_buf_from_raw


# Asset data across code locations:
# - [SourceAsset](https://stackoverflow.com/q/79780791)
# - [AssetSpec](https://release-1-8-9.dagster.dagster-docs.io/concepts/assets/external-assets)
# - [Asset obervations](https://release-1-8-9.dagster.dagster-docs.io/concepts/assets/asset-observations)


GROUP_OIIO_PROCESSOR = "OpenStudioLandscapes_DagsterCodeLocation_JobProcessor_OIIO_Processor"
# KEY_CONSTANTS_DEFAULT = [GROUP_CONSTANTS_DEFAULT, "Constants"]
KEY_OIIO_PROCESSOR = [GROUP_OIIO_PROCESSOR]

ASSET_HEADER_OIIO_PROCESSOR = {
    "group_name": GROUP_OIIO_PROCESSOR,
    "key_prefix": KEY_OIIO_PROCESSOR,
}


@multi_asset(
    # **ASSET_HEADER_OIIO_PROCESSOR,
    outs={
        # "env": AssetOut.from_spec(env_spec),
        "CONFIG_OIIO": AssetOut(
            **ASSET_HEADER_OIIO_PROCESSOR,
            # dagster_type=ConfigOIIO,
            description="Todo",
        ),
    },
)
def CONFIG_OIIO(
        context: AssetExecutionContext,
) -> Generator[Output[List[pathlib.Path]] | AssetMaterialization | Any, Any, None]:

    config_oiio: ConfigOIIO = ConfigOIIO()

    ###############
    # CONFIG_OIIO #
    ###############

    output_name = "CONFIG_OIIO"

    yield Output(
        output_name=output_name,
        value=config_oiio,
    )

    yield AssetMaterialization(
        asset_key=context.asset_key_for_output(output_name),
        metadata={
            "__".join(
                context.asset_key_for_output(output_name).path
            ): MetadataValue.md(
                f"```yaml\n{yaml.safe_dump(json.loads(config_oiio.model_dump_json(fallback=str, indent=2)))}\n```"
            ),
        },
    )


@asset(
    **ASSET_HEADER_OIIO_PROCESSOR,
    deps=[
        AssetKey([*ASSET_HEADER_JOB_SUBMITTER_DEADLINE["key_prefix"], "submit_job"]),  # Does not yet return anything (just returns MaterializeResult)
    ],
    ins={
        "render_version_directory": AssetIn(
            AssetKey([*ASSET_HEADER_JOB_PROCESSOR["key_prefix"], "render_version_directory"]),
        ),
        "output_format": AssetIn(
            AssetKey([*ASSET_HEADER_JOB_PROCESSOR["key_prefix"], "output_format"]),
        ),
        "version": AssetIn(
            AssetKey([*ASSET_HEADER_JOB_PROCESSOR["key_prefix"], "version"]),
        ),
        "CONFIG": AssetIn(
            AssetKey([*ASSET_HEADER_JOB_PROCESSOR["key_prefix"], "CONFIG"]),
        ),
    }
)
def image_sequence_raw(
        context: AssetExecutionContext,
        render_version_directory: pathlib.Path,
        output_format: str,
        version: str,
        CONFIG: DefaultConstants,
) -> Generator[Output[List[pathlib.Path]] | AssetMaterialization | Any, Any, None]:

    sequence_dir: pathlib.Path = render_version_directory.joinpath(version, CONFIG.RENDER_RAW_OUT)

    ret = []

    for root, dirs, files in os.walk(sequence_dir):
        # sort:
        # - [](https://stackoverflow.com/a/18282401)
        for dir_ in sorted(dirs):
            context.log.debug("Processing directory %s", dir_)
        for file_ in sorted(files):
            filepath = pathlib.Path(root, file_)
            context.log.debug("Processing file: %s", filepath)
            if file_.endswith(f".{output_format}"):
                context.log.debug("Appending file to list: %s", filepath)
                ret.append(filepath)
            else:
                context.log.info(f"Skipping file because it does not have extension `.{output_format}`: %s", filepath)

    yield Output(ret)

    yield AssetMaterialization(
        asset_key=context.asset_key,
        metadata={
            "__".join(context.asset_key.path): MetadataValue.md(
                f"```json\n{json.dumps(ret, indent=2, default=str)}\n```"
            ),
        }
    )


@multi_asset(
    outs={
        "raw_to_oiio": AssetOut(
            **ASSET_HEADER_OIIO_PROCESSOR,
            description="Todo",
        ),
    },
    deps=[
        AssetKey([*ASSET_HEADER_JOB_SUBMITTER_DEADLINE["key_prefix"], "submit_job"]),  # Does not yet return anything (just returns MaterializeResult)
    ],
    ins={
        "get_kitsu_task_dict": AssetIn(
            AssetKey([*ASSET_HEADER_JOB_PROCESSOR_PREPROCESSOR_KITSU["key_prefix"], "get_kitsu_task_dict"]),
        ),
        "version": AssetIn(
            AssetKey([*ASSET_HEADER_JOB_PROCESSOR["key_prefix"], "version"]),
        ),
        "render_version_directory": AssetIn(
            AssetKey([*ASSET_HEADER_JOB_PROCESSOR["key_prefix"], "render_version_directory"]),
        ),
        "image_sequence_raw": AssetIn(
            AssetKey([*ASSET_HEADER_OIIO_PROCESSOR["key_prefix"], "image_sequence_raw"]),
        ),
        "CONFIG_OIIO": AssetIn(
            AssetKey([*ASSET_HEADER_OIIO_PROCESSOR["key_prefix"], "CONFIG_OIIO"]),
        ),
    }
)
def raw_to_oiio(
        context: AssetExecutionContext,
        image_sequence_raw: List[pathlib.Path],
        version: str,
        render_version_directory: pathlib.Path,
        get_kitsu_task_dict: Dict,
        CONFIG_OIIO: ConfigOIIO,
) -> Generator[Output[pathlib.Path] | AssetMaterialization | Any, Any, None]:
    # Doesn't work:
    # for i in {1197..1254}; do exrinfo "/data/share/AWSPortalRoot1/out/Test Production/Shot/SH030/Rendering/061/4_1197-1254_4/raw/sh030_001.${i}.exr"; done
    # render_output_raw = pathlib.Path(render_output_directory / "raw" / render_output_filename["padding_bash_expansion"])

    # exrinfo "${BASE_DIR}/raw/sh030_001.${START_F}.exr"

    results = []

    output_dir: pathlib.Path = render_version_directory.joinpath(
        version,
        "oiio",
    )
    output_dir.mkdir(parents=True, exist_ok=True)

    for image_ in image_sequence_raw:
        context.log.debug("Processing image %s", image_)

        # Frame number based on image name (image.0123.png)
        f_no_ = re.findall(
            r"\.[0-9]+\.",
            image_.name
        )

        if bool(f_no_):
            f_no = int(f_no_[-1].replace(".", ""))
        else:
            f_no = 0

        context.log.debug(f"Frame number: {f_no}")

        raw_buf, raw_spec = _create_buf_from_raw(
            raw=image_
        )

        processed_result = _process_image(
            # raw_buf=raw_buf,
            raw_spec=raw_spec,
            context=context,
            image_filepath=image_,
            frame_number=f_no,
            kitsu_task_dict=get_kitsu_task_dict,
            CONFIG_OIIO=CONFIG_OIIO,
            version=version,
            output_dir=output_dir,
            create_exr_from_raw_with_custom_metadata=True,
            create_text_overlay=True,
            create_handle_overlay=True,
        )
        context.log.debug(f"{processed_result = }")
        results.append(processed_result)

    ###############
    # raw_to_oiio #
    ###############

    output_name = "raw_to_oiio"

    yield Output(
        output_name=output_name,
        value=results,
    )

    yield AssetMaterialization(
        asset_key=context.asset_key_for_output(output_name),
        metadata={
            "__".join(
                context.asset_key_for_output(output_name).path
            ): MetadataValue.md(
                f"```json\n{json.dumps(results, indent=2, default=str)}\n```"
            ),
        }
    )
