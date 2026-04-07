import json
import os
import pathlib
import shlex
# import shutil
from typing import Generator, Any, Dict, List

import yaml
# from collections import namedtuple

from dagster import (
    AssetIn,
    AssetKey,
    asset,
    AssetMaterialization,
    AssetExecutionContext,
    # OpExecutionContext,
    Output,
    MetadataValue,
    # SourceAsset,
    # AssetSpec,
)

from OpenStudioLandscapes.DagsterCodeLocation.ShotProcessor.config.models import ConfigOIIO

from OpenStudioLandscapes.DagsterCodeLocation.JobProcessor.dagster_job_processor.config.models import DefaultConstants
from OpenStudioLandscapes.DagsterCodeLocation.JobProcessor.dagster_job_processor.assets.read_yaml import ASSET_HEADER_JOB_PROCESSOR, ASSET_HEADER_JOB_PROCESSOR_PREPROCESSOR_KITSU
from OpenStudioLandscapes.DagsterCodeLocation.JobProcessor.dagster_job_processor.assets.submit_jobs import ASSET_HEADER_JOB_SUBMITTER_DEADLINE
# from OpenStudioLandscapes.DagsterCodeLocation.ShotProcessor.definitions import ASSET_HEADER_JOB_SUBMITTER_DEADLINE
# from OpenStudioLandscapes.DagsterCodeLocation.JobProcessor.deadline_templates.jobs.job_base import Resolution


# from OpenStudioLandscapes.DagsterCodeLocation.ShotProcessor.api import run_shot_processor
from OpenStudioLandscapes.DagsterCodeLocation.ShotProcessor.api import process_image, _process_image
from OpenStudioLandscapes.DagsterCodeLocation.StreamingProcess import submit_cmds


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


@asset(
    **ASSET_HEADER_OIIO_PROCESSOR,
    # deps=[
    #     AssetKey([*ASSET_HEADER_JOB_SUBMITTER_DEADLINE["key_prefix"], "submit_job"]),  # Does not yet return anything (just returns MaterializeResult)
    # ],
    # ins={
    #     "get_kitsu_task_dict": AssetIn(
    #         AssetKey([*ASSET_HEADER_JOB_PROCESSOR_PREPROCESSOR_KITSU["key_prefix"], "get_kitsu_task_dict"]),
    #     ),
    # }
)
def CONFIG_OIIO(
        context: AssetExecutionContext,
        # get_kitsu_task_dict: Dict,
) -> Generator[Output[List[pathlib.Path]] | AssetMaterialization | Any, Any, None]:

    config_oiio: ConfigOIIO = ConfigOIIO()

    ###############
    # CONFIG_OIIO #
    ###############

    # output_name = "CONFIG_OIIO"

    yield Output(
        # output_name=output_name,
        value=config_oiio,
    )

    yield AssetMaterialization(
        # asset_key=context.asset_key_for_output(output_name),
        asset_key=context.asset_key,
        metadata={
            "__".join(
                # context.asset_key_for_output(output_name).path
                context.asset_key.path
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
def image_sequence(
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


@asset(
    **ASSET_HEADER_OIIO_PROCESSOR,
    deps=[
        AssetKey([*ASSET_HEADER_JOB_SUBMITTER_DEADLINE["key_prefix"], "submit_job"]),  # Does not yet return anything (just returns MaterializeResult)
    ],
    ins={
        # "submit_job": AssetIn(
        #     AssetKey([*ASSET_HEADER_JOB_SUBMITTER_DEADLINE["key_prefix"], "submit_job"])
        # ),
        # "render_output_directory": AssetIn(
        #     AssetKey([*ASSET_HEADER_JOB_PROCESSOR["key_prefix"], "render_output_directory"]),
        # ),
        # "output_format": AssetIn(
        #     AssetKey([*ASSET_HEADER_JOB_PROCESSOR["key_prefix"], "output_format"]),
        # ),
        "get_kitsu_task_dict": AssetIn(
            AssetKey([*ASSET_HEADER_JOB_PROCESSOR_PREPROCESSOR_KITSU["key_prefix"], "get_kitsu_task_dict"]),
        ),
        "version": AssetIn(
            AssetKey([*ASSET_HEADER_JOB_PROCESSOR["key_prefix"], "version"]),
        ),
        "render_version_directory": AssetIn(
            AssetKey([*ASSET_HEADER_JOB_PROCESSOR["key_prefix"], "render_version_directory"]),
        ),
        "CONFIG": AssetIn(
            AssetKey([*ASSET_HEADER_JOB_PROCESSOR["key_prefix"], "CONFIG"]),
        ),
        "image_sequence": AssetIn(
            AssetKey([*ASSET_HEADER_OIIO_PROCESSOR["key_prefix"], "image_sequence"]),
        ),
        "CONFIG_OIIO": AssetIn(
            AssetKey([*ASSET_HEADER_OIIO_PROCESSOR["key_prefix"], "CONFIG_OIIO"]),
        ),
    }
)
def raw_to_oiio(
        context: AssetExecutionContext,
        image_sequence: List[pathlib.Path],
        # render_version_directory: pathlib.Path,
        version: str,
        render_version_directory: pathlib.Path,
        # submit_job: str,
        # # batch_name: str,
        # # job_title_str: str,
        # job_title: str,
        # output_format: str,
        # render_output_directory: pathlib.Path,
        # render_output_filename: Dict,
        # frame_start_absolute: int,
        # frame_end_absolute: int,
        # # frames: str,
        # # props: List,
        # # job_model: JobBase,
        get_kitsu_task_dict: Dict,
        CONFIG: DefaultConstants,
        CONFIG_OIIO: ConfigOIIO,
) -> Generator[Output[pathlib.Path] | AssetMaterialization | Any, Any, None]:
    # Doesn't work:
    # for i in {1197..1254}; do exrinfo "/data/share/AWSPortalRoot1/out/Test Production/Shot/SH030/Rendering/061/4_1197-1254_4/raw/sh030_001.${i}.exr"; done
    # render_output_raw = pathlib.Path(render_output_directory / "raw" / render_output_filename["padding_bash_expansion"])

    # exrinfo "${BASE_DIR}/raw/sh030_001.${START_F}.exr"

    raw_out = render_version_directory / version / CONFIG.RENDER_RAW_OUT
    # kitsu_task_json: pathlib.Path = render_output_directory / "kitsu_task.json"
    # with open(kitsu_task_json, "r") as fr:
    #     kitsu_task_json_dict = json.load(fr)

    result = {
        ""
    }

    for image_ in image_sequence:
        processed = _process_image(
            image_filepath=image_,
            kitsu_task_dict=get_kitsu_task_dict,
            CONFIG_OIIO=CONFIG_OIIO,
            version=version,
            render_version_directory=render_version_directory,
            # # output_dir=output_dir,
            # text_border=CONFIG.text_border,
            # overlay_text_size_frame=CONFIG.overlay_text_size_frame,
            # text_spacing=CONFIG.text_spacing,
            # overlay_text_size_scaledown=
        )

    # context.log.debug(f"{kitsu_task_json_dict = }")
    context.log.debug(f"{get_kitsu_task_dict = }")
    context.log.debug(f"raw_out={raw_out}")

    # for root, dirs, files in os.walk(raw_out):
    #     for f in files:
    #         if f.endswith(f".{output_format}"):
    #             context.log.debug(f"raw_out={root}/{f}")

    # # job_title: str
    # # output_format: str
    # # render_output_directory: pathlib.Path
    # # render_output_filename: Dict
    # # frame_start_absolute: int
    # # frame_end_absolute: int
    # # CONFIG: DefaultConstants
    #
    # cmd_shot_processor = [
    #     "shot-processor",
    #     "-vv",
    #     "--exr-sequence-dir", raw_out.as_posix(),
    #     "--output-dir", raw_out.parent.joinpath("oiio").as_posix(),
    #     "--kitsu-task-json", kitsu_task_json.as_posix(),
    # ]
    #
    # tasks: List[List] = [
    #     [
    #         "ls",
    #         "-al",
    #         raw_out.as_posix(),
    #     ],
    #     [
    #         "shot-processor",
    #         "--help",
    #     ],
    # ]
    #
    # log_records: List[str] = submit_cmds(
    #     context=context,
    #     cmds=tasks,
    # )

    yield Output(None)

    yield AssetMaterialization(
        asset_key=context.asset_key,
        metadata={
            "__".join(context.asset_key.path): MetadataValue.md(f"```json\n{json.dumps(None)}\n```"),
            # "cmd_shot_processor": MetadataValue.path(shlex.join(cmd_shot_processor)),
        }
    )