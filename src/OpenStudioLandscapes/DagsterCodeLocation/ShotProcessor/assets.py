import json
import os
import pathlib
# import shlex
# import shutil
from typing import Generator, Any, Dict, List
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

from OpenStudioLandscapes.DagsterCodeLocation.JobProcessor.dagster_job_processor.config.models import DefaultConstants
from OpenStudioLandscapes.DagsterCodeLocation.JobProcessor.dagster_job_processor.assets.read_yaml import ASSET_HEADER_JOB_PROCESSOR, ASSET_HEADER_JOB_PROCESSOR_PREPROCESSOR_KITSU
from OpenStudioLandscapes.DagsterCodeLocation.JobProcessor.dagster_job_processor.assets.submit_jobs import ASSET_HEADER_JOB_SUBMITTER_DEADLINE
# from OpenStudioLandscapes.DagsterCodeLocation.ShotProcessor.definitions import ASSET_HEADER_JOB_SUBMITTER_DEADLINE
# from OpenStudioLandscapes.DagsterCodeLocation.JobProcessor.deadline_templates.jobs.job_base import Resolution


# from OpenStudioLandscapes.DagsterCodeLocation.ShotProcessor.api import run_shot_processor
from OpenStudioLandscapes.DagsterCodeLocation.StreamingProcess import submit_cmds


# Asset data across code locations:
# - [SourceAsset](https://stackoverflow.com/q/79780791)
# - [AssetSpec](https://release-1-8-9.dagster.dagster-docs.io/concepts/assets/external-assets)
# - [Asset overvations](https://release-1-8-9.dagster.dagster-docs.io/concepts/assets/asset-observations)

@asset(
    **ASSET_HEADER_JOB_PROCESSOR,
    deps=[
        AssetKey([*ASSET_HEADER_JOB_PROCESSOR["key_prefix"], "job_title"]),
        # AssetKey([*ASSET_HEADER_JOB_PROCESSOR["key_prefix"], "output_format"]),
        # AssetKey([*ASSET_HEADER_JOB_PROCESSOR["key_prefix"], "render_output_directory"]),
        ## AssetKey([*ASSET_HEADER_JOB_PROCESSOR["key_prefix"], "render_output_filename"]),
        # AssetKey([*ASSET_HEADER_JOB_PROCESSOR["key_prefix"], "frame_start_absolute"]),
        # AssetKey([*ASSET_HEADER_JOB_PROCESSOR["key_prefix"], "frame_end_absolute"]),
        # AssetKey([*ASSET_HEADER_JOB_PROCESSOR["key_prefix"], "CONFIG"]),
        AssetKey([*ASSET_HEADER_JOB_SUBMITTER_DEADLINE["key_prefix"], "submit_job"]),  # Does not yet return anything (just returns MaterializeResult)
    ],
    ins={
        # "submit_job": AssetIn(
        #     AssetKey([*ASSET_HEADER_JOB_SUBMITTER_DEADLINE["key_prefix"], "submit_job"])
        # ),
        "render_output_directory": AssetIn(
            AssetKey([*ASSET_HEADER_JOB_PROCESSOR["key_prefix"], "render_output_directory"]),
        ),
        "output_format": AssetIn(
            AssetKey([*ASSET_HEADER_JOB_PROCESSOR["key_prefix"], "output_format"]),
        ),
        "get_kitsu_task_dict": AssetIn(
            AssetKey([*ASSET_HEADER_JOB_PROCESSOR_PREPROCESSOR_KITSU["key_prefix"], "get_kitsu_task_dict"]),
        ),
        "CONFIG": AssetIn(
            AssetKey([*ASSET_HEADER_JOB_SUBMITTER_DEADLINE["key_prefix"], "submit_job"]),
        ),
    }
)
def raw_to_oiio(
        context: AssetExecutionContext,
        # submit_job: str,
        # # batch_name: str,
        # # job_title_str: str,
        # job_title: str,
        output_format: str,
        render_output_directory: pathlib.Path,
        # render_output_filename: Dict,
        # frame_start_absolute: int,
        # frame_end_absolute: int,
        # # frames: str,
        # # props: List,
        # # job_model: JobBase,
        get_kitsu_task_dict: Dict,
        CONFIG: DefaultConstants,
) -> Generator[Output[pathlib.Path] | AssetMaterialization | Any, Any, None]:
    # Doesn't work:
    # for i in {1197..1254}; do exrinfo "/data/share/AWSPortalRoot1/out/Test Production/Shot/SH030/Rendering/061/4_1197-1254_4/raw/sh030_001.${i}.exr"; done
    # render_output_raw = pathlib.Path(render_output_directory / "raw" / render_output_filename["padding_bash_expansion"])

    # exrinfo "${BASE_DIR}/raw/sh030_001.${START_F}.exr"

    raw_out = render_output_directory / CONFIG.RENDER_RAW_OUT
    kitsu_task_json: pathlib.Path = render_output_directory / "kitsu_task.json"
    with open(kitsu_task_json, "r") as fr:
        kitsu_task_json_dict = json.load(fr)

    context.log.debug(f"{kitsu_task_json_dict = }")
    context.log.debug(f"{get_kitsu_task_dict = }")
    context.log.debug(f"raw_out={raw_out}")

    for root, dirs, files in os.walk(raw_out):
        for f in files:
            if f.endswith(f".{output_format}"):
                context.log.debug(f"raw_out={root}/{f}")

    # job_title: str
    # output_format: str
    # render_output_directory: pathlib.Path
    # render_output_filename: Dict
    # frame_start_absolute: int
    # frame_end_absolute: int
    # CONFIG: DefaultConstants

    tasks: List[List] = [
        [
            "ls",
            "-al",
            raw_out.as_posix(),
        ],
        [
            "shot-processor",
            "--help",
        ],
    ]

    log_records: List[str] = submit_cmds(
        context=context,
        cmds=tasks,
    )

    yield Output(log_records)

    yield AssetMaterialization(
        asset_key=context.asset_key,
        metadata={
            "__".join(context.asset_key.path): MetadataValue.md(f"```json\n{json.dumps(log_records)}\n```"),
        }
    )