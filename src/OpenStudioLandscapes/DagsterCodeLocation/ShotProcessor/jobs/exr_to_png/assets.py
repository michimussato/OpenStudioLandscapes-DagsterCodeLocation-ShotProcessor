import pathlib
from types import NoneType

import yaml
import json
import shlex
import requests
from typing import (
    List,
    Generator,
    Any,
    Dict,
)

from dagster import (
    AssetOut,
    AssetIn,
    AssetKey,
    multi_asset,
    AssetExecutionContext,
    Output,
    asset,
    AssetMaterialization,
    MetadataValue,
)

# from OpenStudioLandscapes.DagsterCodeLocation.ShotProcessor.config.models import ConfigOIIO
from OpenStudioLandscapes.DagsterCodeLocation.JobProcessor.deadline_templates.jobs import models_submission
from OpenStudioLandscapes.DagsterCodeLocation.JobProcessor.dagster_job_processor.config.models import DefaultConstants

from OpenStudioLandscapes.DagsterCodeLocation.JobProcessor.deadline_templates.jobs.job_base import JobBase

from OpenStudioLandscapes.DagsterCodeLocation.JobProcessor.dagster_job_processor.assets.read_yaml import (
    ASSET_HEADER_JOB_PROCESSOR_DEADLINE,
    ASSET_HEADER_JOB_PROCESSOR_READER,
)

from OpenStudioLandscapes.DagsterCodeLocation.ShotProcessor.base.assets import (
    ASSET_HEADER_JOB_PROCESSOR,
    ASSET_HEADER_OIIO_PROCESSOR,
)


JOB = "exr_to_png"


GROUP_OIIO_PROCESSOR_EXR_TO_PNG = f"OpenStudioLandscapes_DagsterCodeLocation_ShotProcessor_OIIO_Processor_{JOB}"
# KEY_CONSTANTS_DEFAULT = [GROUP_CONSTANTS_DEFAULT, "Constants"]
KEY_OIIO_PROCESSOR_EXR_TO_PNG = [GROUP_OIIO_PROCESSOR_EXR_TO_PNG]

ASSET_HEADER_OIIO_PROCESSOR_EXR_TO_PNG = {
    "group_name": GROUP_OIIO_PROCESSOR_EXR_TO_PNG,
    "key_prefix": KEY_OIIO_PROCESSOR_EXR_TO_PNG,
}


@multi_asset(
    outs={
        "cmd": AssetOut(
            **ASSET_HEADER_OIIO_PROCESSOR_EXR_TO_PNG,
            dagster_type=List,
            description="Todo",
        ),
        "Deadline_OutputDirectory": AssetOut(
            **ASSET_HEADER_OIIO_PROCESSOR_EXR_TO_PNG,
            dagster_type=pathlib.Path,
            description="Todo",
        ),
        "Deadline_OutputFilename": AssetOut(
            **ASSET_HEADER_OIIO_PROCESSOR_EXR_TO_PNG,
            dagster_type=NoneType,
            description="Todo",
        ),
    },
    ins={
        "render_output_directory": AssetIn(
            AssetKey([*ASSET_HEADER_JOB_PROCESSOR["key_prefix"], "render_output_directory"]),
        ),
        "render_output_filename": AssetIn(
            AssetKey([*ASSET_HEADER_JOB_PROCESSOR["key_prefix"], "render_output_filename"]),
        ),
        "CONFIG_OIIO_YAML": AssetIn(
            AssetKey([*ASSET_HEADER_OIIO_PROCESSOR["key_prefix"], "CONFIG_OIIO_YAML"]),
        ),
        "CONFIG": AssetIn(
            AssetKey([*ASSET_HEADER_JOB_PROCESSOR["key_prefix"], "CONFIG"]),
        ),
    }
)
def raw_to_png(
        context: AssetExecutionContext,
        render_output_directory: pathlib.Path,
        render_output_filename: Dict,
        CONFIG: DefaultConstants,
) -> Generator[Output[List] | AssetMaterialization | Any, Any, None]:
    # https://stackoverflow.com/questions/24961127/how-to-create-a-video-from-images-with-ffmpeg
    # https://www.ffmpeg.media/articles/image-sequences-timelapse-photos-to-video

    input_dir: pathlib.Path = render_output_directory.joinpath(
        CONFIG.RENDER_RAW_OUT,
    )

    output_dir: pathlib.Path = render_output_directory.joinpath(
        CONFIG.OIIO_BASE_OUT,
        CONFIG.OIIO_RAW_TO_PNG,
    )
    output_dir.mkdir(parents=True, exist_ok=True)

    exr_in: pathlib.Path = input_dir.joinpath(render_output_filename["padding_deadline_batch_startframe"])
    png_out: pathlib.Path = output_dir.joinpath(exr_in.stem + ".png")

    # Todo:
    #  - [ ] add in timestamp
    #  - [ ] add out timestamp
    cmd_oiiotool: List[str] = [
        "oiiotool",
        "-i", exr_in.as_posix(),
        "--create-dir",
        "-o", png_out.as_posix(),
    ]
    # <QUOTE> results in "
    # Results in:
    # oiiotool -i '/raw/vivi_025.<STARTFRAME%4>.exr' --create-dir -o '/oiio/oiio_png/vivi_025.<STARTFRAME%4>.png'
    # oiiotool -i '/data/share/AWSPortalRoot1/out/Test Production/Shot/SH040/Rendering/133/raw/vivi_025.1017.exr' --create-dir -o '/data/share/AWSPortalRoot1/out/Test Production/Shot/SH040/Rendering/133/oiio/oiio_png/vivi_025.1017.png'

    #######
    # cmd #
    #######

    output_name = "cmd"

    yield Output(
        output_name=output_name,
        value=cmd_oiiotool,
    )

    yield AssetMaterialization(
        asset_key=context.asset_key_for_output(output_name),
        metadata={
            "__".join(
                context.asset_key_for_output(output_name).path
            ): MetadataValue.json(cmd_oiiotool),
            "cmd_": MetadataValue.path(shlex.join(cmd_oiiotool)),
        }
    )

    ############################
    # Deadline_OutputDirectory #
    ############################

    output_name = "Deadline_OutputDirectory"

    yield Output(
        output_name=output_name,
        value=output_dir,
    )

    yield AssetMaterialization(
        asset_key=context.asset_key_for_output(output_name),
        metadata={
            "__".join(
                context.asset_key_for_output(output_name).path
            ): MetadataValue.path(output_dir),
            "input_dir": MetadataValue.path(input_dir),
        }
    )

    ###########################
    # Deadline_OutputFilename #
    ###########################

    output_name = "Deadline_OutputFilename"

    yield Output(
        output_name=output_name,
        value=None,
    )

    yield AssetMaterialization(
        asset_key=context.asset_key_for_output(output_name),
        metadata={
            "__".join(
                context.asset_key_for_output(output_name).path
            ): MetadataValue.null(),
            # "ffmpeg_out": MetadataValue.path(ffmpeg_out),
        }
    )


@multi_asset(
    outs={
        "job_info_model": AssetOut(
            **ASSET_HEADER_OIIO_PROCESSOR_EXR_TO_PNG,
            dagster_type=models_submission.JobInfo,
            description="",
        ),
    },
    ins={
        "batch_name": AssetIn(
            AssetKey([*ASSET_HEADER_JOB_PROCESSOR["key_prefix"], "batch_name"])
        ),
        "job_title_str": AssetIn(
            AssetKey([*ASSET_HEADER_JOB_PROCESSOR["key_prefix"], "job_title_str"])
        ),
        "render_output_directory": AssetIn(
            AssetKey([*ASSET_HEADER_JOB_PROCESSOR["key_prefix"], "render_output_directory"])
        ),
        "frames": AssetIn(
            AssetKey([*ASSET_HEADER_JOB_PROCESSOR["key_prefix"], "frames"])
        ),
        # "render_output_filename": AssetIn(
        #     AssetKey([*ASSET_HEADER_JOB_PROCESSOR["key_prefix"], "render_output_filename"])
        # ),
        "job_model": AssetIn(
            AssetKey([*ASSET_HEADER_JOB_PROCESSOR_READER["key_prefix"], "read_job_yaml"])
        ),
        "job_id_raw": AssetIn(
            AssetKey([*ASSET_HEADER_JOB_PROCESSOR_DEADLINE["key_prefix"], "job_id_raw"]),
        ),
        "Deadline_OutputDirectory": AssetIn(
            AssetKey([*ASSET_HEADER_OIIO_PROCESSOR_EXR_TO_PNG["key_prefix"], "Deadline_OutputDirectory"]),
        ),
        # "Deadline_OutputFilename": AssetIn(
        #     AssetKey([*ASSET_HEADER_OIIO_PROCESSOR_CREATE_TEXT_OVERLAY["key_prefix"], "Deadline_OutputFilename"]),
        # ),
    }
)
def job_info_exr_to_png(
        context: AssetExecutionContext,
        batch_name: str,
        job_title_str: str,
        render_output_directory: pathlib.Path,
        frames: str,
        # render_output_filename: Dict,
        job_model: JobBase,
        job_id_raw: str,
        Deadline_OutputDirectory: pathlib.Path,
        # Deadline_OutputFilename: str,
) -> Generator[Output[pathlib.Path] | AssetMaterialization | Any, Any, None]:

    job_id_parent = job_id_raw

    # https://docs.thinkboxsoftware.com/products/deadline/10.2/1_User%20Manual/manual/manual-submission.html#job-info-file-options
    # render_output_directory.mkdir(parents=True, exist_ok=True)
    path = render_output_directory / "jobinfo_info.txt"

    context.log.debug(f"{path = }")

    job_info_dict = {
        "Plugin": models_submission.DeadlinePlugins.CommandLine.value,
        # create_text_overlay is a single task
        "Frames": frames,
        "Name": f"{job_title_str} - {JOB}",
        "Comment": job_model.comment,
        # "Department"
        "BatchName": batch_name,
        "UserName": job_model.deadline_config.user,
        "MachineName": job_model.deadline_config.host,
        # "Pool"
        # "SecondaryPool"
        # "Group"
        "Priority": job_model.job_priority + 1,
        "ChunkSize": 1,
        # "ConcurrentTasks"
        # "LimitConcurrentTasksToNumberOfCpus"
        # "OnJobComplete"
        # "SynchronizeAllAuxiliaryFiles"
        "ForceReloadPlugin": True,
        "JobDependencyPercentage": 100,
        "ResumeOnCompleteDependencies": True,
        # "Sequential"
        # "SuppressEvents"
        # "Protected"
        "InitialStatus": job_model.deadline_initial_status,
        "JobDependencies": [job_id_parent],
        # "StartupDirectory"
        # Todo:
        #  - [ ] integrate these into model (not being used so far)
        "OutputDirectory0": Deadline_OutputDirectory.as_posix(),
        # "OutputFilename0": Deadline_OutputFilename,
    }

    job_info = models_submission.JobInfo(
        **job_info_dict,
    )

    context.log.debug(f"{job_info = }")

    output_name = "job_info_model"

    yield Output(
        output_name=output_name,
        value=job_info,
    )

    yield AssetMaterialization(
        asset_key=context.asset_key_for_output(output_name),
        metadata={
            "job_info_model_yaml": MetadataValue.md(
                f"```yaml\n{yaml.safe_dump(json.loads(job_info.model_dump_json(indent=2, fallback=str)))}\n```"
            ),
        }
    )


@multi_asset(
    outs={
        "plugin_info_model": AssetOut(
            **ASSET_HEADER_OIIO_PROCESSOR_EXR_TO_PNG,
            dagster_type=models_submission.CommandLinePluginInfo,
            description="",
        ),
    },
    ins={
        "render_output_directory": AssetIn(
            AssetKey([*ASSET_HEADER_JOB_PROCESSOR["key_prefix"], "render_output_directory"])
        ),
        # "render_arguments": AssetIn(
        #     AssetKey([*ASSET_HEADER_JOB_PROCESSOR["key_prefix"], "render_arguments"])
        # ),
        # "job_model": AssetIn(
        #     AssetKey([*ASSET_HEADER_JOB_PROCESSOR_READER["key_prefix"], "read_job_yaml"])
        # ),
        "cmd": AssetIn(
            AssetKey([*ASSET_HEADER_OIIO_PROCESSOR_EXR_TO_PNG["key_prefix"], "cmd"])
        ),
    }
)
def plugin_info_text_overlay(
        context: AssetExecutionContext,
        render_output_directory: pathlib.Path,
        # render_arguments: str,
        # job_model: JobBase,
        cmd: List,
) -> Generator[Output[pathlib.Path] | AssetMaterialization | Any, Any, None]:

    # https://docs.thinkboxsoftware.com/products/deadline/10.2/1_User%20Manual/manual/manual-submission.html#plug-in-info-file
    # render_output_directory.mkdir(parents=True, exist_ok=True)
    path = pathlib.Path(f"{render_output_directory}/plugin_info.txt")

    context.log.debug(f"{path = }")

    SHELL = [
        "/bin/bash",
    ]

    plugin_info_dict = {
        "Executable": SHELL[0],
        "Arguments": f'-c "{shlex.join(cmd)}"',
        # "Arguments": f"-c <QUOTE>{shlex.join(cmd_create_text_overlay)}<QUOTE>",
    }

    context.log.debug(f"{plugin_info_dict = }")

    plugin_info = models_submission.CommandLinePluginInfo(
        **plugin_info_dict,
    )

    context.log.debug(f"{plugin_info = }")

    output_name = "plugin_info_model"

    yield Output(
        output_name=output_name,
        value=plugin_info,
    )

    yield AssetMaterialization(
        asset_key=context.asset_key_for_output(output_name),
        metadata={
            "plugin_info_model_yaml": MetadataValue.md(
                f"```yaml\n{yaml.safe_dump(json.loads(plugin_info.model_dump_json(indent=2, fallback=str)))}\n```"
            ),
        }
    )


@asset(
    **ASSET_HEADER_OIIO_PROCESSOR_EXR_TO_PNG,
    ins={
        "CONFIG": AssetIn(
            AssetKey([*ASSET_HEADER_JOB_PROCESSOR["key_prefix"], "CONFIG"]),
        ),
        "job_info_model": AssetIn(
            AssetKey([*ASSET_HEADER_OIIO_PROCESSOR_EXR_TO_PNG["key_prefix"], "job_info_model"]),
        ),
        "plugin_info_model": AssetIn(
            AssetKey([*ASSET_HEADER_OIIO_PROCESSOR_EXR_TO_PNG["key_prefix"], "plugin_info_model"]),
        ),
        # "job_id_raw": AssetIn(
        #     AssetKey([*ASSET_HEADER_JOB_PROCESSOR_DEADLINE["key_prefix"], "job_id_raw"]),
        # ),
    },
)
def payload_request(
        context: AssetExecutionContext,
        CONFIG: DefaultConstants,
        job_info_model: models_submission.JobInfo,
        plugin_info_model: models_submission.CommandLinePluginInfo,
        # job_id_raw: str,
) -> Generator[Output[Dict] | AssetMaterialization | Any, Any, None]:

    """
    Before:
    cat "/data/share/AWSPortalRoot1/out/Test Production/Shot/SH030/Rendering/037/4_1197-1254_4/combined_dict.json"

    After
    cat "/data/share/AWSPortalRoot1/out/Test Production/Shot/SH030/Rendering/045/4_0997-1104_4/combined_dict.json"
    """

    headers = {
        "Content-Type": "application/json",
        "Accept-Charset": "UTF-8",
    }

    context.log.debug(f"{headers = }")

    # https://docs.thinkboxsoftware.com/products/deadline/10.2/1_User%20Manual/manual/rest-jobs.html#submit-job
    payload_raw = {
        "JobInfo": json.loads(job_info_model.model_dump_json(indent=2, fallback=str)),
        "PluginInfo": json.loads(plugin_info_model.model_dump_json(indent=2, fallback=str)),
        "IdOnly": False,
        "AuxFiles": [],
    }

    context.log.debug(f"{payload_raw = }")

    yield Output(payload_raw)

    yield AssetMaterialization(
        asset_key=context.asset_key,
        metadata={
            "__".join(context.asset_key.path): MetadataValue.md(
                f"```json\n{json.dumps(payload_raw, default=str, sort_keys=True, indent=CONFIG.JSON_INDENT)}\n```"
            ),
        }
    )


@multi_asset(
    outs={
        "job": AssetOut(
            **ASSET_HEADER_OIIO_PROCESSOR_EXR_TO_PNG,
            dagster_type=Dict,
            description="The resulting job details received "
                        "from Deadline.",
        ),
        "job_id": AssetOut(
            **ASSET_HEADER_OIIO_PROCESSOR_EXR_TO_PNG,
            dagster_type=str,
            description="The job ID received from Deadline.",
        ),
    },
    ins={
        "CONFIG": AssetIn(
            AssetKey([*ASSET_HEADER_JOB_PROCESSOR["key_prefix"], "CONFIG"]),
        ),
        "payload_request": AssetIn(
            AssetKey([*ASSET_HEADER_OIIO_PROCESSOR_EXR_TO_PNG["key_prefix"], "payload_request"]),
        ),
        "job_model": AssetIn(
            AssetKey([*ASSET_HEADER_JOB_PROCESSOR_READER["key_prefix"], "read_job_yaml"])
        ),
    },
)
def submit_request_exr_to_png(
        context: AssetExecutionContext,
        CONFIG: DefaultConstants,
        payload_request: Dict,
        job_model: JobBase,
) -> Generator[Output[requests.Response] | AssetMaterialization | Any, Any, None]:

    """
    Before:
    cat "/data/share/AWSPortalRoot1/out/Test Production/Shot/SH030/Rendering/037/4_1197-1254_4/combined_dict.json"

    After
    cat "/data/share/AWSPortalRoot1/out/Test Production/Shot/SH030/Rendering/045/4_0997-1104_4/combined_dict.json"
    """

    headers = {
        "Content-Type": "application/json",
        "Accept-Charset": "UTF-8",
    }

    context.log.debug(f"{headers = }")

    payload = json.dumps(payload_request, indent=CONFIG.JSON_INDENT, sort_keys=True, default=str)

    context.log.debug(f"{payload = }")

    context.log.info(f"Sending request to {job_model.deadline_config.rest_api_jobs}...")

    # Requests: data vs. json:
    # - https://stackoverflow.com/a/26685359/2207196
    # - https://requests.readthedocs.io/en/latest/user/quickstart/#more-complicated-post-requests
    #   > If you need that header set and you don’t want to encode the dict yourself, you can
    #   > also pass it directly using the json parameter (added in version 2.4.2) and it will
    #   > be encoded automatically
    # -> using `json=` does not serialize as expected (yet). Hence, `data=` and manual.
    request = requests.Request(
        url=job_model.deadline_config.rest_api_jobs,
        method="POST",
        headers=headers,
        # json=payload_raw,
        data=payload,
    )

    context.log.debug(f"{request = }")

    prepared_request = request.prepare()

    context.log.debug(f"{prepared_request = }")

    session = requests.Session()
    response = session.send(prepared_request, verify=False)

    context.log.debug(f"{response = }")
    context.log.debug(f"{response.raw = }")
    context.log.debug(f"{response.status_code = }")
    # context.log.debug(f"{response.content = }")
    context.log.debug(f"{response.text = }")

    output_name = "job"

    yield Output(
        output_name=output_name,
        value=response.json(),
    )

    yield AssetMaterialization(
        asset_key=context.asset_key_for_output(output_name),
        metadata={
            "__".join(context.asset_key_for_output(output_name).path): MetadataValue.json(payload),
            "headers": MetadataValue.md(
                f"```json\n{json.dumps(headers, default=str, sort_keys=True, indent=CONFIG.JSON_INDENT)}\n```"
            ),
            "payload": MetadataValue.md(
                f"```json\n{payload}\n```"
            ),
            "request": MetadataValue.md(
                f"```json\n{json.dumps(request.__dict__, indent=CONFIG.JSON_INDENT, default=str, sort_keys=True)}\n```"
            ),
            "response": MetadataValue.md(
                f"```json\n{json.dumps(response.json(), default=str, sort_keys=True, indent=CONFIG.JSON_INDENT)}\n```"
            ),
        }
    )

    output_name = "job_id"

    _id = response.json().get("_id", None)

    yield Output(
        output_name=output_name,
        value=_id,
    )

    yield AssetMaterialization(
        asset_key=context.asset_key_for_output(output_name),
        metadata={
            "__".join(context.asset_key_for_output(output_name).path): MetadataValue.path(_id),
        }
    )
