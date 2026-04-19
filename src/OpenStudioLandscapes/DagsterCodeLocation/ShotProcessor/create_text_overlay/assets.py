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


GROUP_OIIO_PROCESSOR_CREATE_TEXT_OVERLAY = "OpenStudioLandscapes_DagsterCodeLocation_JobProcessor_OIIO_Processor_create_text_overlay"
# KEY_CONSTANTS_DEFAULT = [GROUP_CONSTANTS_DEFAULT, "Constants"]
KEY_OIIO_PROCESSOR_CREATE_TEXT_OVERLAY = [GROUP_OIIO_PROCESSOR_CREATE_TEXT_OVERLAY]

ASSET_HEADER_OIIO_PROCESSOR_CREATE_TEXT_OVERLAY = {
    "group_name": GROUP_OIIO_PROCESSOR_CREATE_TEXT_OVERLAY,
    "key_prefix": KEY_OIIO_PROCESSOR_CREATE_TEXT_OVERLAY,
}


@multi_asset(
    outs={
        "cmd": AssetOut(
            **ASSET_HEADER_OIIO_PROCESSOR_CREATE_TEXT_OVERLAY,
            dagster_type=List,
            description="Todo",
        ),
        "Deadline_OutputDirectory": AssetOut(
            **ASSET_HEADER_OIIO_PROCESSOR_CREATE_TEXT_OVERLAY,
            dagster_type=pathlib.Path,
            description="Todo",
        ),
        "Deadline_OutputFilename": AssetOut(
            **ASSET_HEADER_OIIO_PROCESSOR_CREATE_TEXT_OVERLAY,
            dagster_type=NoneType,
            description="Todo",
        ),
    },
    ins={
        "version": AssetIn(
            AssetKey([*ASSET_HEADER_JOB_PROCESSOR["key_prefix"], "version"]),
        ),
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
        # "job_id_raw": AssetIn(
        #     AssetKey([*ASSET_HEADER_JOB_PROCESSOR_DEADLINE["key_prefix"], "job_id_raw"]),
        # ),
    }
)
def create_text_overlay(
        context: AssetExecutionContext,
        # raw_to_oiio: List[Dict],
        # render_version_directory: pathlib.Path,
        render_output_directory: pathlib.Path,
        version: str,
        render_output_filename: Dict,
        # image_sequence_raw: List[pathlib.Path],
        CONFIG_OIIO_YAML: pathlib.Path,
        CONFIG: DefaultConstants,
        # job_id_raw: str,
) -> Generator[Output[List] | AssetMaterialization | Any, Any, None]:
    # https://stackoverflow.com/questions/24961127/how-to-create-a-video-from-images-with-ffmpeg
    # https://www.ffmpeg.media/articles/image-sequences-timelapse-photos-to-video

    # input_format_ = ".png"
    # output_format_ = "mp4"
    # Todo
    #  - [x] Remove hard code
    input_dir: pathlib.Path = render_output_directory.joinpath(
        # version,
        CONFIG.RENDER_RAW_OUT,
    )

    # Todo
    #  - [x] Remove hard code
    output_dir: pathlib.Path = render_output_directory.joinpath(
        # version,
        CONFIG.OIIO_BASE_OUT,
        CONFIG.OIIO_TEXT_OVERLAY_OUT,
    )
    output_dir.mkdir(parents=True, exist_ok=True)

    # png_seq: List[pathlib.Path] = []

    # for d_image in raw_to_oiio:
    #     png: Union[pathlib.Path, None]
    #     png = d_image.get("png_out", None)
    #     if png is not None:
    #         png_seq.append(png)

    # context.log.debug(f"{png_seq = }")

    # # cmds: List[List[str]] = []
    # ffmpeg_out = pathlib.Path(output_dir).joinpath(
    #     f"{output_format_}.{output_format_}"
    # )

    # if bool(png_seq):
    # i_seq = []
    # f: pathlib.Path
    # for f in png_seq:
    #     i_seq.extend(["-i", f.as_posix()])

    # with tempfile.NamedTemporaryFile(
    #         delete=False,
    #         prefix="ffmpeg_images_list__",
    #         suffix=".txt",
    #         mode="w",
    # ) as file_out:
    #
    #     for f in png_seq:
    #         file_out.write(f"file {f.as_posix()}\n")

    # Todo:
    #  - [ ] add in timestamp
    #  - [ ] add out timestamp
    cmd: List[str] = [
        "shot-processor",
        "--exr-image", input_dir.joinpath(render_output_filename["padding_deadline_batch_startframe"]).as_posix(),
        "--kitsu-task-json", render_output_directory.joinpath("kitsu_task.json").as_posix(),
        "--oiio-config-yaml", CONFIG_OIIO_YAML.as_posix(),
        "--version", version,
        # https://docs.thinkboxsoftware.com/products/deadline/10.2/1_User%20Manual/manual/manual-submission.html#arbitrary-command-line-jobs
        "--frame-number", "<STARTFRAME%4>",
        "--output-dir", output_dir.as_posix(),
        "create-text-overlay",
    ]

    #######
    # cmd #
    #######

    """
    =======================================================
    Error
    =======================================================
    Error: FailRenderException : Process returned non-zero exit code '127'
       at Deadline.Plugins.DeadlinePlugin.FailRender(String message) (Python.Runtime.PythonException)
      File "/var/lib/Thinkbox/Deadline10/workers/minion05-deadline-10-2-worker/plugins/69e408f7f32c64522835830d/CommandLine.py", line 79, in RenderTasks
        self.FailRender( "Process returned non-zero exit code '{}'".format( exitCode ) )
       at Python.Runtime.Dispatcher.Dispatch(ArrayList args)
       at __FranticX_GenericDelegate0Dispatcher.Invoke()
       at Deadline.Plugins.DeadlinePlugin.RenderTasks()
       at Deadline.Plugins.DeadlinePlugin.DoRenderTasks()
       at Deadline.Plugins.PluginWrapper.RenderTasks(Task task, String& outMessage, AbortLevel& abortLevel)
       at Deadline.Plugins.PluginWrapper.RenderTasks(Task task, String& outMessage, AbortLevel& abortLevel)
    
    =======================================================
    Type
    =======================================================
    RenderPluginException
    
    =======================================================
    Stack Trace
    =======================================================
       at Deadline.Plugins.SandboxedPlugin.d(DeadlineMessage bgt, CancellationToken bgu)
       at Deadline.Plugins.SandboxedPlugin.RenderTask(Task task, CancellationToken cancellationToken)
       at Deadline.Slaves.SlaveRenderThread.c(TaskLogWriter ajy, CancellationToken ajz)
    
    =======================================================
    Log
    =======================================================
    2026-04-18 22:50:17:  0: Loading Job's Plugin timeout is Disabled
    2026-04-18 22:50:17:  0: SandboxedPlugin: Render Job As User disabled, running as current user 'root'
    2026-04-18 22:50:22:  0: Executing plugin command of type 'Initialize Plugin'
    2026-04-18 22:50:22:  0: INFO: Executing plugin script '/var/lib/Thinkbox/Deadline10/workers/minion05-deadline-10-2-worker/plugins/69e408f7f32c64522835830d/CommandLine.py'
    2026-04-18 22:50:22:  0: INFO: Plugin execution sandbox using Python version 3
    2026-04-18 22:50:22:  0: INFO: Single Frames Only: False
    2026-04-18 22:50:22:  0: INFO: About: Command Line Plugin for Deadline
    2026-04-18 22:50:22:  0: INFO: The job's environment will be merged with the current environment before rendering
    2026-04-18 22:50:22:  0: Done executing plugin command of type 'Initialize Plugin'
    2026-04-18 22:50:23:  0: Start Job timeout is disabled.
    2026-04-18 22:50:23:  0: Task timeout is disabled.
    2026-04-18 22:50:23:  0: Loaded job: Test Production - SH030 - 4_1201-1250_4 - Rendering - sh030_001.blend - 101 - blender - Text Overlay (69e408f7f32c64522835830d)
    2026-04-18 22:50:23:  0: Executing plugin command of type 'Start Job'
    2026-04-18 22:50:23:  0: DEBUG: S3BackedCache Client is not installed.
    2026-04-18 22:50:23:  0: INFO: Executing global asset transfer preload script '/var/lib/Thinkbox/Deadline10/workers/minion05-deadline-10-2-worker/plugins/69e408f7f32c64522835830d/GlobalAssetTransferPreLoad.py'
    2026-04-18 22:50:23:  0: INFO: Looking for legacy (pre-10.0.26) AWS Portal File Transfer...
    2026-04-18 22:50:23:  0: INFO: Looking for legacy (pre-10.0.26) File Transfer controller in /opt/Thinkbox/S3BackedCache/bin/task.py...
    2026-04-18 22:50:23:  0: INFO: Could not find legacy (pre-10.0.26) AWS Portal File Transfer.
    2026-04-18 22:50:23:  0: INFO: Legacy (pre-10.0.26) AWS Portal File Transfer is not installed on the system.
    2026-04-18 22:50:23:  0: Done executing plugin command of type 'Start Job'
    2026-04-18 22:50:23:  0: Plugin rendering frame(s): 1
    2026-04-18 22:50:23:  0: Executing plugin command of type 'Render Task'
    2026-04-18 22:50:23:  0: INFO: Executable: /bin/bash
    2026-04-18 22:50:23:  0: INFO: Arguments: -c "shot-processor --exr-image '/data/share/AWSPortalRoot1/out/Test Production/Shot/SH030/Rendering/101/raw/sh030_001.0001.exr' --kitsu-task-json '/data/share/AWSPortalRoot1/out/Test Production/Shot/SH030/Rendering/101/kitsu_task.json' --oiio-config-yaml '/data/share/AWSPortalRoot1/out/Test Production/Shot/SH030/Rendering/101/config_oiio.yaml' --version 101 --frame-number '0001' --output-dir '/data/share/AWSPortalRoot1/out/Test Production/Shot/SH030/Rendering/101/oiio/oiio_text_overlay' create-text-overlay"
    2026-04-18 22:50:23:  0: INFO: Execute in Shell: False
    2026-04-18 22:50:23:  0: INFO: Invoking: Run Process
    2026-04-18 22:50:23:  0: STDOUT: /bin/bash: line 1: shot-processor: command not found
    2026-04-18 22:50:23:  0: INFO: Process returned: 127
    2026-04-18 22:50:23:  0: Done executing plugin command of type 'Render Task'
    
    =======================================================
    Details
    =======================================================
    Date: 04/18/2026 22:50:27
    Frames: 1
    Elapsed Time: 00:00:00:10
    Job Submit Date: 04/18/2026 22:43:03
    Job User: michael
    Average RAM Usage: 1682622720 (11%)
    Peak RAM Usage: 1685450752 (11%)
    Average CPU Usage: 24%
    Peak CPU Usage: 37%
    Used CPU Clocks (x10^6 cycles): 6131
    Total CPU Clocks (x10^6 cycles): 25545
    
    =======================================================
    Worker Information
    =======================================================
    Worker Name: minion05-deadline-10-2-worker
    Version: v10.2.1.1 Release (094cbe890)
    Operating System: Linux
    Machine User: root
    IP Address: 192.168.178.20
    MAC Address: 5E:48:2B:DA:D0:CC
    CPU Architecture: x86_64
    CPUs: 4
    CPU Usage: 30%
    Memory Usage: 1.6 GB / 15.5 GB (10%)
    Free Disk Space: 3.220 GB 
    Video Card: 
    """

    """
    Job Info Parameters
    
    BatchName=Batch: Test Production - SH030 - 4_1201-1250_4 - Rendering - sh030_001.blend - 101 - blender
    Comment=This is a new Bender job comment
    Denylist=
    EventOptIns=
    ForceReloadPlugin=True
    Frames=1
    JobDependency0=69e406cff32c64522835830c
    JobDependencyPercentage=100
    MachineName=lenovo
    Name=Test Production - SH030 - 4_1201-1250_4 - Rendering - sh030_001.blend - 101 - blender - Text Overlay
    OverrideTaskExtraInfoNames=False
    Plugin=CommandLine
    Priority=70
    Region=
    ScheduledStartDateTime=19/04/2026 00:43
    UserName=michael
    """

    """
    Plugin Info Parameters
    
    Arguments=-c "shot-processor --exr-image '/data/share/AWSPortalRoot1/out/Test Production/Shot/SH030/Rendering/101/raw/sh030_001.<STARTFRAME%4>.exr' --kitsu-task-json '/data/share/AWSPortalRoot1/out/Test Production/Shot/SH030/Rendering/101/kitsu_task.json' --oiio-config-yaml '/data/share/AWSPortalRoot1/out/Test Production/Shot/SH030/Rendering/101/config_oiio.yaml' --version 101 --frame-number '<STARTFRAME%4>' --output-dir '/data/share/AWSPortalRoot1/out/Test Production/Shot/SH030/Rendering/101/oiio/oiio_text_overlay' create-text-overlay"
    Executable=/bin/bash
    """

    """
    BatchName=Batch: Test Production - SH030 - 4_1201-1250_4 - Rendering - sh030_001.blend - 101 - blender
    Comment=This is a new Bender job comment
    Denylist=
    EventOptIns=
    ForceReloadPlugin=True
    Frames=1197,1213,1229,1245,1205,1221,1237,1253,1201,1209,1217,1225,1233,1241,1249,1199,1203,1207,1211,1215,1219,1223,1227,1231,1235,1239,1243,1247,1251,1198,1200,1202,1204,1206,1208,1210,1212,1214,1216,1218,1220,1222,1224,1226,1228,1230,1232,1234,1236,1238,1240,1242,1244,1246,1248,1250,1252,1254
    JobDependency0=69e406cff32c64522835830c
    JobDependencyPercentage=100
    MachineName=lenovo
    Name=Test Production - SH030 - 4_1201-1250_4 - Rendering - sh030_001.blend - 101 - blender - Text Overlay
    OverrideTaskExtraInfoNames=False
    Plugin=CommandLine
    Priority=70
    Region=
    ScheduledStartDateTime=19/04/2026 00:43
    UserName=michael
    """

    """
    =======================================================
    Error
    =======================================================
    Error: FailRenderException : Process returned non-zero exit code '1'
       at Deadline.Plugins.DeadlinePlugin.FailRender(String message) (Python.Runtime.PythonException)
      File "/var/lib/Thinkbox/Deadline10/workers/minion05-deadline-10-2-worker/plugins/69e408f7f32c64522835830d/CommandLine.py", line 79, in RenderTasks
        self.FailRender( "Process returned non-zero exit code '{}'".format( exitCode ) )
       at Python.Runtime.Dispatcher.Dispatch(ArrayList args)
       at __FranticX_GenericDelegate0Dispatcher.Invoke()
       at Deadline.Plugins.DeadlinePlugin.RenderTasks()
       at Deadline.Plugins.DeadlinePlugin.DoRenderTasks()
       at Deadline.Plugins.PluginWrapper.RenderTasks(Task task, String& outMessage, AbortLevel& abortLevel)
       at Deadline.Plugins.PluginWrapper.RenderTasks(Task task, String& outMessage, AbortLevel& abortLevel)
    
    =======================================================
    Type
    =======================================================
    RenderPluginException
    
    =======================================================
    Stack Trace
    =======================================================
       at Deadline.Plugins.SandboxedPlugin.d(DeadlineMessage bgt, CancellationToken bgu)
       at Deadline.Plugins.SandboxedPlugin.RenderTask(Task task, CancellationToken cancellationToken)
       at Deadline.Slaves.SlaveRenderThread.c(TaskLogWriter ajy, CancellationToken ajz)
    
    =======================================================
    Log
    =======================================================
    2026-04-19 06:56:42:  0: Loading Job's Plugin timeout is Disabled
    2026-04-19 06:56:42:  0: SandboxedPlugin: Render Job As User disabled, running as current user 'root'
    2026-04-19 06:56:47:  0: Executing plugin command of type 'Initialize Plugin'
    2026-04-19 06:56:47:  0: INFO: Executing plugin script '/var/lib/Thinkbox/Deadline10/workers/minion05-deadline-10-2-worker/plugins/69e408f7f32c64522835830d/CommandLine.py'
    2026-04-19 06:56:47:  0: INFO: Plugin execution sandbox using Python version 3
    2026-04-19 06:56:47:  0: INFO: Single Frames Only: False
    2026-04-19 06:56:47:  0: INFO: About: Command Line Plugin for Deadline
    2026-04-19 06:56:47:  0: INFO: The job's environment will be merged with the current environment before rendering
    2026-04-19 06:56:47:  0: Done executing plugin command of type 'Initialize Plugin'
    2026-04-19 06:56:47:  0: Start Job timeout is disabled.
    2026-04-19 06:56:47:  0: Task timeout is disabled.
    2026-04-19 06:56:47:  0: Loaded job: Test Production - SH030 - 4_1201-1250_4 - Rendering - sh030_001.blend - 101 - blender - Text Overlay (69e408f7f32c64522835830d)
    2026-04-19 06:56:47:  0: Executing plugin command of type 'Start Job'
    2026-04-19 06:56:47:  0: DEBUG: S3BackedCache Client is not installed.
    2026-04-19 06:56:47:  0: INFO: Executing global asset transfer preload script '/var/lib/Thinkbox/Deadline10/workers/minion05-deadline-10-2-worker/plugins/69e408f7f32c64522835830d/GlobalAssetTransferPreLoad.py'
    2026-04-19 06:56:48:  0: INFO: Looking for legacy (pre-10.0.26) AWS Portal File Transfer...
    2026-04-19 06:56:48:  0: INFO: Looking for legacy (pre-10.0.26) File Transfer controller in /opt/Thinkbox/S3BackedCache/bin/task.py...
    2026-04-19 06:56:48:  0: INFO: Could not find legacy (pre-10.0.26) AWS Portal File Transfer.
    2026-04-19 06:56:48:  0: INFO: Legacy (pre-10.0.26) AWS Portal File Transfer is not installed on the system.
    2026-04-19 06:56:48:  0: Done executing plugin command of type 'Start Job'
    2026-04-19 06:56:48:  0: Plugin rendering frame(s): 1213
    2026-04-19 06:56:48:  0: Executing plugin command of type 'Render Task'
    2026-04-19 06:56:48:  0: INFO: Executable: /bin/bash
    2026-04-19 06:56:48:  0: INFO: Arguments: -c "shot-processor --exr-image '/data/share/AWSPortalRoot1/out/Test Production/Shot/SH030/Rendering/101/raw/sh030_001.1213.exr' --kitsu-task-json '/data/share/AWSPortalRoot1/out/Test Production/Shot/SH030/Rendering/101/kitsu_task.json' --oiio-config-yaml '/data/share/AWSPortalRoot1/out/Test Production/Shot/SH030/Rendering/101/config_oiio.yaml' --version 101 --frame-number '1213' --output-dir '/data/share/AWSPortalRoot1/out/Test Production/Shot/SH030/Rendering/101/oiio/oiio_text_overlay' create-text-overlay"
    2026-04-19 06:56:48:  0: INFO: Execute in Shell: False
    2026-04-19 06:56:48:  0: INFO: Invoking: Run Process
    2026-04-19 06:56:50:  0: STDOUT: Traceback (most recent call last):
    2026-04-19 06:56:50:  0: STDOUT:   File "/opt/python3.11/bin/shot-processor", line 6, in <module>
    2026-04-19 06:56:50:  0: STDOUT:     sys.exit(run())
    2026-04-19 06:56:50:  0: STDOUT:              ^^^^^
    2026-04-19 06:56:50:  0: STDOUT:   File "/opt/python3.11/lib/python3.11/site-packages/OpenStudioLandscapes/DagsterCodeLocation/ShotProcessor/cli.py", line 338, in run
    2026-04-19 06:56:50:  0: STDOUT:     main(sys.argv[1:])
    2026-04-19 06:56:50:  0: STDOUT:   File "/opt/python3.11/lib/python3.11/site-packages/OpenStudioLandscapes/DagsterCodeLocation/ShotProcessor/cli.py", line 233, in main
    2026-04-19 06:56:50:  0: STDOUT:     setup_logging(args.loglevel)
    2026-04-19 06:56:50:  0: STDOUT:   File "/opt/python3.11/lib/python3.11/site-packages/OpenStudioLandscapes/DagsterCodeLocation/ShotProcessor/cli.py", line 228, in setup_logging
    2026-04-19 06:56:50:  0: STDOUT:     LOGGER.setLevel(loglevel)
    2026-04-19 06:56:50:  0: STDOUT:   File "/opt/python3.11/lib/python3.11/logging/__init__.py", line 1464, in setLevel
    2026-04-19 06:56:50:  0: STDOUT:     self.level = _checkLevel(level)
    2026-04-19 06:56:50:  0: STDOUT:                  ^^^^^^^^^^^^^^^^^^
    2026-04-19 06:56:50:  0: STDOUT:   File "/opt/python3.11/lib/python3.11/logging/__init__.py", line 210, in _checkLevel
    2026-04-19 06:56:50:  0: STDOUT:     raise TypeError("Level not an integer or a valid string: %r"
    2026-04-19 06:56:50:  0: STDOUT: TypeError: Level not an integer or a valid string: None
    2026-04-19 06:56:50:  0: INFO: Process returned: 1
    2026-04-19 06:56:50:  0: Done executing plugin command of type 'Render Task'
    
    =======================================================
    Details
    =======================================================
    Date: 04/19/2026 06:56:54
    Frames: 1213
    Elapsed Time: 00:00:00:12
    Job Submit Date: 04/18/2026 22:43:03
    Job User: michael
    Average RAM Usage: 1789409024 (11%)
    Peak RAM Usage: 1833238528 (12%)
    Average CPU Usage: 31%
    Peak CPU Usage: 57%
    Used CPU Clocks (x10^6 cycles): 5003
    Total CPU Clocks (x10^6 cycles): 16137
    
    =======================================================
    Worker Information
    =======================================================
    Worker Name: minion05-deadline-10-2-worker
    Version: v10.2.1.1 Release (094cbe890)
    Operating System: Linux
    Machine User: root
    IP Address: 192.168.178.20
    MAC Address: 5E:48:2B:DA:D0:CC
    CPU Architecture: x86_64
    CPUs: 4
    CPU Usage: 28%
    Memory Usage: 1.7 GB / 15.5 GB (10%)
    Free Disk Space: 1.978 GB 
    Video Card: 
    """

    output_name = "cmd"

    yield Output(
        output_name=output_name,
        value=cmd,
    )

    yield AssetMaterialization(
        asset_key=context.asset_key_for_output(output_name),
        metadata={
            "__".join(
                context.asset_key_for_output(output_name).path
            ): MetadataValue.json(cmd),
            # "cmd": MetadataValue.md(
            #     f"```yaml\n{yaml.safe_dump(cmd)}\n```"
            # ),
            # "logs": MetadataValue.md(
            #     f"```yaml\n{yaml.safe_dump(logs)}\n```"
            # ),
            "cmd_": MetadataValue.path(shlex.join(cmd)),
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
            **ASSET_HEADER_OIIO_PROCESSOR_CREATE_TEXT_OVERLAY,
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
            AssetKey([*ASSET_HEADER_OIIO_PROCESSOR_CREATE_TEXT_OVERLAY["key_prefix"], "Deadline_OutputDirectory"]),
        ),
        # "Deadline_OutputFilename": AssetIn(
        #     AssetKey([*ASSET_HEADER_OIIO_PROCESSOR_CREATE_TEXT_OVERLAY["key_prefix"], "Deadline_OutputFilename"]),
        # ),
    }
)
def job_info(
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
        "Name": f"{job_title_str} - Text Overlay",
        "Comment": job_model.comment,
        # "Department"
        "BatchName": batch_name,
        "UserName": job_model.deadline_config.user,
        "MachineName": job_model.deadline_config.host,
        # "Pool"
        # "SecondaryPool"
        # "Group"
        "Priority": job_model.job_priority,
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
            **ASSET_HEADER_OIIO_PROCESSOR_CREATE_TEXT_OVERLAY,
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
            AssetKey([*ASSET_HEADER_OIIO_PROCESSOR_CREATE_TEXT_OVERLAY["key_prefix"], "cmd"])
        ),
    }
)
def plugin_info(
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
    **ASSET_HEADER_OIIO_PROCESSOR_CREATE_TEXT_OVERLAY,
    ins={
        "CONFIG": AssetIn(
            AssetKey([*ASSET_HEADER_JOB_PROCESSOR["key_prefix"], "CONFIG"]),
        ),
        "job_info_model": AssetIn(
            AssetKey([*ASSET_HEADER_OIIO_PROCESSOR_CREATE_TEXT_OVERLAY["key_prefix"], "job_info_model"]),
        ),
        "plugin_info_model": AssetIn(
            AssetKey([*ASSET_HEADER_OIIO_PROCESSOR_CREATE_TEXT_OVERLAY["key_prefix"], "plugin_info_model"]),
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
            **ASSET_HEADER_OIIO_PROCESSOR_CREATE_TEXT_OVERLAY,
            dagster_type=Dict,
            description="The resulting job details received "
                        "from Deadline.",
        ),
        "job_id": AssetOut(
            **ASSET_HEADER_OIIO_PROCESSOR_CREATE_TEXT_OVERLAY,
            dagster_type=str,
            description="The job ID received from Deadline.",
        ),
    },
    ins={
        "CONFIG": AssetIn(
            AssetKey([*ASSET_HEADER_JOB_PROCESSOR["key_prefix"], "CONFIG"]),
        ),
        "payload_request": AssetIn(
            AssetKey([*ASSET_HEADER_OIIO_PROCESSOR_CREATE_TEXT_OVERLAY["key_prefix"], "payload_request"]),
        ),
        "job_model": AssetIn(
            AssetKey([*ASSET_HEADER_JOB_PROCESSOR_READER["key_prefix"], "read_job_yaml"])
        ),
    },
)
def submit_request_create_text_overlay(
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
