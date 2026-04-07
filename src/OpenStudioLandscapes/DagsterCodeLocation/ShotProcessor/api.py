import json
import os
import pathlib
import re
from typing import Tuple, Dict, Union

from dagster import (
    get_dagster_logger,
    AssetExecutionContext,
    OpExecutionContext,
)

import OpenImageIO as OIIO

from OpenStudioLandscapes.DagsterCodeLocation.ShotProcessor.config.models import ConfigOIIO

oiio = OIIO

__author__ = "Michael Mussato"
__copyright__ = "Michael Mussato"
__url__ = "https://github.com/michimussato/OpenStudioLandscapes"
__license__ = "GNU Affero General Public License v3.0"

LOGGER = get_dagster_logger(__name__)


# @dataclass
# class ShotProcessorArgs:
#     # loglevel: int
#     # Static code inspection for argparse
#     # - [](https://stackoverflow.com/a/71035314)
#     kitsu_task_dict: Dict
#     version: str
#     render_version_directory: pathlib.Path
#
#     exr_sequence_dir: pathlib.Path
#     output_dir: pathlib.Path
#     # fps: float = 25.0
#     # text_border: int = 10
#     # text_spacing: int = 4
#     handle_marker_height: int = 10
#     overlay_text_size_frame: int = 24
#     overlay_text_size_scaledown: int = 8


# ---- Python API ----


# args_: ShotProcessorArgs
# kitsu_task_dict: Dict = {}


def _process_image(
    *,
    CONFIG_OIIO: ConfigOIIO,
    image_filepath: pathlib.Path,
    kitsu_task_dict: Dict,
    version: str,
    render_version_directory: pathlib.Path,
    context: Union[AssetExecutionContext, OpExecutionContext] = None,
) -> Dict[str, pathlib.Path]:

    if context is not None:
        LOGGER = context.log

    output_dir: pathlib.Path = render_version_directory.joinpath(
        version,
        "oiio",
    )
    output_dir.mkdir(parents=True, exist_ok=True)

    f_no_ = re.findall(
        r"\.[0-9]+\.",
        image_filepath.name
    )

    if bool(f_no_):
        f_no = int(f_no_[-1].replace(".", ""))
    else:
        f_no = 0

    LOGGER.debug(f"Frame number: {f_no}")

    # global args_
    # global kitsu_task_dict

    def create_buf_from_raw(
            raw: pathlib.Path,
    ) -> Tuple[OIIO.ImageBuf, OIIO.ImageSpec]:
        raw_image_ = OIIO.ImageInput.open(raw.as_posix())
        raw_spec_ = raw_image_.spec()
        raw_buf_ = OIIO.ImageBuf(raw.as_posix())

        return raw_buf_, raw_spec_

    raw_buf, raw_spec = create_buf_from_raw(image_filepath)

    # Get Some Metadata
    frame = raw_spec.getattribute("Frame")
    camera = raw_spec.getattribute("Camera")
    resolution = f"{raw_spec.width}x{raw_spec.height}"
    render_time = raw_spec.getattribute("RenderTime")
    scene_file = raw_spec.getattribute("File")
    fps = 0

    # Don't change anything to the raw_spec.
    # Just set custom metadata.
    raw_spec["openstudiolandscapes.kitsu.project.name"] = kitsu_task_dict.get("project", {}).get("name", "N/A")
    raw_spec["openstudiolandscapes.kitsu.sequence.name"] = kitsu_task_dict.get("sequence", {}).get("name", "N/A")
    raw_spec["openstudiolandscapes.kitsu.entity.name"] = kitsu_task_dict.get("entity", {}).get("name", "N/A")
    raw_spec["openstudiolandscapes.kitsu.task.id"] = kitsu_task_dict.get("id", "N/A")
    # raw_spec["openstudiolandscapes.kitsu.json"] = args_.kitsu_task_json.as_posix()
    raw_spec["openstudiolandscapes.data.resolution"] = resolution
    raw_spec["openstudiolandscapes.version"] = version
    raw_spec["openstudiolandscapes.kitsu.entity.data.resolution"] = kitsu_task_dict.get("entity", {}).get("data", {}).get("resolution", "N/A")
    raw_spec["openstudiolandscapes.fps"] = f"{float(fps):.3f}"
    raw_spec["openstudiolandscapes.kitsu.entity.data.fps"] = f"{float(kitsu_task_dict.get('entity', {}).get('data', {}).get('fps', 0)):.3f}"
    frame_in = kitsu_task_dict.get("entity", {}).get("data", {}).get("frame_in", 0)
    raw_spec["openstudiolandscapes.kitsu.entity.data.frame_in"] = frame_in
    frame_out = kitsu_task_dict.get("entity", {}).get("data", {}).get("frame_out", 0)
    raw_spec["openstudiolandscapes.kitsu.entity.data.frame_out"] = frame_out
    frame_is_handle = frame_in > f_no or f_no > frame_out
    # frame_is_handle = frame_out < f_no
    LOGGER.debug(f"{frame_is_handle = }")
    raw_spec["openstudiolandscapes.is_handle"] = frame_is_handle
    # raw_spec["openstudiolandscapes.version"] = "001"
    # raw_spec["openstudiolandscapes.author.email"] = "michimussato@gmail.com"

    # Create overlay ImagaBuf (with alpha)
    spec_buf_overlay = raw_spec.copy()
    spec_buf_overlay.nchannels = 4
    spec_buf_overlay.channelnames = ("R", "G", "B", "A")
    spec_buf_overlay.alpha_channel = 3
    # text_overlay_buf = OIIO.ImageBuf(spec_buf_overlay)

    def get_overlay_text_buf(
            spec_buf_overlay: OIIO.ImageSpec,
    ) -> OIIO.ImageBuf:
        buf = OIIO.ImageBuf(spec_buf_overlay)
        pos_y = int(spec_buf_overlay.y + CONFIG_OIIO.text_border) + CONFIG_OIIO.overlay_text_size_frame + CONFIG_OIIO.handle_marker_height
        oiio.ImageBufAlgo.render_text(
            buf,
            x=CONFIG_OIIO.text_border,
            # y=int(spec_buf_overlay.full_height - (overlay_text_size_frame / 2)),
            y=pos_y,
            text=f"Frame: {frame}",
            fontsize=CONFIG_OIIO.overlay_text_size_frame,
            textcolor=[1, 1, 1, 1]
        ) or LOGGER.error("Can't render text: Frame")

        overlay_text_size_camera = CONFIG_OIIO.overlay_text_size_frame - CONFIG_OIIO.overlay_text_size_scaledown
        pos_y += CONFIG_OIIO.text_spacing + overlay_text_size_camera
        oiio.ImageBufAlgo.render_text(
            buf,
            x=CONFIG_OIIO.text_border,
            # y=int((spec_buf_overlay.full_height - overlay_text_size_camera) - (overlay_text_size_frame / 2)),
            y=pos_y,
            text=f"Camera: {camera}",
            fontsize=overlay_text_size_camera,
            textcolor=[1, 1, 1, 1]
        ) or LOGGER.error("Can't render text: Camera")

        overlay_text_size_resolution = CONFIG_OIIO.overlay_text_size_frame - CONFIG_OIIO.overlay_text_size_scaledown
        pos_y += CONFIG_OIIO.text_spacing + overlay_text_size_resolution
        oiio.ImageBufAlgo.render_text(
            buf,
            x=CONFIG_OIIO.text_border,
            # y=int((spec_buf_overlay.full_height - overlay_text_size_camera) - (overlay_text_size_frame / 2)),
            y=pos_y,
            text=f"Resolution: {raw_spec.getattribute('openstudiolandscapes.data.resolution')} @ {raw_spec.getattribute('openstudiolandscapes.fps')}",
            fontsize=overlay_text_size_resolution,
            textcolor=[1, 1, 1, 1]
        ) or LOGGER.error("Can't render text: Resolution")

        overlay_text_size_resolution_kitsu = CONFIG_OIIO.overlay_text_size_frame - CONFIG_OIIO.overlay_text_size_scaledown
        pos_y += CONFIG_OIIO.text_spacing + overlay_text_size_resolution_kitsu
        oiio.ImageBufAlgo.render_text(
            buf,
            x=CONFIG_OIIO.text_border,
            # y=int((spec_buf_overlay.full_height - overlay_text_size_camera) - (overlay_text_size_frame / 2)),
            y=pos_y,
            text=f"Resolution (Kitsu): {raw_spec.getattribute('openstudiolandscapes.kitsu.entity.data.resolution')} @ {raw_spec.getattribute('openstudiolandscapes.kitsu.entity.data.fps')}",
            fontsize=overlay_text_size_resolution,
            textcolor=[1, 1, 1, 1]
        ) or LOGGER.error("Can't render text: Resolution")

        overlay_text_size_taskid = CONFIG_OIIO.overlay_text_size_frame - CONFIG_OIIO.overlay_text_size_scaledown
        pos_y += CONFIG_OIIO.text_spacing + overlay_text_size_taskid
        oiio.ImageBufAlgo.render_text(
            buf,
            x=CONFIG_OIIO.text_border,
            # y=int((spec_buf_overlay.full_height - overlay_text_size_camera) - (overlay_text_size_frame / 2)),
            y=pos_y,
            text=f"Task: {raw_spec.getattribute('openstudiolandscapes.kitsu.task.id')}",
            fontsize=overlay_text_size_taskid,
            textcolor=[1, 1, 1, 1]
        ) or LOGGER.error("Can't render text: Task")

        # overlay_text_size_resolution = args_.overlay_text_size_frame - args_.overlay_text_size_scaledown
        # pos_y += args_.text_spacing + overlay_text_size_resolution
        # oiio.ImageBufAlgo.render_text(
        #     buf,
        #     x=args_.text_border,
        #     # y=int((spec_buf_overlay.full_height - overlay_text_size_resolution) - (overlay_text_size_resolution / 2)),
        #     y=pos_y,
        #     text=f"Resolution: {resolution}",
        #     fontsize=overlay_text_size_resolution,
        #     textcolor=[1, 1, 1, 1]
        # ) or LOGGER.error("Can't render text: Resolution")

        overlay_text_size_rendertime = CONFIG_OIIO.overlay_text_size_frame - CONFIG_OIIO.overlay_text_size_scaledown
        pos_y += CONFIG_OIIO.text_spacing + overlay_text_size_rendertime
        oiio.ImageBufAlgo.render_text(
            buf,
            x=CONFIG_OIIO.text_border,
            # y=int((spec_buf_overlay.full_height - overlay_text_size_resolution) - (overlay_text_size_resolution / 2)),
            y=pos_y,
            text=f"RenderTime: {render_time}",
            fontsize=overlay_text_size_rendertime,
            textcolor=[1, 1, 1, 1]
        ) or LOGGER.error("Can't render text: RenderTime")

        overlay_text_size_file = CONFIG_OIIO.overlay_text_size_frame - CONFIG_OIIO.overlay_text_size_scaledown
        pos_y += CONFIG_OIIO.text_spacing + overlay_text_size_file
        oiio.ImageBufAlgo.render_text(
            buf,
            x=CONFIG_OIIO.text_border,
            # y=int((spec_buf_overlay.full_height - overlay_text_size_resolution) - (overlay_text_size_resolution / 2)),
            y=pos_y,
            text=f"File: {scene_file}",
            fontsize=overlay_text_size_rendertime,
            textcolor=[1, 1, 1, 1]
        ) or LOGGER.error("Can't render text: File")

        overlay_text_size_show = CONFIG_OIIO.overlay_text_size_frame - CONFIG_OIIO.overlay_text_size_scaledown
        pos_y += CONFIG_OIIO.text_spacing + overlay_text_size_show
        oiio.ImageBufAlgo.render_text(
            buf,
            x=CONFIG_OIIO.text_border,
            # y=int((spec_buf_overlay.full_height - overlay_text_size_resolution) - (overlay_text_size_resolution / 2)),
            y=pos_y,
            text=f"Show: {raw_spec.getattribute('openstudiolandscapes.kitsu.project.name')}",
            fontsize=overlay_text_size_show,
            textcolor=[1, 1, 1, 1]
        ) or LOGGER.error("Can't render text: Show")

        overlay_text_size_shot = CONFIG_OIIO.overlay_text_size_frame - CONFIG_OIIO.overlay_text_size_scaledown
        pos_y += CONFIG_OIIO.text_spacing + overlay_text_size_shot
        oiio.ImageBufAlgo.render_text(
            buf,
            x=CONFIG_OIIO.text_border,
            # y=int((spec_buf_overlay.full_height - overlay_text_size_resolution) - (overlay_text_size_resolution / 2)),
            y=pos_y,
            text=f"Shot: {raw_spec.getattribute('openstudiolandscapes.kitsu.sequence.name')}_{raw_spec.getattribute('openstudiolandscapes.kitsu.entity.name')}",
            fontsize=overlay_text_size_shot,
            textcolor=[1, 1, 1, 1]
        ) or LOGGER.error("Can't render text: Shot")

        return buf

    overlay_text_buf = get_overlay_text_buf(spec_buf_overlay=spec_buf_overlay)
    overlay_text_buf_out: pathlib.Path = output_dir / "oiio_overlay_text" / image_filepath.name
    overlay_text_buf_out.parent.mkdir(parents=True, exist_ok=True)
    overlay_text_buf.write(overlay_text_buf_out.as_posix())
    LOGGER.info(f"Overlay text image saved: {overlay_text_buf_out.as_posix()}")

    def get_overlay_handle_buf(
            spec_buf_overlay: oiio.ImageSpec,
            frame_is_handle: bool,
    ) -> OIIO.ImageBuf:
        buf = OIIO.ImageBuf(spec_buf_overlay)

        handle_colors = {
            True: [1, 0, 0, 1],
            False: [0, 1, 0, 1]
        }

        # Top Marker
        oiio.ImageBufAlgo.render_box(
            buf,
            x1=0,
            y1=0,
            x2=spec_buf_overlay.width,
            y2=CONFIG_OIIO.handle_marker_height,
            fill=True,
            color=handle_colors[frame_is_handle]
        ) or LOGGER.error("Can't render box: frame_is_handle top")
        # Bottom Marker
        oiio.ImageBufAlgo.render_box(
            buf,
            x1=0,
            y1=spec_buf_overlay.height - CONFIG_OIIO.handle_marker_height,
            x2=spec_buf_overlay.width,
            y2=spec_buf_overlay.height,
            fill=True,
            color=handle_colors[frame_is_handle]
        ) or LOGGER.error("Can't render box: frame_is_handle bottom")

        return buf

    overlay_handle_buf = get_overlay_handle_buf(
        spec_buf_overlay=spec_buf_overlay,
        frame_is_handle=frame_is_handle,
    )
    overlay_handle_buf_out: pathlib.Path = output_dir / "oiio_overlay_handle" / image_filepath.name
    overlay_handle_buf_out.parent.mkdir(parents=True, exist_ok=True)
    overlay_handle_buf.write(overlay_handle_buf_out.as_posix())
    LOGGER.info(f"Overlay handle image saved: {overlay_handle_buf_out.as_posix()}")

    def create_exr_from_raw_with_custom_metadata(
            raw: pathlib.Path,
            exr_out: pathlib.Path,
    ) -> None:
        raw_image_ = OIIO.ImageInput.open(raw.as_posix())
        raw_image_pixels = raw_image_.read_image()
        out = oiio.ImageOutput.create(exr_out.as_posix())

        LOGGER.info(f"{exr_out.as_posix()} supports 'multiimage': {out.supports('multiimage')}")
        LOGGER.info(f"{exr_out.as_posix()} supports 'appendsubimage': {out.supports('appendsubimage')}")

        out.open(exr_out.as_posix(), raw_spec, "Create")

        try:
            e = None
            out.write_image(raw_image_pixels)
        except Exception as e:
            LOGGER.error(e)
        finally:
            out.close()

        if e is not None:
            raise Exception from e

        return None

    exr_touched_out = output_dir / "oiio_exr" / image_filepath.name
    exr_touched_out.parent.mkdir(parents=True, exist_ok=True)
    create_exr_from_raw_with_custom_metadata(
        raw=image_filepath,
        exr_out=exr_touched_out,
    )

    ret = {
        "overlay_text_buf_out": overlay_text_buf_out,
        "overlay_handle_buf_out": overlay_handle_buf_out,
        "exr_touched_out": exr_touched_out,
    }

    return ret

    # def mov_from_exr_touched(
    #         mov_out: pathlib.Path,
    # ):
    #     # Todo
    #     #  - [ ] implement mov generation
    #     #  - [ ] upload to Kitsu
    #     pass
    #
    # def gif_from_exr_touched(
    #         gif_out: pathlib.Path,
    # ):
    #     # Todo
    #     #  - [ ] implement gif generation
    #     pass
    #
    # def png_from_exr_touched(
    #         png_out: pathlib.Path,
    # ):
    #     # Todo
    #     #  - [ ] implement png sequence generation
    #     pass


def run_shot_processor(
        args,
        # cli: bool = False,
):
    """
    cli: if the processor was invoked from the cli or not.
    """
    # LOGGER.setLevel(args.loglevel)

    LOGGER.debug("Running Shot Processor with args %s", args)

    # global args_
    # args_ = args
    # global kitsu_task_dict

    # _expand_args(args_)

    # return

    # Open this file once
    if args.kitsu_task_json.exists():
        LOGGER.info(f"Kitsu Task JSON found")
        LOGGER.info(f"Reading JSON: {args.kitsu_task_json.as_posix()}")
        with open(args.kitsu_task_json) as fr:
            kitsu_task_dict = json.load(fr)
        LOGGER.debug(f"Kitsu Task JSON loaded: {kitsu_task_dict}")
    else:
        kitsu_task_dict = {}
        LOGGER.warning(f"Kitsu Task JSON not found, using default values: {kitsu_task_dict = }")

    # kitsu_task_json = pathlib.Path(args_.kitsu_task_json)

    # Path.walk was added in Python 3.12
    # - https://stackoverflow.com/a/79132718
    for root, dirs, files in os.walk(args.exr_sequence_dir):
        # sort:
        # - [](https://stackoverflow.com/a/18282401)
        for dir_ in sorted(dirs):
            LOGGER.debug("Processing directory %s", dir_)
        for file_ in sorted(files):
            filepath = pathlib.Path(root, file_)
            LOGGER.debug("Processing file %s", filepath)
            _process_image(
                image_filepath=filepath,
                args=args
            )
