import json
import os
import pathlib
import re
from gettext import npgettext
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


def _create_buf_from_raw(
        raw: pathlib.Path,
) -> Tuple[OIIO.ImageBuf, OIIO.ImageSpec]:
    raw_image_ = OIIO.ImageInput.open(raw.as_posix())
    raw_spec_ = raw_image_.spec()
    raw_buf_ = OIIO.ImageBuf(raw.as_posix())

    return raw_buf_, raw_spec_


def _update_raw_spec(
    *,
    # context: Union[AssetExecutionContext, OpExecutionContext] = None,
    raw_spec: OIIO.ImageSpec,
    CONFIG_OIIO: ConfigOIIO,
    kitsu_task_dict: Dict,
    version: str,
    frame_number: int,
) -> OIIO.ImageSpec:

    raw_spec_ = raw_spec.copy()

    # Get Some Metadata
    # frame = raw_spec_.getattribute("Frame")
    # camera = raw_spec_.getattribute("Camera")
    resolution = f"{raw_spec_.width}x{raw_spec_.height}"
    # render_time = raw_spec_.getattribute("RenderTime")
    # scene_file = raw_spec_.getattribute("File")
    fps = CONFIG_OIIO.fps

    # Don't change anything to the raw_spec.
    # Create a new spec dict with additional metadata.
    frame_in = kitsu_task_dict.get("entity", {}).get("data", {}).get("frame_in", 0)
    frame_out = kitsu_task_dict.get("entity", {}).get("data", {}).get("frame_out", 0)
    frame_is_handle = frame_number < frame_in or frame_number > frame_out
    LOGGER.debug(f"{frame_is_handle = }")

    update_dict = {
        "openstudiolandscapes.kitsu.project.name": kitsu_task_dict.get("project", {}).get("name", "N/A"),
        "openstudiolandscapes.kitsu.sequence.name": kitsu_task_dict.get("sequence", {}).get("name", "N/A"),
        "openstudiolandscapes.kitsu.entity.name": kitsu_task_dict.get("entity", {}).get("name", "N/A"),
        "openstudiolandscapes.kitsu.task.id": kitsu_task_dict.get("id", "N/A"),
        # "openstudiolandscapes.kitsu.json", args_.kitsu_task_json.as_posix(),
        "openstudiolandscapes.data.resolution": resolution,
        "openstudiolandscapes.version": version,
        "openstudiolandscapes.kitsu.entity.data.resolution": kitsu_task_dict.get("entity", {}).get(
            "data", {}
        ).get("resolution", "N/A"),
        "openstudiolandscapes.fps": f"{float(fps):.3f}",
        "openstudiolandscapes.kitsu.entity.data.fps": f"{float(kitsu_task_dict.get('entity', {}).get('data', {}).get('fps', 0)):.3f}",
        "openstudiolandscapes.kitsu.entity.data.frame_in": frame_in,
        "openstudiolandscapes.kitsu.entity.data.frame_out": frame_out,
        "openstudiolandscapes.is_handle": frame_is_handle,
    }

    for k, v in update_dict.items():
        raw_spec_[k] = v

    return raw_spec_


def get_overlay_text_buf(
    *,
    CONFIG_OIIO: ConfigOIIO,
    spec_buf_overlay: OIIO.ImageSpec,
    frame_number: int,
    camera: str,
) -> OIIO.ImageBuf:
    buf = OIIO.ImageBuf(spec_buf_overlay)
    pos_y = int(spec_buf_overlay.y + CONFIG_OIIO.text_border) + CONFIG_OIIO.overlay_text_size_frame + CONFIG_OIIO.handle_marker_height
    oiio.ImageBufAlgo.render_text(
        buf,
        x=CONFIG_OIIO.text_border,
        # y=int(spec_buf_overlay.full_height - (overlay_text_size_frame / 2)),
        y=pos_y,
        text=f"Frame: {frame_number}",
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
        text=f"Resolution: {spec_buf_overlay.getattribute('openstudiolandscapes.data.resolution')} @ {spec_buf_overlay.getattribute('openstudiolandscapes.fps')}",
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
        text=f"Resolution (Kitsu): {spec_buf_overlay.getattribute('openstudiolandscapes.kitsu.entity.data.resolution')} @ {spec_buf_overlay.getattribute('openstudiolandscapes.kitsu.entity.data.fps')}",
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
        text=f"Task: {spec_buf_overlay.getattribute('openstudiolandscapes.kitsu.task.id')}",
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
        text=f"RenderTime: {spec_buf_overlay.getattribute('RenderTime')}",
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
        text=f"File: {spec_buf_overlay.getattribute('File')}",
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
        text=f"Show: {spec_buf_overlay.getattribute('openstudiolandscapes.kitsu.project.name')}",
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
        text=f"Shot: {spec_buf_overlay.getattribute('openstudiolandscapes.kitsu.sequence.name')}_{spec_buf_overlay.getattribute('openstudiolandscapes.kitsu.entity.name')}",
        fontsize=overlay_text_size_shot,
        textcolor=[1, 1, 1, 1]
    ) or LOGGER.error("Can't render text: Shot")

    return buf


def get_overlay_handle_buf(
    *,
    CONFIG_OIIO: ConfigOIIO,
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


# def get_png_buf(
#     *,
#     CONFIG_OIIO: ConfigOIIO,
#     spec: oiio.ImageSpec,
#     raw_buf: oiio.ImageBuf,
#     # frame_is_handle: bool,
# ) -> OIIO.ImageBuf:
#     buf = OIIO.ImageBuf(spec)
#
#     buf.
#
#     handle_colors = {
#         True: [1, 0, 0, 1],
#         False: [0, 1, 0, 1]
#     }
#
#     # # Top Marker
#     # oiio.ImageBufAlgo.render_box(
#     #     buf,
#     #     x1=0,
#     #     y1=0,
#     #     x2=spec_buf_raw.width,
#     #     y2=CONFIG_OIIO.handle_marker_height,
#     #     fill=True,
#     #     color=handle_colors[frame_is_handle]
#     # ) or LOGGER.error("Can't render box: frame_is_handle top")
#     # # Bottom Marker
#     # oiio.ImageBufAlgo.render_box(
#     #     buf,
#     #     x1=0,
#     #     y1=spec_buf_overlay.height - CONFIG_OIIO.handle_marker_height,
#     #     x2=spec_buf_overlay.width,
#     #     y2=spec_buf_overlay.height,
#     #     fill=True,
#     #     color=handle_colors[frame_is_handle]
#     # ) or LOGGER.error("Can't render box: frame_is_handle bottom")
#
#     return buf


def png_from_raw(
        raw: pathlib.Path,
        png_out: pathlib.Path,
        spec: oiio.ImageSpec,
) -> pathlib.Path:
    raw_image_ = OIIO.ImageInput.open(raw.as_posix())
    raw_image_pixels = raw_image_.read_image()
    out = oiio.ImageOutput.create(png_out.as_posix())

    # LOGGER.info(f"{png_out.as_posix()} supports 'multiimage': {out.supports('multiimage')}")
    # LOGGER.info(f"{png_out.as_posix()} supports 'appendsubimage': {out.supports('appendsubimage')}")

    out.open(png_out.as_posix(), spec, "Create")

    try:
        e = None
        out.write_image(raw_image_pixels)
    except Exception as e:
        LOGGER.error(e)
    finally:
        out.close()

    if e is not None:
        raise Exception from e

    return png_out


def exr_from_raw_with_custom_metadata(
        raw: pathlib.Path,
        exr_out: pathlib.Path,
        spec: oiio.ImageSpec,
) -> None:
    raw_image_ = OIIO.ImageInput.open(raw.as_posix())
    raw_image_pixels = raw_image_.read_image()
    out = oiio.ImageOutput.create(exr_out.as_posix())

    LOGGER.info(f"{exr_out.as_posix()} supports 'multiimage': {out.supports('multiimage')}")
    LOGGER.info(f"{exr_out.as_posix()} supports 'appendsubimage': {out.supports('appendsubimage')}")

    out.open(exr_out.as_posix(), spec, "Create")

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


def _process_image(
    *,
    context: Union[AssetExecutionContext, OpExecutionContext] = None,
    raw_buf: OIIO.ImageBuf,
    raw_spec: OIIO.ImageSpec,
    CONFIG_OIIO: ConfigOIIO,
    image_filepath: pathlib.Path,
    frame_number: int,
    kitsu_task_dict: Dict,
    version: str,
    output_dir: pathlib.Path,
    create_exr_from_raw_with_custom_metadata: bool = True,
    create_text_overlay: bool = False,
    create_handle_overlay: bool = False,
    create_png: bool = False,
) -> Dict[str, pathlib.Path]:

    ret = {}

    if context is not None:
        LOGGER = context.log

    # Get Some Metadata
    # frame = raw_spec.getattribute("Frame")
    camera = raw_spec.getattribute("Camera")
    # resolution = f"{raw_spec.width}x{raw_spec.height}"
    # render_time = raw_spec.getattribute("RenderTime")
    # scene_file = raw_spec.getattribute("File")

    raw_spec_updated: OIIO.ImageSpec = _update_raw_spec(
        CONFIG_OIIO=CONFIG_OIIO,
        raw_spec=raw_spec,
        kitsu_task_dict=kitsu_task_dict,
        version=version,
        frame_number=frame_number,
    )

    create_overlay = any(
        [
            create_text_overlay,
            create_handle_overlay,
        ]
    )

    # Create overlay ImagaBuf (with alpha)
    if create_overlay:
        spec_buf_overlay = raw_spec_updated.copy()
        spec_buf_overlay.nchannels = 4
        spec_buf_overlay.channelnames = ("R", "G", "B", "A")
        spec_buf_overlay.alpha_channel = 3

        if create_text_overlay:

            overlay_text_buf = get_overlay_text_buf(
                CONFIG_OIIO=CONFIG_OIIO,
                spec_buf_overlay=spec_buf_overlay,
                frame_number=frame_number,
                camera=camera,
            )
            overlay_text_buf_out: pathlib.Path = output_dir / "oiio_overlay_text" / image_filepath.name
            overlay_text_buf_out.parent.mkdir(parents=True, exist_ok=True)
            overlay_text_buf.write(overlay_text_buf_out.as_posix())
            LOGGER.info(f"Overlay text image saved: {overlay_text_buf_out.as_posix()}")

            ret.update(
                {
                    "overlay_text_buf_out": overlay_text_buf_out,
                }
            )

        if create_handle_overlay:

            overlay_handle_buf = get_overlay_handle_buf(
                CONFIG_OIIO=CONFIG_OIIO,
                spec_buf_overlay=spec_buf_overlay,
                frame_is_handle=bool(spec_buf_overlay["openstudiolandscapes.is_handle"]),
            )
            png_buf_out: pathlib.Path = output_dir / "oiio_overlay_handle" / image_filepath.name
            png_buf_out.parent.mkdir(parents=True, exist_ok=True)
            overlay_handle_buf.write(png_buf_out.as_posix())
            LOGGER.info(f"Overlay handle image saved: {png_buf_out.as_posix()}")

            ret.update(
                {
                    "png_buf_out": png_buf_out,
                }
            )

    if create_exr_from_raw_with_custom_metadata:

        exr_touched_out = output_dir / "oiio_exr" / image_filepath.name
        exr_touched_out.parent.mkdir(parents=True, exist_ok=True)
        exr_from_raw_with_custom_metadata(
            raw=image_filepath,
            exr_out=exr_touched_out,
            spec=raw_spec_updated,
        )

        ret.update(
            {
                "exr_touched_out": exr_touched_out,
            }
        )

    # create_png = True
    if create_png:
        # https://dev.to/plinecom/convert-openexr-to-jpeg-using-openimageiooiio-in-python-52f8
        extension = ".png"
        spec_buf_png = raw_spec_updated.copy()
        channels = ("R", "G", "B")
        spec_buf_png.nchannels = len(channels)
        spec_buf_png.channelnames = channels
        try:
            spec_buf_png.alpha_channel = channels.index("A")
        except ValueError:
            # no alpha channel
            spec_buf_png.alpha_channel = -1
        # file_name = image_filepath.name
        png_out: pathlib.Path = output_dir / "oiio_png" / f"{image_filepath.stem}{extension}"
        png_out.parent.mkdir(parents=True, exist_ok=True)
        # png_buf.write(png_buf_out.as_posix())
        # LOGGER.info(f"PNG image saved: {png_buf_out.as_posix()}")

        png_out_ = png_from_raw(
            raw=image_filepath,
            png_out=png_out,
            # CONFIG_OIIO=CONFIG_OIIO,
            spec=spec_buf_png,
            # raw_buf=raw_buf,
            # spec_buf_overlay=spec_buf_overlay,
            # frame_is_handle=bool(spec_buf_overlay["openstudiolandscapes.is_handle"]),
        )

        ret.update(
            {
                "png_out": png_out_,
            }
        )

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


def _create_gif():
    ...


def _create_mov():
    ...


def _create_png():
    ...


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
