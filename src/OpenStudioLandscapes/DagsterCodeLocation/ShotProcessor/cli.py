import json
import sys
import argparse
import pathlib
import textwrap
import logging
from typing import Dict

# import yaml
from dagster import (
    get_dagster_logger,
)

# from OpenStudioLandscapes.DagsterCodeLocation.ShotProcessor.api import run_shot_processor

__author__ = "Michael Mussato"
__copyright__ = "Michael Mussato"
__url__ = "https://github.com/michimussato/OpenStudioLandscapes"
__license__ = "GNU Affero General Public License v3.0"

from OpenStudioLandscapes.DagsterCodeLocation.ShotProcessor.config.models import ConfigOIIO
# from OpenStudioLandscapes.DagsterCodeLocation.ShotProcessor import __version__


LOGGER = get_dagster_logger(__name__)


# ---- CLI ----
# The functions defined in this section are wrappers around the main Python
# API allowing them to be called directly from the terminal as a CLI
# executable/script.


# def parse_args(args) -> ShotProcessorArgs:
def parse_args(args) -> argparse.Namespace:
    """Parse command line parameters

    Args:
      args (List[str]): command line parameters as list of strings
          (for example  ``["--help"]``).

    Returns:
      :obj:`argparse.Namespace`: command line parameters namespace
    """
    parser = argparse.ArgumentParser(description="Takes an input EXR "
                                                 "and creates a new file based on it "
                                                 "by specifying the relevant sub-command.")
    # parser.add_argument(
    #     "--version",
    #     action="version",
    #     version=f"shot-processor version {__version__}",
    # )
    # parser.add_argument(dest="n", help="n-th Fibonacci number", type=int, metavar="INT")
    parser.add_argument(
        "-v",
        "--verbose",
        dest="loglevel",
        help="set loglevel to INFO",
        action="store_const",
        const=logging.INFO,
    )
    parser.add_argument(
        "-vv",
        "--very-verbose",
        dest="loglevel",
        help="set loglevel to DEBUG",
        action="store_const",
        const=logging.DEBUG,
    )

    parser.add_argument(
        "--kitsu-task-json",
        dest="kitsu_task_json",
        help="The full path to the Kitsu task JSON file.",
        type=pathlib.Path,
        required=True,
        # action="store_const",
        # const=logging.DEBUG,
    )

    parser.add_argument(
        "--oiio-config-yaml",
        dest="oiio_config_yaml",
        help="The full path to the OIIO config YAML file.",
        type=pathlib.Path,
        required=True,
        # action="store_const",
        # const=logging.DEBUG,
    )

    parser.add_argument(
        "--version",
        dest="version",
        help="The version (iteration) number.",
        type=str,
        required=True,
        # action="store_const",
        # const=logging.DEBUG,
    )

    parser.add_argument(
        "--frame-number",
        dest="frame_number",
        help="The frame number.",
        type=int,
        required=True,
        # action="store_const",
        # const=logging.DEBUG,
    )

    parser.add_argument(
        "--exr-image",
        dest="exr_image",
        help="The full path to the EXR file.",
        type=pathlib.Path,
        required=True,
        # action="store_const",
        # const=logging.DEBUG,
    )

    parser.add_argument(
        "--output-dir",
        dest="output_dir",
        help="The full path to the output base directory. Subdirectories will be created.",
        type=pathlib.Path,
        required=True,
        # action="store_const",
        # const=logging.DEBUG,
    )

    subparsers = parser.add_subparsers(
        dest="sub_command",
        required=True,
    )

    subparser_create_text_overlay = subparsers.add_parser(
        "create-text-overlay",
        # aliases=["s"],
    )

    subparser_create_handle_overlay = subparsers.add_parser(
        "create-handle-overlay",
        # aliases=["s"],
    )

    subparser_exr_with_custom_metadata = subparsers.add_parser(
        "exr-with-custom-metadata",
        # aliases=["s"],
    )

    subparser_exr_with_custom_metadata = subparsers.add_parser(
        "create-png",
        # aliases=["s"],
    )

    # parser.add_argument(
    #     "--fps",
    #     dest="fps",
    #     help="The frame rate of the sequence.",
    #     type=float,
    #     default=ShotProcessorArgs.fps,
    #     required=False,
    #     # action="store_const",
    #     # const=logging.DEBUG,
    # )
    # parser.add_argument(
    #     "--text-border",
    #     dest="text_border",
    #     help="The separation of the text from the frame.",
    #     type=int,
    #     default=ShotProcessorArgs.text_border,
    #     required=False,
    #     # action="store_const",
    #     # const=logging.DEBUG,
    # )
    # parser.add_argument(
    #     "--text-spacing",
    #     dest="text_spacing",
    #     help="The text line spacing.",
    #     type=int,
    #     default=ShotProcessorArgs.text_spacing,
    #     required=False,
    #     # action="store_const",
    #     # const=logging.DEBUG,
    # )
    # parser.add_argument(
    #     "--handle-marker-height",
    #     dest="handle_marker_height",
    #     help="The height of the handle marker.",
    #     type=int,
    #     default=ShotProcessorArgs.handle_marker_height,
    #     required=False,
    #     # action="store_const",
    #     # const=logging.DEBUG,
    # )
    # parser.add_argument(
    #     "--overlay-text-size-frame",
    #     dest="overlay_text_size_frame",
    #     help="The size of the frame number.",
    #     type=int,
    #     default=ShotProcessorArgs.overlay_text_size_frame,
    #     required=False,
    #     # action="store_const",
    #     # const=logging.DEBUG,
    # )
    # parser.add_argument(
    #     "--overlay-text-size-scaledown",
    #     dest="overlay_text_size_scaledown",
    #     help="The rest of the text will be scaled down by this amount.",
    #     type=int,
    #     default=ShotProcessorArgs.overlay_text_size_scaledown,
    #     required=False,
    #     # action="store_const",
    #     # const=logging.DEBUG,
    # )
    return parser.parse_args(args)
    return ShotProcessorArgs(**vars(parser.parse_args(args)))


def setup_logging(loglevel):
    """Setup basic logging

    Args:
      loglevel (int): minimum loglevel for emitting messages
    """
    logformat = "[%(asctime)s] %(levelname)s:%(name)s:%(message)s"
    logging.basicConfig(
        level=loglevel, stream=sys.stdout, format=logformat, datefmt="%Y-%m-%d %H:%M:%S"
    )
    LOGGER.setLevel(loglevel)


def main(args):
    args = parse_args(args)
    if args.loglevel is not None:
        # Todo
        #  - [ ] Dagster logger prints DEBUG even in INFO level
        setup_logging(args.loglevel)

    # Open this file once
    def parse_kitsu_task_json(
            kitsu_task_json: pathlib.Path,
    ) -> Dict:
        if kitsu_task_json.exists():
            LOGGER.info(f"Kitsu Task JSON found")
            LOGGER.info(f"Reading JSON: {args.kitsu_task_json.as_posix()}")
            # kitsu_task_dict = json.loads(kitsu_task_json.read_text())
            with open(kitsu_task_json, "r") as fr:
                kitsu_task_dict = json.load(fr)
            LOGGER.debug(f"Kitsu Task JSON loaded: {kitsu_task_dict}")
        else:
            kitsu_task_dict = {}
            LOGGER.warning(f"Kitsu Task JSON not found, using default values: {kitsu_task_dict = }")
        return kitsu_task_dict

    if args.sub_command == "create-text-overlay":
        from OpenStudioLandscapes.DagsterCodeLocation.ShotProcessor.api import create_text_overlay
        # with open(args.oiio_config_yaml, "r") as fr:
        #     config_oiio_dict = yaml.safe_load(fr)
        config_oiio = ConfigOIIO(
            # **config_oiio_dict
        )
        result = create_text_overlay(
            exr_src=args.exr_image,
            CONFIG_OIIO=config_oiio,
            kitsu_task_dict=parse_kitsu_task_json(args.kitsu_task_json),
            version=args.version,
            frame_number=args.frame_number,
            output_dir=args.output_dir,
        )

        sys.stdout.write(f"{result.as_posix()}\n")
        return 0

    elif args.sub_command == "create-handle-overlay":
        from OpenStudioLandscapes.DagsterCodeLocation.ShotProcessor.api import create_handle_overlay
        # with open(args.oiio_config_yaml, "r") as fr:
        #     config_oiio_dict = yaml.safe_load(fr)
        config_oiio = ConfigOIIO(
            # **config_oiio_dict
        )
        result = create_handle_overlay(
            exr_src=args.exr_image,
            CONFIG_OIIO=config_oiio,
            kitsu_task_dict=parse_kitsu_task_json(args.kitsu_task_json),
            version=args.version,
            frame_number=args.frame_number,
            output_dir=args.output_dir,
        )

        sys.stdout.write(f"{result.as_posix()}\n")
        return 0

    elif args.sub_command == "exr-with-custom-metadata":
        from OpenStudioLandscapes.DagsterCodeLocation.ShotProcessor.api import exr_with_custom_metadata
        # with open(args.oiio_config_yaml, "r") as fr:
        #     config_oiio_dict = yaml.safe_load(fr)
        config_oiio = ConfigOIIO(
            # **config_oiio_dict
        )
        result = exr_with_custom_metadata(
            exr_src=args.exr_image,
            CONFIG_OIIO=config_oiio,
            kitsu_task_dict=parse_kitsu_task_json(args.kitsu_task_json),
            version=args.version,
            frame_number=args.frame_number,
            output_dir=args.output_dir,
        )

        sys.stdout.write(f"{result.as_posix()}\n")
        return 0

    elif args.sub_command == "create-png":
        from OpenStudioLandscapes.DagsterCodeLocation.ShotProcessor.api import create_png
        # with open(args.oiio_config_yaml, "r") as fr:
        #     config_oiio_dict = yaml.safe_load(fr)
        config_oiio = ConfigOIIO(
            # **config_oiio_dict
        )
        result = create_png(
            exr_src=args.exr_image,
            CONFIG_OIIO=config_oiio,
            kitsu_task_dict=parse_kitsu_task_json(args.kitsu_task_json),
            version=args.version,
            frame_number=args.frame_number,
            output_dir=args.output_dir,
        )

        sys.stdout.write(f"{result.as_posix()}\n")
        return 0

    # args["kitsu_task_dict"] = parse_kitsu_task_json(args.kitsu_task_json)

    # args_ = ShotProcessorArgs(**vars(args))

    # run_shot_processor(
    #     args=args,
    #     cli=True,
    # )


def run():
    main(sys.argv[1:])


if __name__ == "__main__":
    raise SystemExit(textwrap.dedent("""
            Wrong entry point.
            Run `shot-processor --help` for more information.
            """))
