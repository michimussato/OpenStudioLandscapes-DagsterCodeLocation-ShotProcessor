import json
import sys
import argparse
import pathlib
import textwrap
import logging
from dataclasses import dataclass
from typing import Dict

from dagster import (
    get_dagster_logger,
)


__author__ = "Michael Mussato"
__copyright__ = "Michael Mussato"
__url__ = "https://github.com/michimussato/OpenStudioLandscapes"
__license__ = "GNU Affero General Public License v3.0"

from OpenStudioLandscapes.DagsterCodeLocation.ShotProcessor.config.models import ConfigOIIO
# from OpenStudioLandscapes.DagsterCodeLocation.ShotProcessor import __version__


LOGGER = get_dagster_logger(__name__)


@dataclass
class ShotProcessorArgs:
    # loglevel: int
    # Static code inspection for argparse
    # - [](https://stackoverflow.com/a/71035314)
    kitsu_task_dict: Dict
    version: str
    render_version_directory: pathlib.Path

    exr_sequence_dir: pathlib.Path
    output_dir: pathlib.Path
    # fps: float = 25.0
    # text_border: int = 10
    # text_spacing: int = 4
    handle_marker_height: int = 10
    overlay_text_size_frame: int = 24
    overlay_text_size_scaledown: int = 8


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
    )

    parser.add_argument(
        "--oiio-config-yaml",
        dest="oiio_config_yaml",
        help="The full path to the OIIO config YAML file.",
        type=pathlib.Path,
        required=True,
    )

    parser.add_argument(
        "--version",
        dest="version",
        help="The version (iteration) number.",
        type=str,
        required=True,
    )

    parser.add_argument(
        "--frame-number",
        dest="frame_number",
        help="The frame number.",
        type=int,
        required=True,
    )

    parser.add_argument(
        "--exr-image",
        dest="exr_image",
        help="The full path to the EXR file.",
        type=pathlib.Path,
        required=True,
    )

    parser.add_argument(
        "--output-dir",
        dest="output_dir",
        help="The full path to the output base directory. Subdirectories will be created.",
        type=pathlib.Path,
        required=True,
    )

    subparsers = parser.add_subparsers(
        dest="sub_command",
        required=True,
    )

    subparser_create_text_overlay = subparsers.add_parser(
        "create-text-overlay",
    )

    subparser_create_handle_overlay = subparsers.add_parser(
        "create-handle-overlay",
    )

    subparser_exr_with_custom_metadata = subparsers.add_parser(
        "exr-with-custom-metadata",
    )

    return parser.parse_args(args)


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


def run():
    main(sys.argv[1:])


if __name__ == "__main__":
    raise SystemExit(textwrap.dedent("""
            Wrong entry point.
            Run `shot-processor --help` for more information.
            """))
