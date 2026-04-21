#!/opt/gazu/gazuenv/bin/python3.11
import argparse
import logging
import pathlib
import sys
import time

import gazu


"""
Resources:
- [Building a Portable Kitsu CLI with Python and Gazu (2026)](https://blog.cg-wire.com/kitsu-cli-single-binary/)

Todo:
 - [x] Use Gazu CLI directly?
       [CLI](https://github.com/cgwire/gazu?tab=readme-ov-file#cli)
       -> Nope, doesn't work. `gazu-cli` is very limited and can only (mostly) query
 - [ ] Move this CLI to its own package

```
root@dagster:/dagster# gazu-cli --help
Usage: gazu-cli [OPTIONS] COMMAND [ARGS]...

  Gazu CLI - Command-line client for the Kitsu API.

Options:
  --json     Output as JSON.
  --version  Show the version and exit.
  --help     Show this message and exit.

Commands:
  asset          Show details for an asset.
  asset-types    List asset types.
  assets         List assets for a project.
  comment        Post a comment on a task (with status change).
  episodes       List episodes for a project.
  login          Log in to a Kitsu instance and store credentials.
  logout         Log out and clear stored credentials.
  my-tasks       List tasks assigned to current user.
  persons        List all persons.
  project        Show details for a project.
  projects       List projects.
  search         Search for entities across the Kitsu instance.
  sequences      List sequences for a project.
  shot-casting   Show casting (assets linked) for a shot.
  shots          List shots for a project.
  status         Show current connection status.
  task           Show details for a task (by ID).
  task-statuses  List task statuses.
  task-types     List task types.
  tasks          List tasks for a project.
```

--task-id "b0cfdac7-afa9-4382-a75d-3c80a388e136" 
--comment "Output directory: `/data/share/AWSPortalRoot1/out/Test Production/Shot/SH030/Rendering/033/4_1197-1214_4`<br>Version: `033`<br>Frames: `4_1197-1214_4`<br>Comment: This is a new Bender job comment<br><br>---<br><br>Execution Command: `/data/share/rez-packages/packages/blender/4.1.1/blender --background ""/data/share/AWSPortalRoot1/fixtures/blender/sh030_001.blend"" --render-output ""/data/share/AWSPortalRoot1/out/Test Production/Shot/SH030/Rendering/033/4_1197-1214_4/sh030_001.####.exr"" --render-format EXR --engine CYCLES --frame-start 1 --frame-end 1 --threads 0 --render-anim`<br>Submission Command: Todo<br>Job file: `/data/share/AWSPortalRoot1/fixtures/blender/sh030_001.blend`<br>" 
--host "http://10.1.2.15:4545/api" 
--user "admin@example.com" 
--password "mysecretpassword" 
--movie-file "/data/share/AWSPortalRoot1/out/Test Production/Shot/SH030/Rendering/033/4_1197-1214_4/draft/mov/sh030_001.mov" 
--version "033"

Tanslated to gazu-cli:
gazu-cli login --host "http://10.1.2.15:4545/api" --email "admin@example.com" --password "mysecretpassword"

gazu-cli logout

Example command:
/opt/gazu/gazuenv/bin/python3.9 \
    "/nfs/deadline-repository/DeadlineRepository10/custom/events/Kitsu/kitsu_submission_cli.py" \
        --comment "Batch - blender-4.1.0-xvfb (BLENDER_EEVEE) - test_grease_pencil_001-100 - v121" \
        --host "X" \
        --task-id "X" \
        --user "admin@example.com" \
        --password "mysecretpassword" \
        --movie-file "/nfs/out/test_grease_pencil_001-100/121/Draft/test_grease_pencil_001-100.mov" \
        --version "121" \
        --parent-job "6613e49e5a26a00f36418111"
"""

from dagster import (
    get_dagster_logger,
)

__author__ = "Michael Mussato"
__copyright__ = "Michael Mussato"
__url__ = "https://github.com/michimussato/OpenStudioLandscapes-Deadline-10-2-DeadlineRepository10-OverlayFS"
__license__ = "GNU Affero General Public License v3.0"

# _logger = logging.getLogger(__name__)

_logger = get_dagster_logger(__name__)


# ---- Python API ----


class KitsuSubmissionException(Exception):
    pass


def submit_to_kitsu(args):

    _logger.debug(f"{args = }")
    print(dir(args))

    gazu.client.set_host(args.kitsu_host)

    print(args.kitsu_user)
    print(args.kitsu_password)

    gazu.log_in(args.kitsu_user, args.kitsu_password)

    task_dict = gazu.task.get_task(task_id=args.kitsu_task_id)
    print(task_dict)

    task = task_dict['name']
    _logger.debug(f"{task = }")
    print(task)
    task_status = task_dict['task_status']
    _logger.debug(f"{task_status = }")

    if not args.kitsu_movie_file.exists():
        raise KitsuSubmissionException(
            f"Movie file {args.kitsu_movie_file.as_posix()} does not exist. "
            "Can't publish to Kitsu. "
            "Operation aborted."
        )

    _logger.info(f"Starting Kitsu publish for movie file "
                 f"{args.kitsu_movie_file.as_posix()}...")

    (comment, preview_file) = gazu.task.publish_preview(
        task=task_dict,
        task_status=task_status,
        comment=args.kitsu_comment,
        preview_file_path=args.kitsu_movie_file,
    )

    _logger.info(f"Finished Kitsu publish for movie file.")

    _logger.debug(f"Result: {comment = }")
    _logger.debug(f"Result: {preview_file = }")

    print(f'{comment = }')
    print(f'{preview_file = }')

    return (comment, preview_file)


# ---- CLI ----


def parse_args(args):

    parser = argparse.ArgumentParser(
        description=""
    )

    parser.add_argument(
        "--verbose",
        dest="loglevel",
        help="set loglevel to INFO",
        action="store_const",
        const=logging.INFO,
    )
    parser.add_argument(
        "--very-verbose",
        dest="loglevel",
        help="set loglevel to DEBUG",
        action="store_const",
        const=logging.DEBUG,
    )

    parser.add_argument(
        '--task-id',
        type=str,
        nargs='?',
        dest='kitsu_task_id',
        required=True,
        default=None,
        help=''
    )

    parser.add_argument(
        '--comment',
        type=str,
        nargs='?',
        dest='kitsu_comment',
        required=False,
        default='No Comment',
        help=''
    )

    parser.add_argument(
        '--host',
        type=str,
        nargs='?',
        dest='kitsu_host',
        required=True,
        default=None,
        help=''
    )

    parser.add_argument(
        '--user',
        type=str,
        nargs='?',
        dest='kitsu_user',
        required=True,
        default=None,
        help=''
    )

    parser.add_argument(
        '--password',
        type=str,
        nargs='?',
        dest='kitsu_password',
        required=True,
        default=None,
        help=''
    )

    parser.add_argument(
        '--movie-file',
        type=pathlib.Path,
        nargs='?',
        dest='kitsu_movie_file',
        required=True,
        default=None,
        help=''
    )

    parser.add_argument(
        '--version',
        type=str,
        nargs='?',
        dest='kitsu_version',
        required=False,
        default=None,
        help=''
    )

    parser.add_argument(
        '--parent-job',
        type=str,
        nargs='?',
        dest='kitsu_parent_job',
        required=False,
        default=None,
        help=''
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
    _logger.critical(f"Logging level: {loglevel}")


def main(args):
    args = parse_args(args)
    if args.loglevel is not None:
        # Todo
        #  - [ ] Dagster logger prints DEBUG even in INFO level
        setup_logging(args.loglevel)

    start_time = time.time()

    submit_to_kitsu(args=args)

    elapsed_time = time.time() - start_time
    print(f"Time elapsed: {time.strftime('%H:%M:%S', time.gmtime(elapsed_time))}")
    _logger.debug(f"Publish took {time.strftime('%H:%M:%S', time.gmtime(elapsed_time))}")


def run():
    main(sys.argv[1:])


if __name__ == "__main__":
    run()