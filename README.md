[![ Logo OpenStudioLandscapes ](https://github.com/michimussato/OpenStudioLandscapes/raw/main/media/images/logo128.png)](https://github.com/michimussato/OpenStudioLandscapes)

---

<!-- TOC -->
* [OpenStudioLandscapes-DagsterCodeLocation-ShotProcessor](#openstudiolandscapes-dagstercodelocation-shotprocessor)
  * [Brief](#brief)
  * [Usage](#usage)
    * [CLI](#cli)
* [Development](#development)
  * [pytest](#pytest)
<!-- TOC -->

---

# OpenStudioLandscapes-DagsterCodeLocation-ShotProcessor

> [!NOTE]
> 
> This package was scaffolded with `dagster==1.9.11`
> 
> ```shell
> dagster project scaffold --name OpenStudioLandscapes-DagsterCodeLocation-ShotProcessor
> cd OpenStudioLandscapes-DagsterCodeLocation-ShotProcessor
> git init --initial-branch main
> git remote add origin https://github.com/michimussato/OpenStudioLandscapes-DagsterCodeLocation-ShotProcessor.git
> git add *
> git commit -a -m "initial commit"
> git push -u origin main
> ```

## Brief

A package to run a post-render jobs on the resulting `EXR` files.

## Usage

```python
from typing import Any, Generator, List, Union

from dagster import (
    OpExecutionContext,
    AssetExecutionContext
)

from OpenStudioLandscapes.DagsterCodeLocation import ShotProcessor

...
```

### CLI

```shell
shot-processor -vv \
    --exr-sequence-dir "tests/fixtures/raw/" \
    --output-dir "tests/fixtures/oiio/" \
    --kitsu-task-json "tests/fixtures/kitsu_task.json" \
    --version 005
```

> [!TIP]
> 
> Reinstall this package inside the container it was deployed to:
> 
> ```shell
> docker exec -it dagster.2026-01-21_17-22-54__seasoned-jelly-wholesale-mixer bash
> pip3 install --root-user-action=ignore --editable 'OpenStudioLandscapes-DagsterCodeLocation-ShotProcessor @ git+https://github.com/michimussato/OpenStudioLandscapes-DagsterCodeLocation-ShotProcessor.git@main'
> ```


# Development

```shell
pip install --force-reinstall --editable .[dev]

dagster dev --workspace workspace.yaml
```

## pytest

Add `-s` (equivalent to `--capture=no`) to `pytest` runner
in case `print` or `LOGGER` output is required.

> [!TIP]
> 
> In Pycharm:
> ![Modify Run Configuration](media/images/Screenshot_20260407_110349.png)