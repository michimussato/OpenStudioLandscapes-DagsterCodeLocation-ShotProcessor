[![ Logo OpenStudioLandscapes ](https://github.com/michimussato/OpenStudioLandscapes/raw/main/media/images/logo128.png)](https://github.com/michimussato/OpenStudioLandscapes)

---

<!-- TOC -->
* [OpenStudioLandscapes-DagsterCodeLocation-ShotProcessor](#openstudiolandscapes-dagstercodelocation-shotprocessor)
  * [Brief](#brief)
  * [Usage](#usage)
    * [CLI](#cli)
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
    --exr-sequence-dir "/data/share/AWSPortalRoot1/out/Test Production/Shot/SH030/Rendering/065/raw/" \
    --output-dir "/data/share/AWSPortalRoot1/out/Test Production/Shot/SH030/Rendering/065/oiio/" \
    --kitsu-task-json "/data/share/AWSPortalRoot1/out/Test Production/Shot/SH030/Rendering/065/kitsu_task.json"
```