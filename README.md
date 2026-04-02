[![ Logo OpenStudioLandscapes ](https://github.com/michimussato/OpenStudioLandscapes/raw/main/media/images/logo128.png)](https://github.com/michimussato/OpenStudioLandscapes)

---

<!-- TOC -->
* [OpenStudioLandscapes-Dagster-ShotProcessor](#openstudiolandscapes-dagster-shotprocessor)
  * [Brief](#brief)
  * [Usage](#usage)
<!-- TOC -->

---

# OpenStudioLandscapes-Dagster-ShotProcessor

> [!NOTE]
> 
> This package was scaffolded with `dagster==1.9.11`
> 
> ```shell
> dagster project scaffold --name OpenStudioLandscapes-Dagster-ShotProcessor
> git -C ./OpenStudioLandscapes-ShotProcessor init --initial-branch main
> git -C ./OpenStudioLandscapes-ShotProcessor remote add origin https://github.com/michimussato/OpenStudioLandscapes-Dagster-ShotProcessor.git
> git -C ./OpenStudioLandscapes-ShotProcessor add *
> git -C ./OpenStudioLandscapes-ShotProcessor commit -a -m "initial commit"
> git -C ./OpenStudioLandscapes-ShotProcessor push -u origin main
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

from OpenStudioLandscapes.Dagster_Streaming_Process import submit_cmds

dagster_execution_context: Union[OpExecutionContext, AssetExecutionContext]
tasks: List[List[str]] = [
  [
    "ls",
    "-al",
    "/dir/1",
  ],
]

log_records: List[str] = submit_cmds(
  context=dagster_execution_context,
  cmds=tasks,
)
```
