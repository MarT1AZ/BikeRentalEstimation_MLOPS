# EDA Guidelines

## Markdown cells

You may edit Markdown cells in the notebook. Keep their existing structure unless you are explicitly asked to rephrase the text or change its bullet points.

## Code cells

You may edit only code cells that contain the following comment:

```python
# AGENT TASK
```


## Cell Context

There may be many marked cells. Identify the target cell and call it out before editing.

Read the nearest Markdown cell to identify the target code cell.

## Data investigation scripts

Reusable diagnostic scripts belong in `agent_script/`. They should be read-only by default, report their findings clearly, and avoid modifying source or output data unless explicitly requested. For yearly bike-rental output checks, run:

```bash
python agent_script/check_datetime.py
```

The datetime checker validates the `Year`, `Month`, `Day`, and `Hour` fields, detects invalid or out-of-year timestamps, reports duplicate hourly keys, and compares each file with the complete expected hourly timeline.

## Coding guidelines

Prefer explicit, reviewable code for small fixed sets of columns. For example, if a dataset has five columns and three are bike-related, list those three columns instead of introducing a generalized loop or regular expression.

Keep changes minimal and focused on the requested task. Preserve the existing structure and execution context unless a broader refactor is explicitly requested.

For shell scripts, assume the working directory and invocation style already used by the project. Do not add portability layers, fallback behavior, or extra validation that does not improve the requested workflow.

When editing code, add comments for meaningful behavior changes when requested. Remove temporary change markers before committing.

For DVC-managed datasets, save the generated output locally and let DVC track and push the dataset. Do not add custom S3 upload, version, or tagging logic to ingestion unless explicitly requested.


## Planning implementation guideline


I would like you to grill me like ask me if the behavior or design of what gonna be implemented is not yet well defined enough for you to actually implement it, you need to ask me until we get a common ground


## Common terminology

Please settle for common terminologies between user and you, so that it is easier to understand what the technical stuff you are trying to explain as user do not want to read throught a paragraph just to understand an explanation of an technical issue.

You can keep track of common terms under COMMON_TERMINOLOGIES.md which user can also look at it

You can keep it in format {Term} : {Explantion}

## Handling configuration file

There are many config file sued by Git, DVC and UV. These file should not be manually editted by you but it should be updated using command line with the respectative command

## Pytest

run "uv run pytest -v -s" no extra configuration needed

