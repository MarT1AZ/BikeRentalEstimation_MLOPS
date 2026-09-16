# EDA Guidelines

## Markdown cells

You may edit Markdown cells in the notebook. Keep their existing structure unless you are explicitly asked to rephrase the text or change its bullet points.

## Code cells

You may edit only code cells that contain the following comment:

```python
# AGENT TASK
```


## Cell Context


There may be many amrked cell, you should identify the right cell then call it out before editting

You should read nearest markdown to identify the target cell

## Data investigation scripts

Reusable diagnostic scripts belong in `agent_script/`. They should be read-only by default, report their findings clearly, and avoid modifying source or output data unless explicitly requested. For yearly bike-rental output checks, run:

```bash
python agent_script/check_datetime.py
```

The datetime checker validates the `Year`, `Month`, `Day`, and `Hour` fields, detects invalid or out-of-year timestamps, reports duplicate hourly keys, and compares each file with the complete expected hourly timeline.

