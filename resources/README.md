## Running Production Tests

This is a module that runs the Skymap Scanner CI tests within a production instance (`skydriver` or `skydriver-dev`).

```
python -m prod_tester --skydriver (dev|prod) --cluster sub-2 --n-workers 100 --priority 100 [--one]
```

## SkyDriver Client Request Helper Scripts

1. `pip install -r pyrequest/requirements.txt`
2. Run a [Python script.](./pyrequest)
