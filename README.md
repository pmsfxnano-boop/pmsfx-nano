# PMSF-X Nano

Minimal market-intelligence dashboard built with FastAPI.

## Deploy on Render

The repository already contains a Render Blueprint (`render.yaml`).

1. Open Render and choose **New → Blueprint**.
2. Connect the GitHub repository `pmsfxnano-boop/pmsfx-nano`.
3. Select the repository and deploy the Blueprint.
4. In the Render service, add the environment variable `TIINGO_API_KEY` with your Tiingo API key.
5. Open the generated Render URL.

The service starts with:

```text
uvicorn main:app --host 0.0.0.0 --port $PORT
```

Health check:

```text
/health
```

Dashboard:

```text
/
```

## Important

Without `TIINGO_API_KEY`, the API remains online but live quote requests return a configuration error. The project intentionally does not generate forecasts or trading signals when valid market data is unavailable.

## Local

```bash
pip install -r requirements.txt
uvicorn main:app --reload
```
