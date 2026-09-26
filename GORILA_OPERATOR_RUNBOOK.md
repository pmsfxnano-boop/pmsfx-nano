# Gorila Argentum — Operator Runbook

## Current live service

- Render service: `gorila-argentum-research`
- URL: https://gorila-argentum-research.onrender.com
- Branch: `gorila-argentum-v0-hardening`
- Runtime mode: `RESEARCH`
- Trading execution: disabled
- Automatic promotion: disabled

## Scientific validation closed

Jacobian Stress V2:
- workflow: `Gorila Argentum Jacobian Stress V2`
- run: `36218289147`
- status: success
- stages: central, lag, parameter_60, parameter_90, parameter_120, parameter_180
- frozen snapshot SHA-256: `b9e6da7eab39df607e72faef021e5ca0571590cf865fa62df1db9ea8a0abd753`
- every stage validated against the same snapshot
- aggregate artifact: `gorila-jacobian-stress-v2`

The stress harness uses bounded matrix stages and a vectorized NumPy logistic solver. This is a performance implementation change of the same standardized logistic/L2 model family; it is not treated as independent scientific confirmation of promotion.

## Operational safety

- `POST /api/runtime/tick` is protected by `X-Gorila-Runtime-Key`.
- Runtime tick fails closed without Postgres.
- Startup runs a cross-connection persistence probe.
- A one-time bootstrap run id is configured as `bootstrap-operational-v1`.
- Bootstrap executes only when durable Postgres is present and is persisted in `runtime_runs`.
- Promotion remains derived from validated evidence and cannot be enabled by environment configuration.
- Continuous Learning produces candidates; it does not auto-promote or replace the model.

## Final infrastructure gate

The existing Render Postgres is:
`pmsfx-nano-db`.

A dedicated Blueprint file is committed at:
`render-gorila.yaml`

It references the existing Postgres by name:

`DATABASE_URL <- fromDatabase(pmsfx-nano-db, connectionString)`

Render supports adding an existing service to a Blueprint and referencing an existing Postgres resource by `fromDatabase`. The Blueprint must be synced once from the Render Dashboard.

### One-time Dashboard action

Render Dashboard:
1. New → Blueprint.
2. Connect repository `pmsfxnano-boop/pmsfx-nano`.
3. Branch: `gorila-argentum-v0-hardening`.
4. Blueprint Path: `render-gorila.yaml`.
5. Review the existing `gorila-argentum-research` service and Postgres reference.
6. Deploy Blueprint.

After that sync, the service should redeploy with `DATABASE_URL` derived from the existing Postgres. The next startup will emit:

`GORILA_PERSISTENCE_ROUNDTRIP postgres True ...`

and, because `GORILA_BOOTSTRAP_TICK_RUN_ID=bootstrap-operational-v1` is already present, it will execute the one-time operational cycle and emit:

`GORILA_BOOTSTRAP_TICK_COMPLETED bootstrap-operational-v1 COMPLETED`

## Scheduler

A Render Cron Job was attempted but the current workspace requires a paid plan for that resource. The secured runtime endpoint is therefore ready for a future scheduler, but no recurring scheduler is claimed active.
