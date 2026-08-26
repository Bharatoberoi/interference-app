# Local integration contract

These contracts describe local mock services only. They do not connect to real JIMS, MMP, HPSM, or RF systems.

## Services

| Service | Local URL | Purpose |
|---|---|---|
| JIMS mock | `http://localhost:8101` | Interference candidates and incidents |
| MMP mock | `http://localhost:8102` | Network parameter changes |
| HPSM mock | `http://localhost:8103` | Incident pre-log and audit trail |
| RF telemetry mock | `http://localhost:8104` | Signal and interference measurements |

## Authentication

Local mock credentials are stored in `database/local-auth-seed.sql` and are for development only:

| Service | Username | Password |
|---|---|---|
| JIMS | `jims-local` | `JimsLocal!2026` |
| MMP | `mmp-local` | `MmpLocal!2026` |
| HPSM | `hpsm-local` | `HpsmLocal!2026` |

Mock services accept `Authorization: Basic ...` in local development. Production services must use the approved OAuth2 or mTLS configuration.

## Local permission boundary

Only `localhost` and Docker's internal network are in scope. No external telecom system is contacted.

## Workflow sequence

1. RF telemetry produces measurements.
2. JIMS returns interference candidates.
3. LangGraph decides whether remediation is needed.
4. HPSM receives a pre-log.
5. MMP receives the parameter change.
6. The workflow validates KPIs and records the outcome.
