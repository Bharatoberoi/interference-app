# A6 Interference Management — Full-stack POC

Observable simulator for the dual-interface agent architecture. The backend runs real FastAPI endpoints and streams workflow events over Server-Sent Events; adapters are intentionally mocked but preserve the integration boundaries (JIMS, MMP, HPSM, Redis, Azure SQL, LiteLLM/Azure OpenAI).

## Run

### Backend
```bash
cd backend
python -m venv .venv
# Windows: .venv\\Scripts\\activate | macOS/Linux: source .venv/bin/activate
pip install -r requirements.txt
uvicorn main:app --reload --port 8000
```

### Frontend
```bash
cd frontend
npm install
npm run dev
```

Open http://localhost:5173. Choose a scenario and click **Run workflow**. API cards show started/in-progress/success/failure states, and payloads are expandable. The kill switch is applied at the orchestration boundary before non-idempotent remediation.

## Scenarios

- Bulk remediation success
- KPI guardrail failure with rollback
- External A2A RF scan
- A2A remediation crash with reconciliation
- MMP API failure with retry and circuit breaker

## API

- `GET /api/health`
- `POST /api/runs` body: `{ "scenario": "bulk-success", "kill_switch": false }`
- `GET /api/runs/{runId}/events` SSE stream

For a production implementation, replace the adapter calls in `backend/main.py` with the real clients and move run state/queues to Redis Streams or a durable event bus.
