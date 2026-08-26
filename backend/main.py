import asyncio, json, uuid, os, sqlite3, logging, time
import psycopg
from pathlib import Path
from datetime import datetime, timezone
from typing import Any, TypedDict
from fastapi import FastAPI, Header, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, PlainTextResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from langgraph.graph import END, START, StateGraph

app = FastAPI(title='A6 Interference Management POC')
app.add_middleware(CORSMiddleware, allow_origins=['*'], allow_methods=['*'], allow_headers=['*'])
streams: dict[str, asyncio.Queue] = {}
DB_PATH = os.getenv('A6_DB_PATH', str(Path(__file__).resolve().parent / 'a6_runs.db'))
DATABASE_URL = os.getenv('DATABASE_URL', '')
API_KEY = os.getenv('A6_API_KEY', '')
metrics = {'runs_started': 0, 'runs_completed': 0, 'runs_failed': 0, 'events_emitted': 0}

class JsonFormatter(logging.Formatter):
    def format(self, record):
        return json.dumps({'ts': now(), 'level': record.levelname, 'message': record.getMessage(), 'service': 'a6-orchestrator'})
logging.basicConfig(level=os.getenv('LOG_LEVEL', 'INFO'), format='%(message)s')
log = logging.getLogger('a6')

def init_db():
    if DATABASE_URL:
        with psycopg.connect(DATABASE_URL) as db:
            db.execute('CREATE TABLE IF NOT EXISTS runs (run_id TEXT PRIMARY KEY, scenario TEXT NOT NULL, status TEXT NOT NULL, created_at TIMESTAMPTZ NOT NULL, updated_at TIMESTAMPTZ NOT NULL, outcome TEXT)')
            db.execute('CREATE TABLE IF NOT EXISTS workflow_events (id TEXT PRIMARY KEY, run_id TEXT NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE, ts TIMESTAMPTZ NOT NULL, kind TEXT NOT NULL, title TEXT NOT NULL, message TEXT NOT NULL, payload JSONB)')
        return
    with sqlite3.connect(DB_PATH) as db:
        db.execute('CREATE TABLE IF NOT EXISTS runs (run_id TEXT PRIMARY KEY, scenario TEXT, status TEXT, created_at TEXT, updated_at TEXT, outcome TEXT)')
init_db()

def save_run(run_id, scenario, status, created_at, updated_at, outcome=None):
    if DATABASE_URL:
        with psycopg.connect(DATABASE_URL) as db:
            db.execute('INSERT INTO runs VALUES (%s,%s,%s,%s,%s,%s)', (run_id, scenario, status, created_at, updated_at, outcome))
    else:
        with sqlite3.connect(DB_PATH) as db: db.execute('INSERT INTO runs VALUES (?, ?, ?, ?, ?, ?)', (run_id, scenario, status, created_at, updated_at, outcome))

def update_run(run_id, status, outcome):
    if DATABASE_URL:
        with psycopg.connect(DATABASE_URL) as db: db.execute('UPDATE runs SET status=%s, outcome=%s, updated_at=%s WHERE run_id=%s', (status, outcome, now(), run_id))
    else:
        with sqlite3.connect(DB_PATH) as db: db.execute('UPDATE runs SET status=?, outcome=?, updated_at=? WHERE run_id=?', (status, outcome, now(), run_id))

def require_auth(x_api_key: str | None = Header(default=None)):
    if API_KEY and x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail='Invalid or missing API key')

class RunRequest(BaseModel):
    scenario: str = 'bulk-success'
    kill_switch: bool = False

def now(): return datetime.now(timezone.utc).isoformat()

async def emit(run_id: str, kind: str, title: str, message: str, **extra):
    event = {'id': str(uuid.uuid4()), 'ts': now(), 'kind': kind, 'title': title, 'message': message, **extra}
    if DATABASE_URL:
        with psycopg.connect(DATABASE_URL) as db:
            db.execute('INSERT INTO workflow_events (id,run_id,ts,kind,title,message,payload) VALUES (%s,%s,%s,%s,%s,%s,%s)', (event['id'], run_id, event['ts'], kind, title, message, json.dumps(extra)))
    await streams[run_id].put(event)
    metrics['events_emitted'] += 1
    log.info(json.dumps({'event': 'workflow_event', 'run_id': run_id, 'kind': kind, 'title': title, 'graph_state': extra.get('graph_state')}))

async def call(run_id, system, operation, payload, *, fail=False, retry=False):
    await emit(run_id, 'api', f'{system} request started', f'{operation} request is in progress', system=system, operation=operation, status='in_progress', payload=payload)
    await asyncio.sleep(.55)
    if fail:
        await emit(run_id, 'api', f'{system} request failed', 'Adapter timeout; retry policy engaged', system=system, operation=operation, status='failed', response={'error':'ETIMEDOUT','retryable':True})
        if retry:
            await asyncio.sleep(.45)
            await emit(run_id, 'api', f'{system} retry started', 'Circuit remains closed; retry 1/1', system=system, operation=operation, status='in_progress')
            await asyncio.sleep(.55)
        return False
    await emit(run_id, 'api', f'{system} request succeeded', '200 OK', system=system, operation=operation, status='success', response={'ok':True,'requestId':str(uuid.uuid4())})
    return True

async def legacy_workflow(run_id, scenario, kill_switch):
    try:
        await emit(run_id,'run','Workflow started',f'{scenario} scenario accepted', route='Redis Stream → Orchestrator')
        await asyncio.sleep(.3)
        await emit(run_id,'redis','Redis event consumed','A6.REMEDIATION.REQUEST consumed', stream='a6.events', payload={'sector':'DEL-042','mode':'bulk'})
        await emit(run_id,'agent','RF Scan Agent active','Analyzing RSSI, co-channel frequency and MCS rules', agent='RF Scan Agent', state='RUNNING')
        await call(run_id,'JIMS','GET /interference/candidates',{'sector':'DEL-042'})
        await emit(run_id,'sql','Azure SQL read','Candidate set loaded', operation='SELECT candidates', payload={'rows':12})
        await emit(run_id,'agent','Decision Agent active','MCS decision criteria passed; 4 remediations selected', agent='Decision Agent', state='RUNNING')
        await call(run_id,'LiteLLM → Azure OpenAI','POST /chat/completions',{'prompt':'summarize remediation rationale'})
        if kill_switch:
            await emit(run_id,'kill','Kill switch engaged','Workflow paused before non-idempotent remediation', state='PAUSED')
            await emit(run_id,'run','Final outcome','SAFE STOP — no external changes applied', outcome='paused')
            return
        if scenario == 'a2a-scan':
            await emit(run_id,'a2a','A2A path opened','External RF scan request routed to A2A HTTP server', route='A2A → RF Scan Agent', payload={'task':'scan_sector','sector':'DEL-042'})
            await call(run_id,'JIMS','A2A scan callback',{'taskId':str(uuid.uuid4())})
        await emit(run_id,'agent','Remediation Agent active','Outbox pre-log is being written before MMP PATCH', agent='Remediation Agent', state='RUNNING')
        await call(run_id,'HPSM','POST /incidents/prelog',{'sector':'DEL-042','change':'RF remediation'})
        await emit(run_id,'sql','Azure SQL write','Outbox + audit record committed', operation='INSERT outbox, audit_log', payload={'status':'PRE_LOGGED'})
        if scenario == 'api-failure':
            ok = await call(run_id,'MMP','PATCH /parameters',{'sector':'DEL-042'},fail=True,retry=True)
            await emit(run_id,'circuit','Circuit breaker opened','MMP failures exceeded threshold; TTL 30s', system='MMP', state='OPEN')
            await emit(run_id,'run','Final outcome','FAILED — remediation safely remains in outbox', outcome='failed')
            return
        await call(run_id,'MMP','PATCH /parameters',{'sector':'DEL-042','mcs':'QPSK-1/2'})
        if scenario == 'kpi-rollback':
            await emit(run_id,'kpi','KPI validation failed','Throughput delta outside tolerance; rollback initiated', payload={'throughputDelta':'-18%','threshold':'-5%'})
            await call(run_id,'MMP','PATCH /parameters/rollback',{'sector':'DEL-042','restore':'previous'})
            await emit(run_id,'reconcile','Rollback verified','Previous configuration restored and audit trail closed', state='COMPLETE')
            await emit(run_id,'run','Final outcome','ROLLED BACK — KPI guardrail protected service', outcome='rolled_back')
            return
        if scenario == 'a2a-crash':
            await emit(run_id,'a2a','A2A remediation crashed','Worker disconnected after MMP PATCH; operation is ambiguous', route='A2A → Remediation Agent', state='CRASHED')
            await asyncio.sleep(.4)
            await emit(run_id,'reconcile','Reconciliation started','Comparing HPSM pre-log with MMP current state', state='RUNNING')
            await call(run_id,'MMP','GET /parameters',{'sector':'DEL-042'})
            await emit(run_id,'reconcile','Reconciliation complete','MMP state matches intended change; duplicate write avoided', state='COMPLETE')
        else:
            await emit(run_id,'sql','Azure SQL write','Pipeline status updated: COMPLETED', operation='UPDATE pipeline_status', payload={'status':'COMPLETED'})
        await emit(run_id,'run','Final outcome','SUCCESS — remediation completed and audited', outcome='success')
    finally:
        await streams[run_id].put(None)

class GraphState(TypedDict, total=False):
    run_id: str
    scenario: str
    kill_switch: bool
    state: str
    outcome: str

async def graph_node(name: str, graph_state: GraphState, message: str, kind='agent', **extra):
    # Simulate observable node work so the SSE state progression is visible in the UI.
    await asyncio.sleep(0.65)
    await emit(graph_state['run_id'], kind, name, message, graph_node=name, graph_state=graph_state.get('state'), **extra)
    return graph_state

async def node_start(s):
    s['state'] = 'STARTED'; return await graph_node('Graph started', s, f"LangGraph accepted {s['scenario']}", 'run', route='START → ingest')
async def node_ingest(s):
    s['state'] = 'INGESTING'; return await graph_node('Redis event consumed', s, 'A6.REMEDIATION.REQUEST consumed', 'redis', route='ingest → scan', stream='a6.events', payload={'sector':'DEL-042','mode':'bulk'})
async def node_scan(s):
    s['state'] = 'SCANNING'; return await graph_node('RF Scan Agent active', s, 'Analyzing RSSI, co-channel frequency and MCS rules', agent='RF Scan Agent', state='RUNNING')
async def node_decide(s):
    s['state'] = 'DECIDING'; return await graph_node('Decision Agent active', s, 'MCS criteria passed; 4 remediations selected', agent='Decision Agent', state='RUNNING')
async def node_guard(s):
    if s['kill_switch']:
        s['state'] = 'PAUSED'; s['outcome'] = 'paused'
        await graph_node('Kill switch engaged', s, 'Workflow paused before non-idempotent remediation', 'kill', state='PAUSED')
        return s
    s['state'] = 'PRE_LOGGED'; return await graph_node('Remediation Agent active', s, 'Outbox pre-log written before non-idempotent remediation', agent='Remediation Agent', state='RUNNING')
async def node_remediate(s):
    if s.get('outcome'): return s
    if s['scenario'] == 'api-failure':
        s['state'] = 'RETRYING'; s['outcome'] = 'failed'; await graph_node('Retry policy engaged', s, 'MMP timeout; retry 1/1 and circuit breaker opened', 'circuit', state='OPEN'); return s
    s['state'] = 'REMEDIATING'; await graph_node('MMP remediation applied', s, 'Non-idempotent change committed', 'api', system='MMP', status='success'); return s
async def node_validate(s):
    if s.get('outcome'): return s
    if s['scenario'] == 'kpi-rollback':
        s['state'] = 'ROLLING_BACK'; s['outcome'] = 'rolled_back'; await graph_node('KPI rollback complete', s, 'Guardrail failed; previous configuration restored', 'reconcile', state='COMPLETE'); return s
    if s['scenario'] == 'a2a-crash':
        s['state'] = 'RECONCILING'; await graph_node('Reconciliation complete', s, 'MMP state matches intended change; duplicate write avoided', 'reconcile', state='COMPLETE')
    s['state'] = 'COMPLETED'; s['outcome'] = 'success'; return s
async def node_finalize(s):
    labels = {'success':'SUCCESS — remediation completed and audited','failed':'FAILED — remediation remains safely in outbox','rolled_back':'ROLLED BACK — KPI guardrail protected service','paused':'SAFE STOP — no external changes applied'}
    await graph_node('Final outcome', s, labels[s['outcome']], 'run', outcome=s['outcome']); return s

def route_after_guard(s): return 'finalize' if s.get('outcome') else 'remediate'
def route_after_remediate(s): return 'finalize' if s.get('outcome') else 'validate'

graph = StateGraph(GraphState)
for n, fn in [('start',node_start),('ingest',node_ingest),('scan',node_scan),('decide',node_decide),('guard',node_guard),('remediate',node_remediate),('validate',node_validate),('finalize',node_finalize)]: graph.add_node(n, fn)
graph.add_edge(START, 'start'); graph.add_edge('start','ingest'); graph.add_edge('ingest','scan'); graph.add_edge('scan','decide'); graph.add_edge('decide','guard')
graph.add_conditional_edges('guard', route_after_guard, {'finalize':'finalize','remediate':'remediate'}); graph.add_conditional_edges('remediate', route_after_remediate, {'finalize':'finalize','validate':'validate'}); graph.add_edge('validate','finalize'); graph.add_edge('finalize', END)
compiled_graph = graph.compile()

async def workflow(run_id, scenario, kill_switch):
    try:
        result = await compiled_graph.ainvoke({'run_id':run_id, 'scenario':scenario, 'kill_switch':kill_switch})
        outcome = result.get('outcome', 'success')
        update_run(run_id, 'completed', outcome)
        if outcome == 'failed': metrics['runs_failed'] += 1
        else: metrics['runs_completed'] += 1
    except Exception:
        update_run(run_id, 'failed', 'error')
        metrics['runs_failed'] += 1
        log.exception(json.dumps({'event':'workflow_failed','run_id':run_id}))
    finally: await streams[run_id].put(None)

@app.post('/api/runs', dependencies=[Depends(require_auth)])
async def start(req: RunRequest):
    run_id = str(uuid.uuid4()); streams[run_id] = asyncio.Queue()
    save_run(run_id, req.scenario, 'running', now(), now())
    metrics['runs_started'] += 1
    log.info(json.dumps({'event':'workflow_started','run_id':run_id,'scenario':req.scenario}))
    asyncio.create_task(workflow(run_id, req.scenario, req.kill_switch))
    return {'runId':run_id}

@app.get('/api/runs/{run_id}/events', dependencies=[Depends(require_auth)])
async def events(run_id: str):
    async def gen():
        q = streams.get(run_id)
        if not q: return
        while True:
            item = await q.get()
            if item is None: break
            yield f'data: {json.dumps(item)}\n\n'
    return StreamingResponse(gen(), media_type='text/event-stream', headers={'Cache-Control':'no-cache','X-Accel-Buffering':'no'})

@app.get('/api/runs/{run_id}', dependencies=[Depends(require_auth)])
def run_status(run_id: str):
    with sqlite3.connect(DB_PATH) as db:
        row = db.execute('SELECT run_id, scenario, status, created_at, updated_at, outcome FROM runs WHERE run_id=?', (run_id,)).fetchone()
    if not row: raise HTTPException(404, 'Run not found')
    return dict(zip(('runId','scenario','status','createdAt','updatedAt','outcome'), row))

@app.get('/metrics')
def prometheus_metrics():
    return PlainTextResponse('\n'.join(f'a6_{k} {v}' for k,v in metrics.items()) + '\n', media_type='text/plain; version=0.0.4')

@app.get('/api/health')
def health(): return {'status':'ok','service':'a6-orchestrator'}

frontend_dist = Path(__file__).resolve().parent.parent / 'frontend' / 'dist'
if frontend_dist.exists():
    app.mount('/', StaticFiles(directory=str(frontend_dist), html=True), name='frontend')
