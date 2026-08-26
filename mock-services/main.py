import os, uuid
from datetime import datetime, timezone
from fastapi import FastAPI, HTTPException, Header

service = os.getenv('SERVICE_NAME', 'jims')
app = FastAPI(title=f'Local {service.upper()} Mock')

def auth(authorization: str | None):
    expected = {'jims':'jims-local:JimsLocal!2026','mmp':'mmp-local:MmpLocal!2026','hpsm':'hpsm-local:HpsmLocal!2026'}.get(service)
    if expected and authorization not in (f'Basic {expected}', 'Bearer local-dev-token'):
        raise HTTPException(401, 'Use local mock credentials')

@app.get('/health')
def health(): return {'status':'ok','service':service,'local_only':True}

@app.get('/interference/candidates')
def candidates(sector: str, authorization: str | None = Header(default=None)):
    auth(authorization)
    return {'sector':sector,'candidates':[{'cell':f'{sector}-C1','rssi_dbm':-92,'co_channel_interference':True,'mcs':'16QAM'}, {'cell':f'{sector}-C2','rssi_dbm':-71,'co_channel_interference':False,'mcs':'64QAM'}]}

@app.get('/telemetry')
def telemetry(sector: str, authorization: str | None = Header(default=None)):
    return {'sector':sector,'measurements':[{'timestamp':datetime.now(timezone.utc).isoformat(),'cell':f'{sector}-C1','rssi_dbm':-92,'sinr_db':3.1,'throughput_mbps':18.4,'co_channel_interference':True}]}

@app.patch('/parameters')
def parameters(body: dict, authorization: str | None = Header(default=None)):
    auth(authorization)
    return {'ok':True,'requestId':str(uuid.uuid4()),'appliedAt':datetime.now(timezone.utc).isoformat(),'change':body}

@app.patch('/parameters/rollback')
def rollback(body: dict, authorization: str | None = Header(default=None)):
    auth(authorization); return {'ok':True,'requestId':str(uuid.uuid4()),'restored':body.get('restore','previous')}

@app.post('/incidents/prelog', status_code=201)
def prelog(body: dict, authorization: str | None = Header(default=None)):
    auth(authorization); return {'incidentId':str(uuid.uuid4()),'status':'PRE_LOGGED','request':body}
