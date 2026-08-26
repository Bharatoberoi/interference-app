import concurrent.futures
import json
import urllib.request

URL = "http://127.0.0.1:8000/api/runs"
SCENARIOS = ["bulk-success", "api-failure", "kpi-rollback", "a2a-crash"]

def send(index: int) -> int:
    body = json.dumps({"scenario": SCENARIOS[index % len(SCENARIOS)], "kill_switch": False}).encode()
    request = urllib.request.Request(URL, data=body, headers={"Content-Type": "application/json"}, method="POST")
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            return response.status
    except Exception:
        return 0

with concurrent.futures.ThreadPoolExecutor(max_workers=40) as pool:
    results = list(pool.map(send, range(1000)))

print({"requests": len(results), "http_200": results.count(200), "http_failed": sum(x != 200 for x in results)})
