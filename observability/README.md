# Local observability

Start the stack with:

```powershell
docker compose up -d app prometheus grafana
```

Open:

- Prometheus: http://localhost:9090
- Grafana: http://localhost:3000
- App metrics: http://localhost:8000/metrics

The app writes structured JSON logs to container stdout:

```powershell
docker compose logs -f app
```

Prometheus scrapes the app every five seconds and evaluates the local alert rules in `alerts.yml`.
