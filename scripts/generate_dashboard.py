#!/usr/bin/env python3
"""Build a Grafana dashboard from the endpoint registry of a running server."""

import argparse
import json
import time
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


DEFAULT_OUTPUT = (
    Path(__file__).resolve().parents[1]
    / "grafana"
    / "dashboards"
    / "endpoints.json"
)


def fetch_endpoints(server_url):
    request = Request(
        server_url.rstrip("/") + "/endpoints",
        headers={"Accept": "application/json"},
    )
    with urlopen(request, timeout=10) as response:
        endpoints = json.load(response)

    if not isinstance(endpoints, list):
        raise ValueError("endpoint registry must be a JSON array")
    seen = set()
    for endpoint in endpoints:
        if not isinstance(endpoint, dict):
            raise ValueError("each endpoint must contain method and path")
        method = endpoint.get("method")
        path = endpoint.get("path")
        if not isinstance(method, str) or not method:
            raise ValueError("each endpoint must have an HTTP method")
        if not isinstance(path, str) or not path.startswith("/"):
            raise ValueError("each endpoint must have an absolute path template")
        identity = (method, path)
        if identity in seen:
            raise ValueError(f"duplicate endpoint: {method} {path}")
        seen.add(identity)
    return sorted(endpoints, key=lambda endpoint: (endpoint["path"], endpoint["method"]))


def wait_for_endpoints(server_url, wait_seconds):
    deadline = time.monotonic() + wait_seconds
    while True:
        try:
            return fetch_endpoints(server_url)
        except (HTTPError, URLError, OSError):
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise
            time.sleep(min(1, remaining))


def panel(panel_id, title, x, y, unit, fill_opacity, targets, datasource):
    return {
        "id": panel_id,
        "title": title,
        "type": "timeseries",
        "datasource": datasource,
        "gridPos": {"h": 9, "w": 12, "x": x, "y": y},
        "fieldConfig": {
            "defaults": {
                "unit": unit,
                "min": 0,
                "custom": {
                    "drawStyle": "line",
                    "lineWidth": 2,
                    "fillOpacity": fill_opacity,
                },
            },
            "overrides": [],
        },
        "options": {
            "legend": {"displayMode": "list", "placement": "bottom", "showLegend": True},
            "tooltip": {"mode": "multi"},
        },
        "targets": targets,
    }


def endpoint_panels(index, endpoint, datasource, service_selector):
    method = endpoint["method"]
    path = endpoint["path"]
    title = f"{method} {path}"
    selector = (
        f"{service_selector},path={json.dumps(path)},method={json.dumps(method)},"
        'dc=~"${dc:regex}",pod=~"${pod:regex}"'
    )
    requests = [
        {
            "refId": "A",
            "expr": f"sum(rate(http_requests_total{{{selector}}}[$__rate_interval]))",
            "datasource": datasource,
            "legendFormat": "requests/s",
        },
        {
            "refId": "B",
            "expr": (
                f"sum(rate(http_requests_total{{{selector},"
                'code=~"4..|5.."}[$__rate_interval]))'
            ),
            "datasource": datasource,
            "legendFormat": "errors/s",
        },
    ]
    bucket_rates = (
        "sum by (le) "
        f"(rate(http_request_duration_seconds_bucket{{{selector}}}"
        "[$__rate_interval]))"
    )
    latency = [
        {
            "refId": chr(ord("A") + target_index),
            "expr": f"histogram_quantile({quantile}, {bucket_rates})",
            "datasource": datasource,
            "legendFormat": f"p{int(quantile * 100)}",
        }
        for target_index, quantile in enumerate((0.50, 0.75, 0.90, 0.95, 0.99))
    ]
    y = index * 9
    return [
        panel(
            2 * index + 1,
            f"{title} - requests & errors",
            0,
            y,
            "reqps",
            25,
            requests,
            datasource,
        ),
        panel(2 * index + 2, f"{title} - latency", 12, y, "s", 0, latency, datasource),
    ]


def variable(name, label, query, datasource):
    return {
        "name": name,
        "label": label,
        "type": "query",
        "datasource": datasource,
        "definition": query,
        "query": {"query": query, "refId": f"{name}-variable"},
        "refresh": 1,
        "sort": 1,
        "multi": True,
        "includeAll": True,
        "allValue": ".*",
        "current": {"text": "All", "value": "$__all"},
        "options": [],
    }


def build_dashboard(endpoints, datasource_uid, service):
    datasource = {"type": "prometheus", "uid": datasource_uid}
    service_selector = f"service={json.dumps(service)}"
    dc_query = f"label_values(http_requests_total{{{service_selector}}}, dc)"
    pod_query = (
        f"label_values(http_requests_total{{{service_selector},"
        'dc=~"${dc:regex}"}, pod)'
    )
    if not endpoints:
        raise ValueError("endpoint registry is empty")
    panels = [
        current_panel
        for index, endpoint in enumerate(endpoints)
        for current_panel in endpoint_panels(index, endpoint, datasource, service_selector)
    ]

    return {
        "uid": "ddia-1-endpoints",
        "title": "ddia-1 Endpoints",
        "tags": ["ddia-1", "generated"],
        "editable": False,
        "templating": {
            "list": [
                variable("dc", "Data center", dc_query, datasource),
                variable("pod", "Pod", pod_query, datasource),
            ]
        },
        "timezone": "browser",
        "schemaVersion": 39,
        "version": 1,
        "refresh": "15s",
        "time": {"from": "now-1h", "to": "now"},
        "panels": panels,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--server-url", default="http://127.0.0.1:3030", help="running backend URL"
    )
    parser.add_argument(
        "--output", type=Path, default=DEFAULT_OUTPUT, help="dashboard JSON path"
    )
    parser.add_argument(
        "--datasource-uid", default="victoriametrics", help="Grafana data source UID"
    )
    parser.add_argument(
        "--service", default="ddia-1", help="Prometheus service label"
    )
    parser.add_argument(
        "--wait-seconds", type=float, default=0, help="wait for the backend to start"
    )
    args = parser.parse_args()
    if args.wait_seconds < 0:
        parser.error("--wait-seconds must be nonnegative")

    try:
        endpoints = wait_for_endpoints(args.server_url, args.wait_seconds)
        dashboard = build_dashboard(endpoints, args.datasource_uid, args.service)
        args.output.parent.mkdir(parents=True, exist_ok=True)
        temporary = args.output.with_name(args.output.name + ".tmp")
        temporary.write_text(json.dumps(dashboard, indent=2) + "\n", encoding="utf-8")
        temporary.replace(args.output)
    except (HTTPError, URLError, OSError, ValueError) as error:
        parser.exit(1, f"error: {error}\n")

    print(f"Wrote {len(dashboard['panels'])} panels to {args.output}")


if __name__ == "__main__":
    main()
