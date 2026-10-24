#!/usr/bin/env python3
"""Prepare Grafana credentials and run the dashboard CLI with a service account token."""

import argparse
import base64
import os
import secrets
import subprocess
import sys
from pathlib import Path
from urllib.error import HTTPError, URLError

from generate_dashboard import grafana_request


SECRETS_DIR = Path("/run/secrets/grafana")
ACCOUNT_NAME = "ddia-dashboard-generator"
TOKEN_PREFIX = "dashboard-generator-"


def initialize_password():
    SECRETS_DIR.mkdir(parents=True, exist_ok=True)
    SECRETS_DIR.chmod(0o750)
    password_file = SECRETS_DIR / "admin-password"
    if not password_file.exists():
        descriptor = os.open(password_file, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o440)
        with os.fdopen(descriptor, "w", encoding="utf-8") as output:
            os.fchown(output.fileno(), 472, 0)  # Grafana's container UID and group.
            output.write(secrets.token_urlsafe(32) + "\n")
    if not password_file.read_text(encoding="utf-8").strip():
        raise ValueError("Grafana admin password file is empty")


def configure(grafana_url, server_url):
    password = (SECRETS_DIR / "admin-password").read_text(encoding="utf-8").strip()
    credentials = base64.b64encode(f"admin:{password}".encode("utf-8")).decode("ascii")

    def admin_request(path, method="GET", data=None):
        return grafana_request(grafana_url, path, f"Basic {credentials}", method, data)

    accounts = admin_request(f"/api/serviceaccounts/search?query={ACCOUNT_NAME}")
    account = next(
        (account for account in accounts["serviceAccounts"] if account["name"] == ACCOUNT_NAME),
        None,
    )
    if account is None:
        account = admin_request(
            "/api/serviceaccounts", "POST", {"name": ACCOUNT_NAME, "role": "Editor"}
        )
    elif account["role"] != "Editor" or account["isDisabled"]:
        admin_request(
            f"/api/serviceaccounts/{account['id']}",
            "PATCH",
            {"role": "Editor", "isDisabled": False},
        )

    tokens_path = f"/api/serviceaccounts/{account['id']}/tokens"
    previous_tokens = admin_request(tokens_path)
    token = admin_request(
        tokens_path,
        "POST",
        {"name": TOKEN_PREFIX + secrets.token_hex(8), "secondsToLive": 86400},
    )
    token_file = SECRETS_DIR / "service-account-token"
    temporary = token_file.with_suffix(".tmp")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(descriptor, "w", encoding="utf-8") as output:
        output.write(token["key"] + "\n")
    temporary.replace(token_file)

    subprocess.run(
        [
            sys.executable,
            str(Path(__file__).with_name("generate_dashboard.py")),
            "--server-url", server_url,
            "--grafana-url", grafana_url,
            "--grafana-token-file", str(token_file),
            "--wait-seconds", "60",
        ],
        check=True,
    )
    preferences = admin_request("/api/org/preferences")
    preferences["homeDashboardUID"] = "ddia-1-endpoints"
    admin_request("/api/org/preferences", "PUT", preferences)
    for previous in previous_tokens:
        if previous["name"].startswith(TOKEN_PREFIX):
            admin_request(f"{tokens_path}/{previous['id']}", "DELETE")
    print("Configured Grafana service account and home dashboard")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--init-secrets", action="store_true")
    parser.add_argument("--grafana-url", default="http://grafana:3000")
    parser.add_argument("--server-url", default="http://backend:3030")
    args = parser.parse_args()
    os.umask(0o077)
    try:
        if args.init_secrets:
            initialize_password()
        else:
            configure(args.grafana_url, args.server_url)
    except (HTTPError, URLError, OSError, ValueError, subprocess.CalledProcessError) as error:
        parser.exit(1, f"error: {error}\n")


if __name__ == "__main__":
    main()
