#!/bin/sh
set -eu

# Keep an existing data volume in sync with the managed bootstrap credential.
if [ -f /var/lib/grafana/grafana.db ]; then
    grafana cli --homepath /usr/share/grafana admin reset-admin-password \
        --password-from-stdin < /run/secrets/grafana/admin-password
fi

exec /run.sh
