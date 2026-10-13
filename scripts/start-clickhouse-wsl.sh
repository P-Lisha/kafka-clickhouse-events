#!/usr/bin/env bash
# Starts ClickHouse server inside the Ubuntu-24.04 WSL distro (no Docker).
# Also keeps the WSL2 VM alive with a background sleep, since WSL2 shuts its
# lightweight VM down ~8s after the last attached process exits, which would
# otherwise restart/kill clickhouse-server between commands.
set -e

MSYS_NO_PATHCONV=1 wsl -d Ubuntu-24.04 -u root -- bash -c "sleep 999999" &
echo "WSL keep-alive started (pid $!)"
sleep 2

MSYS_NO_PATHCONV=1 wsl -d Ubuntu-24.04 -u root -- service clickhouse-server start
sleep 2
MSYS_NO_PATHCONV=1 wsl -d Ubuntu-24.04 -u root -- service clickhouse-server status | head -5

echo
echo "ClickHouse should now be reachable from Windows at http://localhost:8123"
echo "  user: default / password: clickhouse123 (see users.d/default-password.xml)"
