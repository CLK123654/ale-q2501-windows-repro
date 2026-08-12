#!/usr/bin/env bash
set -euo pipefail

repo=$1
iface=$(ip route show default | awk '{print $5; exit}')
test -n "$iface"
sudo ip link set "$iface" down
ip -j link show "$iface" > "$repo/evidence/network-guard.json"
