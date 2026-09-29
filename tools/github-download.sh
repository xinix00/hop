#!/bin/sh
# De echte artifact-download van de bewoner, op de host: dezelfde
# `agentd_hopos::HttpImages` als in het slot (leanhttp, leantls met
# ketenverificatie op de ingebakken Mozilla-wortels, de redirect van
# github.com naar objects.githubusercontent.com), met std-sockets en de
# systeemklok eronder in plaats van appnet en SNTP. Groen als het antwoord
# 200 is, de lengte klopt en de bytes met een ELF-kop beginnen.
#
#   sh tools/github-download.sh                   de welcome-app van HopOS
#   sh tools/github-download.sh <https-url>       een andere release-asset
#
# Heeft internet nodig; draait daarom niet in tools/gate.sh.
set -eu
cd "$(dirname "$0")/.."
if [ $# -gt 0 ]; then
	HOP_TEST_URL="$1"
	export HOP_TEST_URL
fi
exec cargo test -p agentd-hopos --lib real_github_release_download -- --ignored --nocapture
