#!/bin/sh
# Where the compat API's extra time goes: two CPU profiles of the SAME program.
#
#   sh bench/micro/prof-compat.sh [duration]
#
# The h2lean server arm, profiled twice -- once with the stream API and once
# with the compat API -- under the same h2load workload. A CPU profile reports
# shares, and shares are useless for comparing two programs; tools/cpuprofile-report.js
# normalises every self-time entry to ns per request, so the diff column is the
# real answer: what the compat path costs that the stream path does not.
#
# Only self time is used, and the comparison is between two profiles of the same
# program (h2lean against h2lean), which is the only kind the tool can answer.
set -e
cd "$(dirname "$0")/.."

dur=${1:-10}
out=out/prof-compat
rm -rf "$out"; mkdir -p "$out/stream" "$out/compat"
port=19010

run() {
  api=$1
  port=$((port+1))
  ROLE=server API="$api" PORT=$port DURATION=$((dur*1000+3000)) GC=1 \
    node --cpu-prof --cpu-prof-dir="$out/$api" bench/h2lean-h2.js > "$out/$api.server.txt" 2>&1 &
  srvpid=$!
  i=0
  while [ $i -lt 60 ]; do
    grep -q listening "$out/$api.server.txt" && break
    sleep 0.2
    i=$((i+1))
  done
  sleep 0.3
  h2load -c 64 -t 4 -m 32 --duration "$dur" -n 10000000 \
    "http://127.0.0.1:$port/" > "$out/$api.h2load.txt" 2>&1 || true
  wait $srvpid || true
  grep -oE "req=[0-9]+" "$out/$api.server.txt" | head -1
}

sr=$(run stream)
cr=$(run compat)
sreq=$(echo "$sr" | grep -oE "[0-9]+")
creq=$(echo "$cr" | grep -oE "[0-9]+")
echo "stream requests: $sreq   compat requests: $creq"

sp=$(ls "$out/stream"/*.cpuprofile | head -1)
cp=$(ls "$out/compat"/*.cpuprofile | head -1)
node tools/cpuprofile-report.js "stream:$sp:$sreq" "compat:$cp:$creq"
