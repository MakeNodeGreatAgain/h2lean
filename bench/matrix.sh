#!/bin/sh
# Runs one fixed h2load matrix so that numbers taken before and after a change
# are directly comparable. Every case uses the same client shape (64 clients,
# 8 threads, 8 s), and both the throughput line and the server's own cpuPerReq
# line are kept, because a req/s delta taken under differing load means
# little while cpuPerReq is measured inside the server process.
#
#   sh bench/matrix.sh <label>        -> /tmp/mx-<label>/<case>.txt
#
# Every h2lean case is paired with the same case on node:http2. The pairs are not
# only the parity answer -- the native half is the drift meter. The same native
# code measured minutes apart moves 8-13%, so an unpaired before/after run cannot
# tell a regression from run-to-run noise; only the paired number can.
#
# Uploads carry a request body, and bench/h2load.js then sets READBODY=1, so both
# servers actually read it. Without that, node answers and resets the unread half
# (submitRstStream was ~38% of its CPU) and the upload numbers mean nothing.
# Bodies are taken both below and above the 64 KB default window, because only
# above it does flow control get exercised on the server.
label=$1
out=/tmp/mx-$label
mkdir -p "$out"

run() {
  name=$1
  shift
  node bench/h2load.js "$@" --clients=64 --threads=8 --duration=8 > "$out/$name.txt" 2>&1
  echo "--- $name"
  grep -E "h2load:|server:" "$out/$name.txt"
}

run plain-stream       --server=h2lean --api=stream
run native-plain-strm  --server=native --api=stream
run plain-compat       --server=h2lean --api=compat
run native-plain-compt --server=native --api=compat
run plain-up32k        --server=h2lean --api=stream --body=32768
run native-plain-up32k --server=native --api=stream --body=32768
run plain-up128k       --server=h2lean --api=stream --body=131072
run native-plain-up128 --server=native --api=stream --body=131072
run plain-down32k      --server=h2lean --api=stream --resp=32768
run native-plain-dn32k --server=native --api=stream --resp=32768
run plain-down256k     --server=h2lean --api=stream --resp=262144
run native-plain-dn256 --server=native --api=stream --resp=262144
run tls-stream         --server=h2lean --api=stream --tls
run native-tls-stream  --server=native --api=stream --tls
run tls-up32k          --server=h2lean --api=stream --body=32768 --tls
run native-tls-up32k   --server=native --api=stream --body=32768 --tls
