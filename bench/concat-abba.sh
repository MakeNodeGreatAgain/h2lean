#!/bin/sh
# Paired A/B/B/A of the corked _writev baseline against the Buffer.concat arm, in one
# session, matrix.sh's client shape (64 clients x 8 threads x 8 s), GC=1 so the collector
# share is reported. Cases: GET (single-chunk control -- the concat path never fires),
# down32k and down256k (the multi-chunk downloads where the two strategies differ).
# Each round runs baseline -> concat -> concat -> baseline so drift is bracketed.
out=/tmp/concat-abba
mkdir -p "$out"
res="$out/results.txt"
: > "$res"

one() { # name concat?(0/1) extra-args
  name=$1; cc=$2; shift 2
  if [ "$cc" = "1" ]; then arm=concat; else arm=writev; fi
  line=$(CONCAT=$cc GC=1 PRELOAD=bench/concat-write.js node bench/h2load.js \
      --server=h2lite --api=stream --clients=64 --threads=8 --duration=8 "$@" 2>&1)
  cpu=$(echo "$line" | sed -n 's/.*cpuPerReq=\([0-9.]*\)us.*/\1/p')
  rps=$(echo "$line" | sed -n 's/.*h2load:  \([0-9.]*\) req\/s.*/\1/p')
  gcpct=$(echo "$line" | sed -n 's/.*gcPct=\([0-9.]*\)%.*/\1/p')
  gc=$(echo "$line" | sed -n 's/.*gc=\([0-9.]*\)ms.*/\1/p')
  rss=$(echo "$line" | sed -n 's/PEAK_RSS=\([0-9.]*\)MB/\1/p')
  multi=$(echo "$line" | sed -n 's/.*multi=\([0-9]*\).*/\1/p')
  echo "$name $arm cpu=$cpu rps=$rps gcPct=$gcPct gc=$gc peakRSS=$rss multi=$multi" | tee -a "$res"
}

for r in 1 2 3; do
  echo "--- round $r" | tee -a "$res"
  one GET 0
  one GET 1
  one GET 1
  one GET 0
  one down32k 0 --resp=32768
  one down32k 1 --resp=32768
  one down32k 1 --resp=32768
  one down32k 0 --resp=32768
  one down256k 0 --resp=262144
  one down256k 1 --resp=262144
  one down256k 1 --resp=262144
  one down256k 0 --resp=262144
done
