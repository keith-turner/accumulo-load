#!/bin/bash

TESTS_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

cd $TESTS_DIR/..

(
  echo "deletetable -f wct"
  echo "createtable wct"
  echo "config -t wct -s table.cache.index.enable=true"
  echo "config -t wct -s table.cache.block.enable=true"
  echo "config -t wct -s table.cache.warm=true"
  echo "config -t wct -s table.file.compress.type=snappy"
) | accumulo shell -u root -p secret

./bin/run.sh cmd.Ingest wct 1000000 20 10 > results/ingest.out 2>results/ingest.err &

accumulo shell -u root -p secret -e 'flush -t wct -w'

mkdir -p results

./bin/run.sh cmd.Compact wct 60000 > results/compact.out 2>results/compact.err &

./bin/run.sh cmd.Scan wct 6000 20 100 16 > results/scan.out 2>results/scan.err

pkill -f cmd.Compact

cat results/compact.out results/scan.out | ./bin/run.sh cmd.Summarize > results/summary.csv 2>results/summary.err
