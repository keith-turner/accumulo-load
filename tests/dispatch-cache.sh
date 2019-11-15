#!/bin/bash

TESTS_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

cd $TESTS_DIR/..

(
  echo "deletetable -f dc"
  echo "createtable dc"
  echo "config -s tserver.scan.executors.bge.threads=1"
  echo "config -t dc -s table.scan.dispatcher.opts.executor.background=bge"
  echo "config -t dc -s table.scan.dispatcher.opts.cache.background=opportunistic"
  echo "config -t dc -s table.cache.index.enable=true"
  echo "config -t dc -s table.cache.block.enable=true"
  echo "config -t dc -s table.file.compress.type=snappy"
) | accumulo shell -u root -p secret

./bin/run.sh cmd.Ingest dc 1000000 20 10 > results/ingest.out 2>results/ingest.err

accumulo shell -u root -p secret -e 'flush -t dc -w'

# give time for any splits
sleep 3

mkdir -p results

./bin/run.sh cmd.FullScan dc 3 > results/full-scan.out 2>results/full-scan.err &

./bin/run.sh cmd.Scan dc 6000 20 100 16 > results/scan.out 2>results/scan.err

pkill -f cmd.FullScan

