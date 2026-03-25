#!/bin/bash

set -e

# Compute defaults: last two benchmarks.
DEF=$(ls -ltr docs/profile/bench-* | awk '{ print $9 }' | tail -2)
OLD_DEF=$(echo "${DEF}" | sed -n '1p')
NEW_DEF=$(echo "${DEF}" | sed -n '2p')

# Otherwise take from args.
case "$#" in
    0)
        OLD="${OLD_DEF}"
        NEW="${NEW_DEF}"
        ;;
    1)
        OLD="${OLD_DEF}"
        NEW="${1}"
        ;;
    *)
        OLD="${1}"
        NEW="${2}"
        ;;
esac

echo "Comparing old=${OLD} new=${NEW}"
go tool benchstat <(grep -v 'Stats:' "${OLD}" | grep '^Benchmark') \
    <(grep -v 'Stats:' "${NEW}" | grep '^Benchmark')
