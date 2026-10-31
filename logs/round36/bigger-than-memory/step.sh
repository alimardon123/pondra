#!/bin/bash
# One git-bisect step: build this commit, then run two spilling queries under 2 GB with the 10-16 build (3be8783) and
# this one back to back, each on a hardlinked copy of the 10-16 lake. The sandbox's speed drifts, so only the ratio
# counts: good under 1.25, bad over 1.45, between: once more, then skip.
set -u
cd /tmp/l1/wt-main
c=$(git rev-parse --short HEAD)
echo "== $c $(date -u +%H:%M:%S)" >> /tmp/l1/bisect/log
CARGO_TARGET_DIR=/tmp/l1/target cargo build --release > /tmp/l1/bisect/build-$c.log 2>&1 || { echo "  build failed: skip" >> /tmp/l1/bisect/log; exit 125; }
cp /tmp/l1/target/release/pondra /tmp/l1/bisect/pondra-step
cd /tmp/l1/big
q() {  # bin out port: the two queries' summed seconds, or "no"
  rm -rf /tmp/l1/bisect/lake && cp -al /tmp/l1/lt-old /tmp/l1/bisect/lake && rm -f "$2"
  python3 pbig.py --bin "$1" --data /tmp/l1/sf10 --lake /tmp/l1/bisect/lake --cap 2 --only groupby_60m_ints,join3_group_1m --port "$3" --env PONDRA_SPILL_GB=8 --out "$2" >> /tmp/l1/bisect/run-$c.log 2>&1
  python3 -c "
import json
r=[json.loads(l) for l in open('$2')]
print(round(sum(x['secs'] for x in r),1) if len(r)==2 and all(x['status']=='ok' for x in r) else 'no')"
}
for try in 1 2; do
  old=$(q /tmp/l1/pondra-3be8783 /tmp/l1/bisect/old.jsonl 7900)
  new=$(q /tmp/l1/bisect/pondra-step /tmp/l1/bisect/new.jsonl 7910)
  echo "  try $try: 10-16 build ${old} s, this ${new} s" >> /tmp/l1/bisect/log
  { [ "$old" = no ] || [ "$new" = no ]; } && { echo "  no answer: skip" >> /tmp/l1/bisect/log; exit 125; }
  v=$(python3 -c "r=$new/$old; print('good' if r<1.25 else 'bad' if r>1.45 else 'unclear')")
  echo "  ratio $(python3 -c "print(round($new/$old,2))"): $v" >> /tmp/l1/bisect/log
  [ $v = good ] && exit 0
  [ $v = bad ] && exit 1
done
exit 125
