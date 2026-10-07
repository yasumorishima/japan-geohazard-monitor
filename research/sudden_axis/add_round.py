# -*- coding: utf-8 -*-
"""Append the declared statistics of one pre-registered round to results.csv, read from the round's run log.

Usage: python add_round.py ROUND "Observation system" LOGFILE NAME=label [NAME=label ...]
  e.g. python add_round.py 299 "Outgoing longwave radiation" pre299_run.log TA=anomaly TS="spread ratio"
It reads the verdict lines the registered scripts print:
  "<NAME> pooled SA <auc> (pos <n>) | ... | <F> <mean> sd <sd> p <p> | ..."  and  "H(<NAME>): ... -> <verdict>"
takes the widest sd and the largest p over the verdict floors on that line, and refuses to write if a declared name
has no line, no verdict, or is already in results.csv for that round.  The figures are redrawn by the workflow.
"""
import csv
import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
CSV = os.path.join(HERE, "results.csv")
COLS = ("round", "system", "statistic", "auc", "floor_sd_max", "max_floor_p", "positives", "verdict", "log")

rnd, system, log = int(sys.argv[1]), sys.argv[2], sys.argv[3]
names = dict(a.split("=", 1) for a in sys.argv[4:])
assert names, "declare at least one NAME=label"
text = open(log, encoding="utf-8").read()
with open(CSV, newline="", encoding="utf-8") as f:
    old = list(csv.DictReader(f))
new = []
for nm, label in names.items():
    m = re.search(r"^%s pooled SA ([0-9.]+) \(pos (\d+)\)(.*)$" % re.escape(nm), text, re.M)
    assert m, ("no pooled SA line for", nm)
    floors = re.findall(r"\| ([A-Z]) [0-9.]+ sd ([0-9.]+) p ([0-9.]+)", m.group(3))
    assert floors, ("no floor entries for", nm)
    v = re.search(r"^H\(%s\):.*-> (.+)$" % re.escape(nm), text, re.M)
    assert v, ("no verdict line for", nm)
    assert not any(int(r["round"]) == rnd and r["statistic"] == label for r in old), ("already in results.csv", rnd, label)
    new.append(dict(round=rnd, system=system, statistic=label, auc=m.group(1),
                    floor_sd_max=max(floors, key=lambda x: float(x[1]))[1], max_floor_p=max(floors, key=lambda x: float(x[2]))[2],
                    positives=m.group(2), verdict=v.group(1).strip(), log=os.path.basename(log)))
with open(CSV, "a", newline="", encoding="utf-8") as f:
    w = csv.DictWriter(f, fieldnames=COLS)
    for r in new:
        w.writerow(r)
        print("added", r)
