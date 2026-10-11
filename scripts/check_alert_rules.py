#!/usr/bin/env python3
"""Fail if an alert/recording rule references a metric no Pharos service registers.

Usage: scripts/check_alert_rules.py [rules.yml ...]   (default: observability/prometheus/alerts.yml)

Registered names are read from the `Name:` fields in internal/metrics/*/*.go.
Prometheus's own `up` is also allowed. Histogram series suffixes
(_bucket/_sum/_count) resolve to their base metric. Also checks that every
rule has the annotations the runbooks wiring depends on and that each
runbook_url points at a file that exists in docs/runbooks/.
"""
import glob, os, re, sys

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
try:
    import yaml
except ImportError:
    sys.exit("PyYAML is required: pip install pyyaml")

registered = set()
for f in glob.glob(os.path.join(ROOT, "internal/metrics/*/*.go")):
    if f.endswith("_test.go"):
        continue
    registered |= set(re.findall(r'Name:\s*"(pharos_[a-z_]+)"', open(f).read()))
if not registered:
    sys.exit("found no registered metrics under internal/metrics")
allowed = registered | {"up"}

files = sys.argv[1:] or [os.path.join(ROOT, "observability/prometheus/alerts.yml")]
problems = []
for path in files:
    doc = yaml.safe_load(open(path))
    for group in doc["groups"]:
        for rule in group["rules"]:
            name = rule.get("alert") or rule.get("record")
            expr = rule["expr"]
            # strip label matchers and quoted strings so label values are not read as metrics
            bare = re.sub(r"\{[^}]*\}", "", expr)
            bare = re.sub(r'"[^"]*"', "", bare)
            bare = re.sub(r"\b(by|without)\s*\([^)]*\)", "", bare)
            funcs = {"rate", "increase", "sum", "by", "histogram_quantile", "and", "or", "without", "avg", "max", "min", "time", "le"}
            for ident in set(re.findall(r"[a-zA-Z_:][a-zA-Z0-9_:]*", bare)):
                if ident in funcs or not ("_" in ident or ident == "up"):
                    continue
                base = re.sub(r"_(bucket|sum|count)$", "", ident)
                if ident not in allowed and base not in allowed:
                    problems.append("%s: rule %s references unknown metric %s" % (path, name, ident))
            if "alert" in rule:
                ann = rule.get("annotations", {})
                for k in ("summary", "description", "runbook_url"):
                    if k not in ann:
                        problems.append("%s: alert %s lacks annotation %s" % (path, name, k))
                url = ann.get("runbook_url", "")
                m = re.search(r"/docs/runbooks/([a-z0-9-]+\.md)$", url)
                if not m or not os.path.exists(os.path.join(ROOT, "docs/runbooks", m.group(1))):
                    problems.append("%s: alert %s runbook_url does not resolve to a file in docs/runbooks/: %s" % (path, name, url))
if problems:
    print("\n".join(problems))
    sys.exit(1)
print("ok: %d registered metrics; %d file(s) checked" % (len(registered), len(files)))
