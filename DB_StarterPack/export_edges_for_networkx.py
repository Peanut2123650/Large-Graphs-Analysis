# export_edges_for_networkx.py
# Usage:
#   pip install pymongo
#   python export_edges_for_networkx.py
# Writes edges.csv and users.csv to the ../data/ directory.
# Minimal, robust, streaming version (keeps same columns as original).

import os
import csv
from pymongo import MongoClient

MONGO_URI = "mongodb://127.0.0.1:27017"  # MongoDB local host
DB_NAME = "minor_proj"
OUT_DIR = os.path.join("..", "data")
EDGE_OUT = os.path.join(OUT_DIR, "edges.csv")
USER_OUT = os.path.join(OUT_DIR, "users.csv")

# Tuneable
CURSOR_BATCH_SIZE = 5000
EDGE_PROGRESS_EVERY = 50000
USER_PROGRESS_EVERY = 5000

os.makedirs(OUT_DIR, exist_ok=True)

client = MongoClient(MONGO_URI)
db = client[DB_NAME]

# ----------------------------
# Export edges (streamed)
# ----------------------------
print("Exporting edges to:", EDGE_OUT)
edge_projection = {"src": 1, "dst": 1, "type": 1, "weight": 1}
cursor = db.edges.find({}, edge_projection).batch_size(CURSOR_BATCH_SIZE)

written_edges = 0
with open(EDGE_OUT, "w", newline="", encoding="utf-8") as f:
    w = csv.writer(f)
    w.writerow(["src", "dst", "type", "weight"])
    for doc in cursor:
        src = doc.get("src")
        dst = doc.get("dst")
        if src is None or dst is None:
            continue
        etype = doc.get("type", "friend")
        try:
            weight = float(doc.get("weight", 1.0))
        except Exception:
            weight = 1.0
        w.writerow([str(src), str(dst), etype, weight])
        written_edges += 1
        if written_edges % EDGE_PROGRESS_EVERY == 0:
            f.flush()
            print(f"  edges written: {written_edges}")
    f.flush()

print("Finished exporting edges. total written:", written_edges)

# ----------------------------
# Export users (streamed)
# ----------------------------
print("Exporting users to:", USER_OUT)
user_cursor = db.users.find({}).batch_size(CURSOR_BATCH_SIZE)

written_users = 0
with open(USER_OUT, "w", newline="", encoding="utf-8") as f:
    w = csv.writer(f)
    w.writerow([
        "_id",
        "name",
        "age",
        "gender",
        "city",
        "state",
        "country",
        "primaryLang",
        "languages",        # comma-separated codes
        "joinedAt",
        "education",
        "profession",
        "interests",        # comma-separated interests
        "purpose",
        "thirdParty",
        "community"
    ])
    for u in user_cursor:
        loc = u.get("location", {}) or {}
        # handle languages stored as list of dicts or list of codes
        langs = u.get("languages", [])
        if isinstance(langs, list):
            lang_codes = []
            for item in langs:
                if isinstance(item, dict):
                    code = item.get("code", "")
                    if code: lang_codes.append(code)
                elif isinstance(item, str):
                    lang_codes.append(item)
            langs_str = ",".join(lang_codes)
        else:
            langs_str = str(langs)

        interests = u.get("interests", [])
        if isinstance(interests, list):
            interests_str = ",".join([str(x) for x in interests])
        else:
            interests_str = str(interests)

        w.writerow([
            str(u.get("_id", "")),
            u.get("name", ""),
            u.get("age", ""),
            u.get("gender", ""),
            loc.get("city", ""),
            loc.get("state", ""),
            loc.get("country", ""),
            u.get("primaryLang", ""),
            langs_str,
            u.get("joinedAt", ""),
            u.get("education", ""),
            u.get("profession", ""),
            interests_str,
            u.get("purpose", ""),
            u.get("thirdParty", False),
            u.get("community", "")
        ])
        written_users += 1
        if written_users % USER_PROGRESS_EVERY == 0:
            f.flush()
            print(f"  users written: {written_users}")
    f.flush()

print("Finished exporting users. total written:", written_users)
print("✅ Wrote edges.csv and users.csv to", OUT_DIR)
