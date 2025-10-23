# export_edges_for_networkx.py
# Usage:
#   pip install pymongo pandas
#   python export_edges_for_networkx.py
# Writes edges.csv and users.csv to the ../data/ directory.

import csv
from pymongo import MongoClient

MONGO_URI = "mongodb://127.0.0.1:27017"  # MongoDB local host
DB_NAME = "minor_proj"

client = MongoClient(MONGO_URI)
db = client[DB_NAME]

# ----------------------------
# Export edges
# ----------------------------
with open("../data/edges.csv", "w", newline="", encoding="utf-8") as f:
    w = csv.writer(f)
    w.writerow(["src", "dst", "type", "weight"])
    for doc in db.edges.find({}, {"src": 1, "dst": 1, "type": 1, "weight": 1}):
        w.writerow([
            str(doc["src"]),
            str(doc["dst"]),
            doc.get("type", "friend"),
            float(doc.get("weight", 1.0))
        ])

# ----------------------------
# Export users
# ----------------------------
with open("../data/users.csv", "w", newline="", encoding="utf-8") as f:
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
    for u in db.users.find({}):
        loc = u.get("location", {})
        langs = [lang.get("code", "") for lang in u.get("languages", [])]
        interests = u.get("interests", [])
        interests = ",".join(interests) if isinstance(interests, list) else interests
        w.writerow([
            str(u["_id"]),
            u.get("name", ""),
            u.get("age", ""),
            u.get("gender", ""),
            loc.get("city", ""),
            loc.get("state", ""),
            loc.get("country", ""),
            u.get("primaryLang", ""),
            ",".join(langs),
            u.get("joinedAt", ""),
            u.get("education", ""),
            u.get("profession", ""),
            interests,
            u.get("purpose", ""),
            u.get("thirdParty", False),
            u.get("community", "")
        ])

print("✅ Wrote edges.csv and users.csv to ../data/")
