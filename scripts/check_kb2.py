# -*- coding: utf-8 -*-
"""Check KB coverage for capacity compensation and FR pricing data."""
import sys
import psycopg2

sys.stdout.reconfigure(encoding='utf-8')

pg_url = "postgresql://postgres:!BESSmap2026@bess-platform-pg.cjs000o4wn2w.ap-southeast-1.rds.amazonaws.com:5432/marketdata"
conn = psycopg2.connect(pg_url, connect_timeout=15)
cur = conn.cursor()

print("=== Docs with FR-pricing chunks ===")
cur.execute("""
  SELECT d.file_name, d.region_province, COUNT(c.id) as cnt
  FROM staging.spot_knowledge_docs d
  JOIN staging.spot_knowledge_chunks c ON c.doc_id = d.id
  WHERE c.chunk_text ILIKE '%调频容量%' OR c.chunk_text ILIKE '%调频价格%'
  GROUP BY d.file_name, d.region_province
  ORDER BY cnt DESC LIMIT 15
""")
for row in cur.fetchall():
    print(row)

print("\n=== Docs with cap-comp chunks ===")
cur.execute("""
  SELECT d.file_name, d.region_province, COUNT(c.id) as cnt
  FROM staging.spot_knowledge_docs d
  JOIN staging.spot_knowledge_chunks c ON c.doc_id = d.id
  WHERE c.chunk_text ILIKE '%容量补偿%' OR c.chunk_text ILIKE '%容量电价%'
  GROUP BY d.file_name, d.region_province
  ORDER BY cnt DESC LIMIT 15
""")
for row in cur.fetchall():
    print(row)

print("\n=== Sample FR chunks (first 400 chars each) ===")
cur.execute("""
  SELECT LEFT(c.chunk_text, 400), d.file_name, d.region_province
  FROM staging.spot_knowledge_chunks c
  JOIN staging.spot_knowledge_docs d ON d.id = c.doc_id
  WHERE c.chunk_text ILIKE '%调频容量%'
  LIMIT 3
""")
for row in cur.fetchall():
    print("\n---", row[1], "|", row[2])
    print(row[0])

print("\n=== Sample cap-comp chunks ===")
cur.execute("""
  SELECT LEFT(c.chunk_text, 400), d.file_name, d.region_province
  FROM staging.spot_knowledge_chunks c
  JOIN staging.spot_knowledge_docs d ON d.id = c.doc_id
  WHERE c.chunk_text ILIKE '%容量补偿%'
  LIMIT 3
""")
for row in cur.fetchall():
    print("\n---", row[1], "|", row[2])
    print(row[0])

conn.close()
print("\nDone.")
