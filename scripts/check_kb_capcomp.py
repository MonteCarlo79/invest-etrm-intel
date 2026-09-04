"""Check KB coverage for capacity compensation and FR pricing data."""
import psycopg2

pg_url = "postgresql://postgres:!BESSmap2026@bess-platform-pg.cjs000o4wn2w.ap-southeast-1.rds.amazonaws.com:5432/marketdata"
conn = psycopg2.connect(pg_url, connect_timeout=15)
cur = conn.cursor()

print("=== Docs with 调频容量/调频价格 chunks ===")
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

print("\n=== Docs with 容量补偿/容量电价 chunks ===")
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

print("\n=== Sample chunks with numbers for FR pricing ===")
cur.execute("""
  SELECT c.chunk_text[:500], d.file_name, d.region_province
  FROM staging.spot_knowledge_chunks c
  JOIN staging.spot_knowledge_docs d ON d.id = c.doc_id
  WHERE (c.chunk_text ILIKE '%调频容量%' OR c.chunk_text ILIKE '%调频价格%')
    AND c.chunk_text ~ '[0-9]+(\.[0-9]+)?.*元'
  LIMIT 5
""")
for row in cur.fetchall():
    print("\n---")
    print("Province:", row[2], "| File:", row[1])
    print(row[0])

print("\n=== Sample chunks with numbers for cap comp ===")
cur.execute("""
  SELECT c.chunk_text[:500], d.file_name, d.region_province
  FROM staging.spot_knowledge_chunks c
  JOIN staging.spot_knowledge_docs d ON d.id = c.doc_id
  WHERE (c.chunk_text ILIKE '%容量补偿%' OR c.chunk_text ILIKE '%容量电价%')
    AND c.chunk_text ~ '[0-9]+.*元/[千kK][瓦wW]'
  LIMIT 5
""")
for row in cur.fetchall():
    print("\n---")
    print("Province:", row[2], "| File:", row[1])
    print(row[0])

conn.close()
