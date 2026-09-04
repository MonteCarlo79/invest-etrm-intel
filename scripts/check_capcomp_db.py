# -*- coding: utf-8 -*-
import sys, psycopg2
sys.stdout.reconfigure(encoding='utf-8')
pg_url = "postgresql://postgres:!BESSmap2026@bess-platform-pg.cjs000o4wn2w.ap-southeast-1.rds.amazonaws.com:5432/marketdata"
conn = psycopg2.connect(pg_url, connect_timeout=15)
cur = conn.cursor()

print("=== province_cap_comp ===")
cur.execute("SELECT province, effective_date, cap_comp_yuan_kw, peak_duration_hours, source, status, ingested_at FROM marketdata.province_cap_comp ORDER BY ingested_at DESC LIMIT 20")
for r in cur.fetchall():
    print(r)

print("\n=== province_fr_market ===")
cur.execute("SELECT province, effective_date, fr_price_yuan_kw_h, fr_pool_billion_yuan, source, status, ingested_at FROM marketdata.province_fr_market ORDER BY ingested_at DESC LIMIT 20")
for r in cur.fetchall():
    print(r)

conn.close()
