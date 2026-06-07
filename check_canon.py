import psycopg2, os

conn = psycopg2.connect(os.environ["PGURL"])
cur = conn.cursor()

# Get the view definition for canon.nodal_rt_price_15min
cur.execute("""
    SELECT pg_get_viewdef('canon.nodal_rt_price_15min'::regclass, true)
""")
row = cur.fetchone()
print("=== canon.nodal_rt_price_15min view definition ===")
print(row[0] if row else "NOT FOUND")

# Also check canon.scenario_dispatch_15min
cur.execute("""
    SELECT pg_get_viewdef('canon.scenario_dispatch_15min'::regclass, true)
""")
row = cur.fetchone()
print("\n=== canon.scenario_dispatch_15min view definition ===")
print(row[0] if row else "NOT FOUND")

# List all tables/views in canon schema
cur.execute("""
    SELECT table_name, table_type
    FROM information_schema.tables
    WHERE table_schema = 'canon'
    ORDER BY table_type, table_name
""")
print("\n=== canon schema objects ===")
for r in cur.fetchall():
    print(f"  {r[1]:10s}  {r[0]}")

conn.close()
