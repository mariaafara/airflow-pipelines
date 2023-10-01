import psycopg2
from faker import Faker

# Connect to the PostgreSQL database
conn = psycopg2.connect(
    host="127.0.0.1",
    port="5432",
    database="postgres",
    user="postgres",
    password="postgres_password"
)

fake = Faker()

# Generate 100 unique samples
unique_samples = set()
while len(unique_samples) < 100:
    unique_samples.add((fake.name(), fake.email()))

for name, email in unique_samples:
    # Insert each row of data into the "users" table
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO users (name, email) VALUES (%s, %s)",
        (name, email)
    )
    conn.commit()

# Verify that tables and data are inserted
cur = conn.cursor()
cur.execute("SELECT * FROM users LIMIT 3;")
rows = cur.fetchall()
for row in rows:
    print(row)

cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public';")
table_names = cur.fetchall()
for table_name in table_names:
    print(table_name[0])


# Close the database connection
conn.close()

