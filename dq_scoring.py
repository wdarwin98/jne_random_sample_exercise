
import pandas as pd
import psycopg2
from datetime import datetime

# ----------------------------- 1. Connect to PostgreSQL -----------------------------
conn = psycopg2.connect(
    host="localhost",
    port=5433,
    database="jnelogistics",
    user="postgres",
    password="postgres"
)
cursor = conn.cursor()

# -------------------------- 2. Add DQ Columns to Existing Table --------------------------
dq_columns = [
    ("row_completeness", "FLOAT"),
    ("row_accuracy", "FLOAT"),
    ("row_timeliness", "FLOAT"),
    ("row_validity", "FLOAT"),
    ("row_consistency", "FLOAT")
]

for col_name, col_type in dq_columns:
    cursor.execute(f"""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_name='jne_logistics_data' AND column_name='{col_name}'
            ) THEN
                ALTER TABLE jne_logistics_data ADD COLUMN {col_name} {col_type};
            END IF;
        END$$;
    """)

# ----------------------------- 3. Load Data from Database -----------------------------
df = pd.read_sql("SELECT * FROM jne_logistics_data", conn)

# ----------------------------- 4. Define DQ Metric Functions -----------------------------
def completeness(row):
    total_fields = len(row)
    non_null_fields = row.count()
    return round(non_null_fields / total_fields, 2)

# 1. Completeness
# Measures how many non fields are present in the row
# Divides the number of non-null fields by the total fields
# Returns a score from 0-1 after calculating the entire

def accuracy(row):
    try:
        lat, lon = float(row['actual_lat']), float(row['actual_lon'])
        if -90 <= lat <= 90 and -180 <= lon <= 180:
            return 1.0
    except:
        pass
    return 0.0

# 2. Accuracy
# Checks if the actual_lat and actual_lon are within Earth's valid latitude/longitude ranges.
# Returns 1 if valid, else 0

def timeliness(row):
    try:
        created = pd.to_datetime(row['created_at'], errors='coerce', dayfirst = True)
        delivered = pd.to_datetime(row['delivery_time'], errors='coerce')
        if pd.isnull(created) or pd.isnull(delivered):
            return 0.0
        duration = (delivered - created).total_seconds()
        return 1.0 if duration <= 3 * 24 * 3600 else 0.0
    except:
        return 0.0
    
# 3. Timeliness
# Calculates how long it took to deliver the package
# Returns 1 if it was delivered within 3 days, else 0

def validity(row):
    try:
        return 1.0 if float(row['weight_kg']) > 0 and 'x' in str(row['dimensions_cm']) else 0.0
    except:
        return 0.0
    
# 4. Validity
# Checks if the weight is positive and the dimensions field contains "x" as a separator
# Returns 1 if both conditions are met

def consistency(row):
    try:
        string_columns = ['origin', 'destination', 'current_status']
        numeric_columns = ['weight_kg']
        datetime_columns = ['created_at', 'delivery_time']

        for col in string_columns:
            val = row[col]
            if pd.isnull(val) or not isinstance(val, str):
                return 0.0

        for col in numeric_columns:
            val = float(row[col])
            if pd.isnull(val):
                return 0.0

        for col in datetime_columns:
            if pd.isnull(pd.to_datetime(row[col], errors='coerce')):
                return 0.0

        return 1.0
    except:
        return 0.0
    
# 5. Consistency
# Ensures string fields are lowercase (to avoid inconsistent casings)
# Ensures numeric fields are actually numbers
# Ensures datetime fields are proper datetime objects


# ---------------------- 5. Compute Scores & Update Main Table ----------------------
for _, row in df.iterrows():
    c = completeness(row)
    a = accuracy(row)
    t = timeliness(row)
    v = validity(row)
    s = consistency(row)

    cursor.execute("""
        UPDATE jne_logistics_data
        SET row_completeness = %s,
            row_accuracy = %s,
            row_timeliness = %s,
            row_validity = %s,
            row_consistency = %s
        WHERE tracking_number = %s
    """, (
        float(c), float(a), float(t), float(v), float(s),
        str(row['tracking_number'])  # Ensures no accidental type mismatch
    ))


# ---------------------- 6. Compute Aggregate Scores and Save Summary ----------------------
total_rows = len(df)

sum_completeness = df.apply(completeness, axis=1).sum()
sum_accuracy = df.apply(accuracy, axis=1).sum()
sum_timeliness = df.apply(timeliness, axis=1).sum()
sum_validity = df.apply(validity, axis=1).sum()
sum_consistency = df.apply(consistency, axis=1).sum()

pct_completeness = round((sum_completeness / total_rows) * 100, 2)
pct_accuracy = round((sum_accuracy / total_rows) * 100, 2)
pct_timeliness = round((sum_timeliness / total_rows) * 100, 2)
pct_validity = round((sum_validity / total_rows) * 100, 2)
pct_consistency = round((sum_consistency / total_rows) * 100, 2)

cursor.execute("""
    CREATE TABLE IF NOT EXISTS logistics_dq_summary (
        id SERIAL PRIMARY KEY,
        pct_completeness FLOAT,
        pct_accuracy FLOAT,
        pct_timeliness FLOAT,
        pct_validity FLOAT,
        pct_consistency FLOAT,
        total_rows INT,
        calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
""")

cursor.execute("""
    INSERT INTO logistics_dq_summary (
        pct_completeness, pct_accuracy, pct_timeliness,
        pct_validity, pct_consistency, total_rows
    ) VALUES (%s, %s, %s, %s, %s, %s);
""", (
    float(pct_completeness), float(pct_accuracy), float(pct_timeliness),
    float(pct_validity), float(pct_consistency), int(total_rows)
))

# ----------------------------- 7. Commit and Close -----------------------------
conn.commit()
cursor.close()
conn.close()
print("DQ scoring complete.")
