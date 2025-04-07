import mysql.connector
import os
import time

# Database connection details
db_config = {
    "host": "127.0.0.1",
    "port": 3307,
    "user": "admin",
    "password": "C0lumnStore!",
    "charset": "utf8mb4",
    "collation": "utf8mb4_general_ci",
    "database": "bts"
}

# Connect to the database
conn = mysql.connector.connect(**db_config)
cursor = conn.cursor(buffered=True)  # Use a buffered cursor

# Folder containing SQL files
queries_folder = "sql"

# Get list of SQL files and sort them numerically
sql_files = sorted(
    [f for f in os.listdir(queries_folder) if f.endswith(".sql")], 
    key=lambda x: int(x.split(".")[0])  # Extract numeric part for sorting
)

# Loop through sorted SQL files
for sql_file in sql_files:
    file_path = os.path.join(queries_folder, sql_file)
    with open(file_path, "r", encoding="utf-8") as file:
        sql_script = file.read()
        try:
            start_time = time.time()  # Start timing
            
            for statement in sql_script.split(";"):  # Split script into individual queries
                if statement.strip():  # Ensure it's not empty
                    cursor.execute(statement)

                    # Handle SELECT results
                    if cursor.with_rows:
                        cursor.fetchall()  # Consume all rows

                    # Handle multiple result sets
                    while cursor.nextset():
                        pass  # Consume any pending results

                    conn.commit()

            elapsed_time = time.time() - start_time  # End timing
            minutes, seconds = divmod(elapsed_time, 60)  # Convert to minutes & seconds
            print(f"Executed {sql_file} successfully in {int(minutes)}m {seconds:.2f}s.")
        except mysql.connector.Error as err:
            print(f"Error executing {sql_file}: {err}")

# Close the connection
cursor.close()
conn.close()
