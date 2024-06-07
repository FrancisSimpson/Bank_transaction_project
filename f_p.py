import logging
import sqlite3
import csv

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Define the data validation function
def validate_data(record):
    # Ensure record is a dictionary
    if not isinstance(record, dict):
        return False, "Invalid record format. Expected a dictionary."

    # Define required fields
    required_fields = ["Type", "Product", "Started_Date", "Completed_Date", "Description", "Amount", "Fee", "Currency", "State", "Balance"]

    # Check for missing fields
    for field in required_fields:
        if field not in record:
            return False, f"Missing field: {field}"

    # Data type validation
    if not isinstance(record["Amount"], (int, float)):
        return False, "Invalid data type for 'Amount'. Expected an integer or float."
    if not isinstance(record["Fee"], (int, float)):
        return False, "Invalid data type for 'Fee'. Expected an integer or float."

    # All checks passed
    return True, "Record is valid."

def load_data(csv_file, db_file):
    conn = sqlite3.connect(db_file)
    cur = conn.cursor()
    logger.info("Connected to database successfully")

    # Executing SQL to create table
    cur.execute('DROP TABLE IF EXISTS Transactions;')
    cur.execute('''
        CREATE TABLE Transactions (
            TransactionID INTEGER PRIMARY KEY AUTOINCREMENT,
            Type TEXT,
            Product TEXT,
            Started_Date TEXT,
            Completed_Date TEXT,
            Description TEXT,
            Amount DECIMAL,
            Fee DECIMAL,
            Currency TEXT,
            State TEXT,
            Balance DECIMAL
        );
    ''')
    logger.info("Table created successfully")

    # Open the CSV file and insert data
    with open(csv_file, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            cur.execute('''
                INSERT INTO Transactions (Type, Product, Started_Date, Completed_Date, Description, Amount, Fee, Currency, State, Balance)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (row['Type'], row['Product'], row['Started Date'], row['Completed Date'], row['Description'], row['Amount'], row['Fee'], row['Currency'], row['State'], row['Balance']))
            logger.debug(f"Row inserted: {row}")

    conn.commit()
    logger.info("Data inserted successfully")
    conn.close()
    logger.info("Database connection closed")

# Example usage
load_data('francis_account-statement_2023-05-01_2024-05-16.csv', 'personal_finance.db')

# Error Handling Section
try:
    load_data('francis_account-statement_2023-05-01_2024-05-16.csv', 'personal_finance.db')
except sqlite3.DatabaseError as db_err:
    logger.error("Database error occurred", exc_info=True)
except csv.Error as csv_err:
    logger.error("CSV reading error occurred", exc_info=True)
except Exception as e:
    logger.error("An unexpected error occurred", exc_info=True)
