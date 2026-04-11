"""
Simple Stripe Payment Ingestion Script
Load Stripe API data into PostgreSQL payment table - Easy Version
"""

import json
import psycopg2
from datetime import datetime

# Database connection settings
DB_HOST = "localhost"
DB_PORT = 5431
DB_NAME = "novafinds_db"
DB_USER = "novafinds_user"
DB_PASSWORD = "novafinds_password"

# Path to Stripe JSON file
STRIPE_JSON_FILE = "20251230_stripe_payments.json"


def load_stripe_payments():
    """
    Main function to load Stripe payments into database
    """
    
    # Step 1: Connect to database
    print("\n1. Connecting to database...")
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        cur = conn.cursor()
        print("   SUCCESS: Connected to database")
    except Exception as e:
        print(f"   ERROR: Could not connect - {e}")
        return False
    
    # Step 2: Read Stripe JSON file
    print("\n2. Reading Stripe JSON file...")
    try:
        with open(STRIPE_JSON_FILE, 'r') as f:
            data = json.load(f)
        payments = data.get("data", [])
        print(f"   SUCCESS: Found {len(payments)} payments")
    except Exception as e:
        print(f"   ERROR: Could not read file - {e}")
        cur.close()
        conn.close()
        return False
    
    # Step 3: Add new columns to payment table (if they don't exist)
    print("\n3. Adding Stripe columns to payment table...")
    try:
        # List of new columns we'll add
        new_columns = [
            "stripe_payment_id VARCHAR(255) UNIQUE",
            "stripe_status VARCHAR(50)",
            "stripe_currency VARCHAR(3)",
            "stripe_customer_id VARCHAR(255)",
            "stripe_amount_received BIGINT",
            "stripe_created_timestamp BIGINT",
            "stripe_description TEXT",
            "stripe_metadata JSONB",
        ]
        
        # Add each column if it doesn't exist
        for column_def in new_columns:
            column_name = column_def.split()[0]
            try:
                cur.execute(f"ALTER TABLE payment ADD COLUMN {column_def};")
                print(f"   Added column: {column_name}")
            except psycopg2.Error:
                # Column already exists, skip
                pass
        
        conn.commit()
        print("   SUCCESS: Columns ready")
    except Exception as e:
        print(f"   ERROR: Could not add columns - {e}")
        conn.close()
        return False
    
    # Step 4: Insert payment data
    print("\n4. Inserting payments into database...")
    inserted = 0
    skipped = 0
    errors = 0
    
    for payment in payments:
        try:
            stripe_id = payment.get("id")
            
            # Check if payment already exists (avoid duplicates)
            cur.execute(
                "SELECT payment_id FROM payment WHERE stripe_payment_id = %s",
                (stripe_id,)
            )
            
            if cur.fetchone():
                # Payment already exists, skip it
                skipped += 1
                continue
            
            # Get the next payment_id
            cur.execute("SELECT MAX(payment_id) FROM payment")
            max_id = cur.fetchone()[0]
            next_id = (max_id or 0) + 1
            
            # Convert amount from cents to dollars
            amount = payment.get("amount_received", 0) / 100
            
            # Convert timestamp to datetime
            created_timestamp = payment.get("created", 0)
            payment_date = datetime.fromtimestamp(created_timestamp)
            
            # Insert into database
            cur.execute("""
                INSERT INTO payment (
                    payment_id,
                    amount,
                    payment_date,
                    stripe_payment_id,
                    stripe_status,
                    stripe_currency,
                    stripe_customer_id,
                    stripe_amount_received,
                    stripe_created_timestamp,
                    stripe_description,
                    stripe_metadata
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                next_id,
                amount,
                payment_date,
                stripe_id,
                payment.get("status"),
                payment.get("currency"),
                payment.get("customer"),
                payment.get("amount_received"),
                created_timestamp,
                payment.get("description"),
                json.dumps(payment.get("metadata", {}))
            ))
            
            inserted += 1
            print(f"   Inserted: {stripe_id}")
            
        except Exception as e:
            errors += 1
            print(f"   ERROR: Could not insert {payment.get('id')} - {str(e)[:50]}")
    
    # Step 5: Commit changes
    print("\n5. Saving changes...")
    try:
        conn.commit()
        print("   SUCCESS: Changes saved")
    except Exception as e:
        print(f"   ERROR: Could not save - {e}")
        conn.rollback()
        cur.close()
        conn.close()
        return False
    
    # Step 6: Summary
    print("\n" + "="*50)
    print("SUMMARY")
    print("="*50)
    print(f"Total payments read:    {len(payments)}")
    print(f"Payments inserted:      {inserted}")
    print(f"Payments skipped:       {skipped}")
    print(f"Errors:                 {errors}")
    print("="*50)
    
    # Close connection
    cur.close()
    conn.close()
    
    return True


if __name__ == "__main__":
    print("\n" + "="*50)
    print("STRIPE PAYMENT LOADER")
    print("="*50)
    
    success = load_stripe_payments()
    
    if success:
        print("\n[SUCCESS] Done! Check the summary above.")
    else:
        print("\n[FAILED] Check error messages above.")
