"""
Simple Stripe Payment Loader - WITH CURRENCY CONVERSION
Load Stripe API data into PostgreSQL payment table
"""

import json
import psycopg2
from datetime import datetime

# Database connection
DB_HOST = "localhost"
DB_PORT = 5431
DB_NAME = "novafinds_db"
DB_USER = "novafinds_user"
DB_PASSWORD = "novafinds_password"

# Stripe JSON file (amounts are in CENTS)
STRIPE_JSON_FILE = "20251230_stripe_payments.json"

# Exchange rates to USD (as of Dec 2024)
EXCHANGE_RATES = {
    "usd": 1.0,
    "eur": 1.08,    # 1 EUR = 1.08 USD
    "gbp": 1.25,    # 1 GBP = 1.25 USD
    "cad": 0.71,    # 1 CAD = 0.71 USD
    "aud": 0.64,    # 1 AUD = 0.64 USD
    "jpy": 0.0067,  # 1 JPY = 0.0067 USD
}

def convert_to_usd(amount_cents, currency):
    """
    Convert amount from cents in given currency to USD dollars
    
    Args:
        amount_cents: Amount in cents (Stripe format)
        currency: Currency code (lowercase)
    
    Returns:
        Amount in USD dollars
    """
    # Convert cents to base currency (e.g., 2500 cents = 25.00 EUR)
    amount_in_currency = amount_cents / 100
    
    # Get exchange rate (default to 1.0 if unknown)
    rate = EXCHANGE_RATES.get(currency.lower(), 1.0)
    
    # Convert to USD
    amount_usd = amount_in_currency * rate
    
    return amount_usd, rate

def load_stripe_payments():
    """Main function to load Stripe payments"""
    
    # Step 1: Connect
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
        print("   SUCCESS: Connected")
    except Exception as e:
        print(f"   ERROR: {e}")
        return False
    
    # Step 2: Read JSON
    print("\n2. Reading Stripe JSON file...")
    try:
        with open(STRIPE_JSON_FILE, 'r') as f:
            data = json.load(f)
        payments = data.get("data", [])
        print(f"   SUCCESS: Found {len(payments)} payments")
    except Exception as e:
        print(f"   ERROR: {e}")
        cur.close()
        conn.close()
        return False
    
    # Step 3: Add columns
    print("\n3. Adding Stripe columns...")
    try:
        new_columns = [
            "stripe_payment_id VARCHAR(255) UNIQUE",
            "stripe_status VARCHAR(50)",
            "stripe_currency VARCHAR(3)",
            "stripe_customer_id VARCHAR(255)",
            "stripe_amount_cents BIGINT",
            "stripe_amount_usd DECIMAL(10,2)",
            "stripe_created_timestamp BIGINT",
            "stripe_description TEXT",
            "stripe_metadata JSONB",
        ]
        
        for column_def in new_columns:
            column_name = column_def.split()[0]
            try:
                cur.execute(f"ALTER TABLE payment ADD COLUMN {column_def};")
            except:
                pass  # Column already exists
        
        conn.commit()
        print("   SUCCESS: Columns ready")
    except Exception as e:
        print(f"   ERROR: {e}")
        conn.close()
        return False
    
    # Step 4: Insert payments with currency conversion
    print("\n4. Inserting payments with currency conversion...")
    inserted = 0
    skipped = 0
    errors = 0
    
    print("\n   Payment Details:")
    print("   " + "="*80)
    print(f"   {'Payment ID':<35} {'Currency':<8} {'Amount (cents)':<15} {'USD Amount':<12} {'Status':<10}")
    print("   " + "="*80)
    
    for payment in payments:
        try:
            stripe_id = payment.get("id")
            
            # Check if exists
            cur.execute(
                "SELECT payment_id FROM payment WHERE stripe_payment_id = %s",
                (stripe_id,)
            )
            
            if cur.fetchone():
                skipped += 1
                continue
            
            # Get next payment_id
            cur.execute("SELECT MAX(payment_id) FROM payment")
            next_id = (cur.fetchone()[0] or 0) + 1
            
            # Extract data
            amount_cents = payment.get("amount_received", 0)
            currency = payment.get("currency", "usd").lower()
            created_timestamp = payment.get("created", 0)
            payment_date = datetime.fromtimestamp(created_timestamp)
            
            # Convert to USD
            amount_usd, rate = convert_to_usd(amount_cents, currency)
            
            # Insert
            cur.execute("""
                INSERT INTO payment (
                    payment_id,
                    amount,
                    payment_date,
                    stripe_payment_id,
                    stripe_status,
                    stripe_currency,
                    stripe_customer_id,
                    stripe_amount_cents,
                    stripe_amount_usd,
                    stripe_created_timestamp,
                    stripe_description,
                    stripe_metadata
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                next_id,
                amount_usd,  # Store USD amount in legacy column
                payment_date,
                stripe_id,
                payment.get("status"),
                currency,
                payment.get("customer"),
                amount_cents,  # Store original cents
                amount_usd,    # Store converted USD
                created_timestamp,
                payment.get("description"),
                json.dumps(payment.get("metadata", {}))
            ))
            
            inserted += 1
            print(f"   {stripe_id:<35} {currency:<8} {amount_cents:<15} ${amount_usd:<11.2f} {payment.get('status'):<10}")
            
        except Exception as e:
            errors += 1
            print(f"   ERROR: Could not insert {payment.get('id')} - {str(e)[:50]}")
    
    # Step 5: Commit
    print("\n5. Saving changes...")
    try:
        conn.commit()
        print("   SUCCESS: Changes saved")
    except Exception as e:
        print(f"   ERROR: {e}")
        conn.rollback()
        cur.close()
        conn.close()
        return False
    
    # Step 6: Summary
    print("\n" + "="*80)
    print("SUMMARY WITH CURRENCY CONVERSION")
    print("="*80)
    print(f"Total payments read:       {len(payments)}")
    print(f"Payments inserted:         {inserted}")
    print(f"Payments skipped:          {skipped}")
    print(f"Errors:                    {errors}")
    print("\nNotes:")
    print("- All amounts converted to USD using standard exchange rates")
    print("- Original amount (cents) stored in: stripe_amount_cents")
    print("- Converted amount (USD) stored in: stripe_amount_usd")
    print("- Legacy column 'amount' also contains USD value")
    print("="*80)
    
    # Show currency conversion details
    print("\nCurrency Conversion Rates Used:")
    currencies_used = set(p.get("currency", "usd").lower() for p in payments)
    for curr in sorted(currencies_used):
        rate = EXCHANGE_RATES.get(curr, 1.0)
        print(f"  {curr.upper()}: 1 {curr.upper()} = {rate} USD")
    
    cur.close()
    conn.close()
    
    return True


if __name__ == "__main__":
    print("\n" + "="*80)
    print("STRIPE PAYMENT LOADER - WITH CURRENCY CONVERSION")
    print("="*80)
    print("\nKey Features:")
    print("- Reads Stripe JSON file (amounts in CENTS)")
    print("- Converts all currencies to USD")
    print("- Stores both original and converted amounts")
    print("- Handles duplicate detection")
    print("="*80)
    
    success = load_stripe_payments()
    
    if success:
        print("\n[SUCCESS] Done! All payments loaded with currency conversion.")
    else:
        print("\n[FAILED] Check error messages above.")
