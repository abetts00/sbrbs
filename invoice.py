import mysql.connector
from datetime import date, timedelta, datetime
from calendar import monthrange
from collections import defaultdict
import sys  # For exiting on critical errors
import decimal  # Use Decimal for financial calculations
import os  # For environment variables
import argparse

# --- CONFIGURE THIS ---
# Use environment variables for security
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': 'Sweetlou47',  # Consider using environment variables or a config file for security
    'database': 'horse_stable'
}

# --- RATES AND CONSTANTS ---
DISCOUNTED_OWNERS = ['Tim Betts', 'Andrew Betts', 'Scott Betts', 'Randy Taft', 'Arnie Witkin']
DEFAULT_RATE = decimal.Decimal('75.00')
DISCOUNT_RATE = decimal.Decimal('65.00')
TURNOUT_RATE = decimal.Decimal('25.00')
BROODMARE_RATE = decimal.Decimal('45.00')
RACE_FEE = decimal.Decimal('200.00')  # Fee per race start

TRACK_SHIPPING_COSTS = {
    'PCD': decimal.Decimal('655.00'),
    'NFLD': decimal.Decimal('315.00'),
    'MVR': decimal.Decimal('530.00'),
    'MEA': decimal.Decimal('0.00'),
    'SCD': decimal.Decimal('500.00')
}

FEE_CONFIG = {
    'LASIX': {'DEFAULT': decimal.Decimal('25.00')},
    'WARMUP': {'DEFAULT': decimal.Decimal('10.00')},
    'PADDOCK': {
        'PCD': decimal.Decimal('100.00'), 
        'NFLD': decimal.Decimal('100.00'), 
        'MVR': decimal.Decimal('100.00'), 
        'MEA': decimal.Decimal('0.00'),
        'SCD': decimal.Decimal('100.00')
    },
    'OVERNIGHT': {'PCD': decimal.Decimal('10.00')}
}
# --- END CONFIGURATION ---

def parse_arguments():
    """Parse command line arguments for invoice generation."""
    parser = argparse.ArgumentParser(description='Generate invoices for Scott Betts Racing Stable.')
    
    # Add arguments for month and year
    parser.add_argument('-m', '--month', type=int, 
                        help='Billing month (1-12)')
    parser.add_argument('-y', '--year', type=int, 
                        help='Billing year (e.g., 2025)')
    
    # Option to use previous month automatically
    parser.add_argument('--previous-month', action='store_true',
                        help='Use previous month automatically')
    
    args = parser.parse_args()
    
    # If --previous-month is specified, calculate it
    if args.previous_month:
        today = date.today()
        if today.month == 1:
            billing_month = 12
            billing_year = today.year - 1
        else:
            billing_month = today.month - 1
            billing_year = today.year
    else:
        # If specific month/year provided, use those
        if args.month and args.year:
            billing_month = args.month
            billing_year = args.year
        else:
            # Default to current month
            today = date.today()
            billing_month = today.month
            billing_year = today.year
    
    # Validate month
    if not 1 <= billing_month <= 12:
        raise ValueError(f"Month must be between 1 and 12, got {billing_month}")
    
    return billing_month, billing_year

# --- Helper to convert DB values to Decimal ---
def to_decimal(value, default='0.0'):
    """Safely converts a value to Decimal, handling None or errors."""
    if value is None:
        return decimal.Decimal(default)
    try:
        return decimal.Decimal(str(value))
    except (TypeError, decimal.InvalidOperation):
        print(f"⚠️ Warning: Could not convert '{value}' to Decimal. Using {default}.")
        return decimal.Decimal(default)

def fetch_billing_data(cursor, month, year, start_date, end_date):
    """Fetch all necessary data from the database."""
    print("Fetching data from database...")
    
    cursor.execute("SELECT owner_id, name, receives_purse_checks FROM Owners")
    owners = {o['owner_id']: o for o in cursor.fetchall()}
    
    cursor.execute("SELECT horse_id, name, status, training_status, sale_date, inactive_date, exempt_from_earnings_credit FROM Horses WHERE status != 'sold'")
    all_horses_data = {h['horse_id']: h for h in cursor.fetchall()}
    
    cursor.execute("SELECT horse_id, training_days FROM TrainingDaysOverride WHERE year = %s AND month = %s", (year, month))
    overrides = {row['horse_id']: row['training_days'] for row in cursor.fetchall()}
    
    cursor.execute("""
        SELECT owner_id, horse_id, SUM(percentage_ownership) AS percentage_ownership
        FROM Ownership
        WHERE horse_id IN (SELECT horse_id FROM Horses WHERE status != 'sold')
        GROUP BY owner_id, horse_id
        HAVING SUM(percentage_ownership) > 0
    """)
    ownership_data = cursor.fetchall()
    
    cursor.execute("SELECT * FROM RacePerformance WHERE race_date BETWEEN %s AND %s", (start_date, end_date))
    races_this_month = cursor.fetchall()
    
    cursor.execute("SELECT * FROM Expenses WHERE expense_date BETWEEN %s AND %s", (start_date, end_date))
    expenses_this_month = cursor.fetchall()
    
    print("Data fetching complete.")
    
    return owners, all_horses_data, overrides, ownership_data, races_this_month, expenses_this_month

def insert_race_day_fees(cursor, races_this_month, expenses_this_month):
    """Insert automatic race day fees if they don't already exist."""
    print("Checking for and inserting automatic race day fees...")
    
    existing_fees = {
        (row['horse_id'], row['expense_date'], row['notes'])
        for row in expenses_this_month
        if row['expense_type'] == 'race_day_fee' and row['notes']
    }
    
    race_fee_inserts = []
    for race in races_this_month:
        horse_id = race['horse_id']
        race_dt = race['race_date']
        track = (race.get('track') or 'UNKNOWN').strip().upper()
        # 🎯 don't auto-insert LASIX/Warm-Up paddock for MEA
        if track == 'MEA':
            continue

        for fee_type, config_map in FEE_CONFIG.items():
            amount = config_map.get(track, config_map.get('DEFAULT'))
            if amount is not None and amount > 0:  # Check amount exists and is positive
                note = f"{fee_type.replace('_', ' ').title()} – {track}"
                if (horse_id, race_dt, note) not in existing_fees:
                    race_fee_inserts.append((horse_id, amount, 'race_day_fee', race_dt, note))
                    existing_fees.add((horse_id, race_dt, note))
    
    if race_fee_inserts:
        try:
            cursor.executemany("""
                INSERT INTO Expenses (horse_id, amount, expense_type, expense_date, notes)
                VALUES (%s, %s, %s, %s, %s)
            """, race_fee_inserts)
            print(f"✅ Inserted {len(race_fee_inserts)} automatic race day fees.")
            return True
        except mysql.connector.Error as err:
            print(f"❌ Database error inserting race day fees: {err}")
            return False
    else:
        print("No new automatic race day fees to insert.")
        return False

def calculate_shipping_costs(races_this_month, track_shipping_costs):
    """Calculate shipping costs per horse."""
    print("Calculating shipping costs...")
    
    shipping_groups = defaultdict(lambda: defaultdict(set))
    for r in races_this_month:
        track = (r.get('track') or 'UNKNOWN').strip().upper()
        race_date_obj = r['race_date']
        if track != 'MEA' and track in track_shipping_costs:
            shipping_groups[track][race_date_obj].add(r['horse_id'])
    
    horse_shipping_cost_per_trip = defaultdict(lambda: defaultdict(decimal.Decimal))
    for track, date_groups in shipping_groups.items():
        cost_per_trip = track_shipping_costs.get(track, decimal.Decimal('0.0'))
        if cost_per_trip > 0:
            for race_date_obj, group_horse_ids in date_groups.items():
                num_horses = len(group_horse_ids)
                if num_horses > 0:
                    # Use ROUND_HALF_UP for currency
                    share = (cost_per_trip / num_horses).quantize(decimal.Decimal('0.01'), 
                                                                 rounding=decimal.ROUND_HALF_UP)
                    for h_id in group_horse_ids:
                        horse_shipping_cost_per_trip[h_id][race_date_obj] += share
    
    horse_total_shipping = defaultdict(decimal.Decimal)
    for h_id, date_costs in horse_shipping_cost_per_trip.items():
        horse_total_shipping[h_id] = sum(date_costs.values())
    
    return horse_shipping_cost_per_trip, horse_total_shipping

def group_expenses(expenses_this_month):
    """Group expenses by horse."""
    print("Grouping expenses...")
    
    expense_map = defaultdict(lambda: defaultdict(decimal.Decimal))
    expense_details = defaultdict(list)  # Keep for potential future detailed breakdown
    
    for e in expenses_this_month:
        horse_id = e['horse_id']
        amt = to_decimal(e.get('amount'))
        etype = (e.get('expense_type') or 'other').lower().strip()
        expense_map[horse_id][etype] += amt
        expense_details[horse_id].append(e)
    
    return expense_map, expense_details

def group_races_by_track_date(horse_races):
    """Group races by track and date."""
    race_grouped = defaultdict(list)
    for race in horse_races:
        race_track = (race.get("track") or "UNKNOWN").strip().upper()
        race_date = race.get("race_date")
        race_grouped[(race_track, race_date)].append(race)
    return race_grouped

def calculate_billed_days(horse, start_date, end_date, default_days):
    """Calculate billable days for a horse, accounting for sale or inactive dates."""
    billed_days = default_days
    
    # Check for sale date
    sale_date = horse.get('sale_date')
    if sale_date and isinstance(sale_date, date):
        if sale_date < start_date:
            print(f"⏭️ Skipping horse '{horse.get('name', 'Unknown')}' – sold before billing month")
            return 0  # Horse sold before billing period
        elif start_date <= sale_date <= end_date:
            billed_days = (sale_date - start_date).days + 1
            print(f"📉 Horse '{horse.get('name', 'Unknown')}' sold mid-month on {sale_date}. Billing for {billed_days} days.")
    
    # Check for inactive date - take earlier of sale_date or inactive_date if both exist
    inactive_date = horse.get('inactive_date')
    if inactive_date and isinstance(inactive_date, date):
        if inactive_date < start_date:
            print(f"⏭️ Skipping horse '{horse.get('name', 'Unknown')}' – inactive before billing month")
            return 0  # Horse inactive before billing period
        elif start_date <= inactive_date <= end_date:
            inactive_days = (inactive_date - start_date).days + 1
            if billed_days == default_days or inactive_days < billed_days:
                billed_days = inactive_days
                print(f"📉 Horse '{horse.get('name', 'Unknown')}' inactive mid-month on {inactive_date}. Billing for {billed_days} days.")
    
    return billed_days

def process_board_split_config(horse_id, expense_details):
    """Process BOARD_SPLIT_CONFIG expense if present."""
    split_config = {}
    split_config_expense_id = None
    
    for exp in expense_details.get(horse_id, []):
        if exp.get('expense_type') == 'BOARD_SPLIT_CONFIG':
            split_config_expense_id = exp.get('expense_id')
            notes = exp.get('notes', '').upper()
            try:
                # Parse configuration from notes
                config_parts = {part.split(':')[0].strip(): part.split(':')[1].strip() 
                              for part in notes.split(',') if ':' in part}
                
                split_config = {
                    'normal_days': int(config_parts.get('NORMAL_DAYS', 0)),
                    'sub_days': int(config_parts.get('SUB_DAYS', 0)),
                    'sub_cost_type': config_parts.get('SUB_COST_TYPE', 'SUM').strip(),
                    'sub_rate': to_decimal(config_parts.get('SUB_RATE', '0.0'))
                }
                
                # Found valid config, stop processing
                break
            except Exception as e:
                print(f"❌ Error parsing BOARD_SPLIT_CONFIG: {e}")
    
    return split_config, split_config_expense_id

def determine_board_cost(horse, owner_name, billed_days, split_config, expense_details, subcontract_consumed_ids):
    """Determine board cost based on horse status and any split configuration."""
    horse_id = horse.get('horse_id')
    training_status = (horse.get('training_status') or 'in_training').lower().strip()
    billing_items = []
    
    # Find relevant subcontractor expenses
    subcontractor_expenses = [
        exp for exp in expense_details.get(horse.get('horse_id'), [])
        if exp.get('subcontractor_name') and exp.get('subcontractor_name').strip() != ''
    ]
    if horse_id == 6:
        print("Emeralds Legacy Expenses:", expense_details.get(horse_id, []))
        #print("Emeralds Legacy Subcontractor Expenses:", subcontractor_expenses)
        #print("Emeralds Legacy use_subcontractor_rate:", use_subcontractor_rate)

    # Check if we're using a split board configuration
    if split_config and (split_config.get('normal_days', 0) > 0 or split_config.get('sub_days', 0) > 0):
        normal_days = split_config['normal_days']
        sub_days = split_config['sub_days']
        
        # Validate total days don't exceed billed days
        if (normal_days + sub_days) > billed_days:
            print(f"⚠️ Split config days ({normal_days + sub_days}) exceed billable days ({billed_days}) for {horse_name}. Adjusting.")
            sub_days = max(0, billed_days - normal_days)
        
        print(f"ℹ️ Using split board for {horse_name}: {normal_days}d normal + {sub_days}d subcontracted")
        
        # Part 1: Normal board days
        if normal_days > 0:
            daily_rate = DISCOUNT_RATE if owner_name in DISCOUNTED_OWNERS else DEFAULT_RATE
            
            # Adjust rate based on training status
            if training_status == 'turned_out':
                daily_rate = TURNOUT_RATE
                board_description = "Turnout"
            elif training_status == 'broodmare':
                daily_rate = BROODMARE_RATE
                board_description = "Broodmare Care"
            else:
                board_description = "Training & Board"
            
            normal_cost = daily_rate * decimal.Decimal(normal_days)
            billing_items.append({
                'item_type': 'Board',
                'description': f"{board_description} ({normal_days} days @ ${daily_rate}/day)",
                'amount': normal_cost
            })
        
        # Part 2: Subcontracted board days
        if sub_days > 0:
            sub_cost = decimal.Decimal('0.0')
            
            # Get subcontractor names for display
            sub_names = set()
            for exp in subcontractor_expenses:
                if exp.get('subcontractor_name'):
                    sub_names.add(exp.get('subcontractor_name').strip())
            
            sub_name_display = ", ".join(sorted(list(sub_names))) if sub_names else "Subcontractor"
            
            if split_config.get('sub_cost_type') == 'RATE':
                # Use the daily rate specified in the config
                sub_rate = split_config.get('sub_rate', decimal.Decimal('0.0'))
                sub_cost = sub_rate * decimal.Decimal(sub_days)
                description = f"Subcontractor Board – {sub_name_display} ({sub_days} days @ ${sub_rate}/day)"
            else:
                # Use SUM method - sum actual expenses
                for exp in subcontractor_expenses:
                    sub_cost += to_decimal(exp.get('amount'))
                    # Mark this expense as consumed
                    if 'expense_id' in exp:
                        subcontract_consumed_ids.add(exp['expense_id'])
                
                description = f"Subcontractor Board – {sub_name_display} ({sub_days} days)"
            
            billing_items.append({
                'item_type': 'Board',
                'description': description,
                'amount': sub_cost
            })
    
    # No split config - handle as standard board or full subcontractor board
    else:
        # Check if we have subcontractor expenses
        if subcontractor_expenses:
            # Sum all subcontractor expenses
            sub_cost = decimal.Decimal('0.0')
            sub_names = set()
            
            for exp in subcontractor_expenses:
                sub_cost += to_decimal(exp.get('amount'))
                # Mark expense as consumed
                if 'expense_id' in exp:
                    subcontract_consumed_ids.add(exp['expense_id'])
                if exp.get('subcontractor_name'):
                    sub_names.add(exp.get('subcontractor_name').strip())
            
            sub_name_display = ", ".join(sorted(list(sub_names))) if sub_names else "Subcontractor"
            
            billing_items.append({
                'item_type': 'Board',
                'description': f"Subcontractor Board – {sub_name_display}",
                'amount': sub_cost
            })
        else:
            # Standard board based on training status
            if training_status == 'turned_out':
                daily_rate = TURNOUT_RATE
                board_description = "Turnout"
            elif training_status == 'broodmare':
                daily_rate = BROODMARE_RATE
                board_description = "Broodmare Care"
            else:
                daily_rate = DISCOUNT_RATE if owner_name in DISCOUNTED_OWNERS else DEFAULT_RATE
                board_description = "Training & Board"
            
            board_cost = daily_rate * decimal.Decimal(billed_days)
            
            billing_items.append({
                'item_type': 'Board',
                'description': f"{board_description} ({billed_days} days @ ${daily_rate}/day)",
                'amount': board_cost
            })
    
    return billing_items

def calculate_race_fees(horse_id, horse_name, horse_races, pct):
    """Calculate race fees for a horse."""
    if not horse_races:
        return []
    
    billing_items = []
    race_grouped = group_races_by_track_date(horse_races)
    
    for (track, race_date), races in race_grouped.items():
        num_starts = len(races)
        
        if track == "MEA":
            total_cost = RACE_FEE * num_starts
            item_desc = f"{num_starts} Race Start(s) at MEA @ ${RACE_FEE}/start"
        else:
            # For non-MEA tracks, don't include cost with race information line
            total_cost = decimal.Decimal('0.00')
            item_desc = f"{num_starts} Race Start(s) at {track}"
        
        # Calculate owner's share
        race_owner_share = (total_cost * (pct / decimal.Decimal('100'))).quantize(
            decimal.Decimal('0.01'), rounding=decimal.ROUND_HALF_UP
        )
        
        if not race_owner_share.is_zero():
            billing_items.append({
                'item_type': 'Race Starts',
                'description': item_desc,
                'owner_share': race_owner_share,
                'horse_id': horse_id,
                'horse_name': horse_name
            })
    
    return billing_items

def calculate_earnings_credit(horse, owner_detail, horse_races, pct):
    """Calculate earnings credit for races."""
    if not horse_races:
        return None
    
    # Check if owner or horse is exempt from earnings credit
    horse_exempt = horse.get('exempt_from_earnings_credit', False)
    owner_exempt = owner_detail.get('receives_purse_checks', False)
    receives_purse = horse_exempt or owner_exempt
    
    # Calculate total earnings
    gross_earnings = sum(to_decimal(r.get('earnings')) for r in horse_races)
    
    # Apply 90% factor for net earnings (driver/trainer take 5% each)
    net_earnings = (gross_earnings * decimal.Decimal('0.90')).quantize(
        decimal.Decimal('0.01'), rounding=decimal.ROUND_HALF_UP
    )
    
    # If owner receives purse checks directly, no credit is applied
    total_earnings = decimal.Decimal('0.0') if receives_purse else net_earnings
    
    if total_earnings.is_zero():
        return None
    
    # Calculate owner's share (negative amount as it's a credit)
    earn_owner_share = (-total_earnings * (pct / decimal.Decimal('100'))).quantize(
        decimal.Decimal('0.01'), rounding=decimal.ROUND_HALF_UP
    )
    
    return {
        'item_type': 'Earnings Credit',
        'description': f"Total Earnings Credit",
        'owner_share': earn_owner_share  # Negative amount
    }

def insert_billing_data(cursor, conn, owner_totals, month, year, bill_date, due_date):
    """Insert the calculated billing data into the database."""
    print("Inserting invoice data into database...")
    created_count = 0
    skipped_owners = 0
    
    for owner_id, data in owner_totals.items():
        # Get the final calculated total for the Billing record
        owner_final_total_amount = data['total']
        owner_billing_items = data['items']  # List of item dicts
        
        # Skip creating invoice if total is zero AND no items
        if owner_final_total_amount.is_zero() and not owner_billing_items:
            print(f"Skipping invoice for owner {owner_id} (zero balance and no items).")
            skipped_owners += 1
            continue
        
        # Check if items list is empty OR all items have zero share, despite a non-zero final total
        all_items_effectively_zero = True
        if owner_billing_items:
            for item_dict in owner_billing_items:
                item_share = to_decimal(item_dict.get('owner_share'))
                if not item_share.is_zero():
                    all_items_effectively_zero = False
                    break
        
        # If the final total isn't zero, but we have no items OR all are zero, add adjustment item
        if not owner_final_total_amount.is_zero() and all_items_effectively_zero:
            print(f"⚠️ Adding balance adjustment item for Owner ID: {owner_id} - Total: ${owner_final_total_amount}")
            adjustment_item = {
                'horse_id': None,
                'horse_name': 'Summary',
                'item_type': 'Balance Adjustment',
                'description': 'Net balance for billing period',
                'owner_share': owner_final_total_amount,  # The item amount is the total due
            }
            # Prepend adjustment item so it appears first on invoice
            owner_billing_items.insert(0, adjustment_item)
        
        # --- Start Transaction for this Owner ---
        try:
            # === INSERT into Billing table ===
            billing_sql = """
                INSERT INTO Billing (owner_id, bill_date, due_date, total_amount, status, billing_period_month, billing_period_year)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            billing_values = (
                owner_id, bill_date, due_date,
                owner_final_total_amount,  # Use the final calculated Decimal total
                'Pending', month, year
            )
            cursor.execute(billing_sql, billing_values)
            billing_id = cursor.lastrowid
            print(f"  Created Billing record ID: {billing_id} for Owner ID: {owner_id}")
            
            # === INSERT into BillingItem table ===
            billing_item_inserts = []
            for item in owner_billing_items:
                # Only insert if the item's share is actually non-zero
                item_amount = to_decimal(item.get('owner_share'))
                if not item_amount.is_zero():
                    # Combine type and description for a meaningful line item
                    item_desc = f"{item['item_type']}: {item['description']}"
                    billing_item_values = (
                        billing_id,
                        item.get('horse_id'),  # Handles None for adjustment item
                        item.get('horse_name', 'Summary'),  # Handles adjustment item
                        item_desc,
                        item_amount  # Insert the Decimal amount
                    )
                    billing_item_inserts.append(billing_item_values)
            
            if billing_item_inserts:
                item_sql = """
                    INSERT INTO BillingItem (billing_id, horse_id, horse_name, item_description, item_amount)
                    VALUES (%s, %s, %s, %s, %s)
                """
                cursor.executemany(item_sql, billing_item_inserts)
                print(f"    Inserted {len(billing_item_inserts)} non-zero billing items for Billing ID: {billing_id}")
            elif not owner_final_total_amount.is_zero():
                print(f"    No non-zero billing items were generated to insert for Bill ID: {billing_id}, although total is ${owner_final_total_amount:.2f}")
            
            # --- Commit Transaction for this Owner ---
            conn.commit()
            created_count += 1
            print(f"  Successfully committed invoice for Owner ID: {owner_id}")
            
        except mysql.connector.Error as err:
            print(f"❌ Database error processing invoice for Owner ID: {owner_id}: {err}")
            print(f"  Rolling back transaction for Owner ID: {owner_id}")
            conn.rollback()
        except Exception as e:
            print(f"❌ An unexpected error occurred processing invoice for Owner ID: {owner_id}: {e}")
            print(f"  Rolling back transaction for Owner ID: {owner_id}")
            conn.rollback()
    
    print(f"--- Invoice Generation Complete ---")
    print(f"✅ {created_count} owner invoice(s) generated and saved for {month:02d}/{year}.")
    if skipped_owners > 0:
        print(f"ℹ️ {skipped_owners} owner(s) were skipped (zero balance or no items).")
    
    return created_count, skipped_owners

def days_overlap(start1, end1, start2, end2):
                """Returns (number of overlapping days, latest_start, earliest_end) between two date ranges."""
                latest_start = max(start1, start2)
                earliest_end = min(end1, end2)
                delta = (earliest_end - latest_start).days + 1
                return max(0, delta), latest_start, earliest_end
                print(f"[DEBUG] {horse.get('name', 'Unknown Horse')} - Assignment: {a['subcontractor_name']}, Start: {a['start_date']}, End: {a['end_date']}, Overlap: {overlap_days}")


# --- Main Function ---
def generate_invoices(month, year):
    """
    Generates invoices for the specified month and year based on horse ownership,
    training status, races, and expenses.
    """
    conn = None
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor(dictionary=True, buffered=True)  # Use buffered cursor
        cursor.execute("SET SESSION sql_mode = 'TRADITIONAL'")  # Good practice
        print(f"--- Generating invoices for Billing Period: {month:02d}/{year} ---")
        
        # --- Calculate Dates ---
        try:
            days_in_month = monthrange(year, month)[1]
            start_date = date(year, month, 1)
            end_date = date(year, month, days_in_month)
            
            # Change from first of next month to current date
            bill_date = datetime.today().date()
            # Change from fixed 15th to 15 days after bill date
            due_date = bill_date + timedelta(days=15)
            
            print(f"Billing Period: {start_date} to {end_date}")
            print(f"Bill Date: {bill_date}, Due Date: {due_date}")
        except ValueError as e:
            print(f"❌ Error: Invalid month ({month}) or year ({year}). {e}")
            return
        
        # --- Fetch all necessary data ---
        owners, all_horses_data, overrides, ownership_data, races_this_month, expenses_this_month = fetch_billing_data(
            cursor, month, year, start_date, end_date
        )
        
        # --- Fetch Subcontractor Assignments for This Billing Period ---
        cursor.execute("""
            SELECT *
              FROM SubcontractorAssignments
             WHERE (start_date <= %s)
               AND (end_date IS NULL OR end_date >= %s)
        """, (end_date, start_date))
        subcontractor_assignments = cursor.fetchall()

        # --- Insert automatic race day fees ---
        if insert_race_day_fees(cursor, races_this_month, expenses_this_month):
            conn.commit()
            # Re-fetch expenses AFTER inserting new ones
            cursor.execute("SELECT * FROM Expenses WHERE expense_date BETWEEN %s AND %s", (start_date, end_date))
            expenses_this_month = cursor.fetchall()
            print("Refreshed expenses data.")
        
        # --- Calculate shipping costs ---
        horse_shipping_cost_per_trip, horse_total_shipping = calculate_shipping_costs(
            races_this_month, TRACK_SHIPPING_COSTS
        )
        
        # --- Group expenses by horse ---
        expense_map, expense_details = group_expenses(expenses_this_month)
        
        # --- Group ownership by owner ---
        print("Grouping ownership...")
        owner_horses = defaultdict(list)
        for entry in ownership_data:
            percentage = to_decimal(entry.get('percentage_ownership'), default=None)  # Check if valid
            if percentage is not None and percentage > 0:
                owner_horses[entry['owner_id']].append({
                    'horse_id': entry['horse_id'],
                    'percentage': percentage  # Store as Decimal
                })
        
        # --- Calculate Billing Items Per Owner ---
        print("Calculating billing items per owner...")
        # owner_totals structure: owner_id -> {'total': Decimal, 'items': list_of_item_dicts}
        owner_totals = defaultdict(lambda: {'total': decimal.Decimal('0.0'), 'items': []})
        
        for owner_id, owner_horse_list in owner_horses.items():
            if owner_id not in owners:
                print(f"⚠️ Warning: Ownership data found for unknown owner_id {owner_id}. Skipping.")
                continue
            
            owner_detail = owners[owner_id]
            owner_name = owner_detail.get('name', 'Unknown Owner')
            
            # Get previous balance and payments
            cursor.execute("""
                SELECT IFNULL(balance_due, 0) AS prev_due
                FROM Billing
                WHERE owner_id = %s
                AND bill_date = (
                    SELECT MAX(bill_date)
                    FROM Billing
                    WHERE owner_id = %s
                )
            """, (owner_id, owner_id))
            result = cursor.fetchone()
            
            prev_due = decimal.Decimal('0.0')
            if result is not None:
                prev_due = to_decimal(result['prev_due'])
            else:
                print(f"  Note: No previous billing record found for owner_id {owner_id}")

            # Get payments made since last invoice
            cursor.execute("""
                SELECT COALESCE(SUM(amount), 0) AS total_payments
                FROM Payments
                WHERE owner_id = %s
                AND payment_date >= COALESCE(
                    (SELECT MAX(bill_date) FROM Billing WHERE owner_id = %s),
                    '1900-01-01'  -- Default date if no previous invoice
                )
                AND payment_date < %s  -- Before current invoice date
            """, (owner_id, owner_id, bill_date))

            payment_result = cursor.fetchone()
            total_payments = to_decimal(payment_result['total_payments'])
            print(f"  Found ${total_payments} in payments since last invoice for owner_id {owner_id}")

            # Calculate adjusted previous balance
            adjusted_prev_due = max(prev_due - total_payments, decimal.Decimal('0.0'))
            
            # Add opening balance if any
            if prev_due > decimal.Decimal('0.0'):
                owner_totals[owner_id]['items'].append({
                    'horse_id': None,
                    'horse_name': 'Summary',
                    'item_type': 'Opening Balance',
                    'description': 'Balance carried from prior billing',
                    'owner_share': prev_due
                })

            # Add payment credit if any
            if total_payments > decimal.Decimal('0.0'):
                owner_totals[owner_id]['items'].append({
                    'horse_id': None,
                    'horse_name': 'Summary',
                    'item_type': 'Payment',
                    'description': 'Payments received since last invoice',
                    'owner_share': -total_payments  # Negative amount as it's a credit
                })
            
            # Initialize running total with adjusted previous balance
            owner_grand_total_share = adjusted_prev_due
            
            # Process each horse for this owner
            # FOR HORSE LOOP
            for horse_entry in owner_horse_list:
                horse_id = horse_entry['horse_id']
                pct = horse_entry['percentage']  # Already a Decimal > 0

                # --- SET THE HORSE VARIABLE AT THE TOP ---
                if horse_id not in all_horses_data:
                    print(f"⚠️ Warning: Ownership for owner {owner_name} (ID: {owner_id}) for horse_id {horse_id}, but horse data not found. Skipping horse.")
                    continue
                horse = all_horses_data[horse_id]
                horse_name = horse.get('name', 'Unknown Horse')            
                period_start = start_date
                period_end = end_date

                # --- NEW: Get all assignments for this horse for the billing period ---
                horse_assignments = [
                    a for a in subcontractor_assignments if int(a['horse_id']) == int(horse_id)
                ]
                print(f"[DEBUG] Horse: {horse_id}, Type: {type(horse_id)}, Assignments: {[ (a['horse_id'], type(a['horse_id'])) for a in subcontractor_assignments ]}")
                print(f"[DEBUG] Filtered assignments: {horse_assignments}")

                all_in_covered_days = set()  # Track which days are "all-in" (covered by Pantaleano, etc.)

                if horse_id == 6:
                    print("Emeralds Legacy Expenses:", expense_details.get(horse_id, []))
                    #print("Emeralds Legacy Subcontractor Expenses:", subcontractor_expenses)
                    print("Checkpoint: got to here without error")

                #    print("Emeralds Legacy use_subcontractor_rate:", use_subcontractor_rate)

                for a in horse_assignments:
                    asgn_start = a['start_date']
                    asgn_end = a['end_date'] or period_end
                    overlap_days, ov_start, ov_end = days_overlap(period_start, period_end, asgn_start, asgn_end)
                    if overlap_days == 0 or not a['all_in']:
                        continue  # No overlap or not all-in
                    # Mark those days as covered by 'all-in'
                    for i in range(overlap_days):
                        all_in_covered_days.add(ov_start + timedelta(days=i))
                    # Bill as "all-in" (total or per diem)
                    print(f"DEBUG: Assignment rate_type: {a.get('rate_type')}, total_amount: {a.get('total_amount')}, daily_rate: {a.get('daily_rate')}, overlap_days: {overlap_days}")
                        # Before calculating/adding board item, print these:
                    print(f"DEBUG: pct (ownership percent) = {pct}")

                    if a.get('rate_type') == 'total' and a.get('total_amount'):
                        total_assign_days = (asgn_end - asgn_start).days + 1
                        prorate = overlap_days / total_assign_days
                        prorated_amount = decimal.Decimal(a['total_amount']) * decimal.Decimal(prorate)
                        board_desc = f"{a['subcontractor_name']} Flat All-In Fee ({ov_start} to {ov_end})"
                        owner_share = (prorated_amount * (pct / decimal.Decimal('100'))).quantize(decimal.Decimal('0.01'), rounding=decimal.ROUND_HALF_UP)
                        if not owner_share.is_zero():
                            owner_totals[owner_id]['items'].append({
                                'horse_id': horse_id,
                                'horse_name': horse.get('name', 'Unknown Horse'),
                                'item_type': 'Board',
                                'description': board_desc,
                                'owner_share': owner_share
                            })

                    else:
                        daily_rate = a['daily_rate']
                        total_fee = decimal.Decimal(daily_rate) * overlap_days
                        board_desc = f"{a['subcontractor_name']} All-In Board ({overlap_days} days @ ${daily_rate}/day, {ov_start} to {ov_end})"
                        owner_share = (total_fee * (pct / decimal.Decimal('100'))).quantize(decimal.Decimal('0.01'), rounding=decimal.ROUND_HALF_UP)
                        print(f"DEBUG: Calculated owner_share={owner_share} for {overlap_days} days at ${daily_rate}/day, pct={pct} ({horse.get('name', 'Unknown Horse')})")

                        if not owner_share.is_zero():
                            print(f"DEBUG: APPENDING Pantaleano board for {horse.get('name', 'Unknown Horse')}: {overlap_days} days at ${daily_rate}/day, owner_share={owner_share}")
                            owner_totals[owner_id]['items'].append({
                                'horse_id': horse_id,
                                'horse_name': horse.get('name', 'Unknown Horse'),
                                'item_type': 'Board',
                                'description': board_desc,
                                'owner_share': owner_share
                            })
                        else:
                            print(f"DEBUG: NOT APPENDING Pantaleano board, owner_share is zero! ({horse.get('name', 'Unknown Horse')})")


                if horse_id not in all_horses_data:
                    print(f"⚠️ Warning: Ownership for owner {owner_name} (ID: {owner_id}) for horse_id {horse_id}, but horse data not found. Skipping horse.")
                    continue
                
                horse = all_horses_data[horse_id]
                horse_name = horse.get('name', 'Unknown Horse')
                training_status = (horse.get('training_status') or 'in_training').lower().strip()
                
                # ADD PRINTS HERE — before any continue!
                print(f"DEBUG: Owner: {owner_name}, Horse: {horse_name}, Horse ID: {horse_id}")
                horse_assignments = [a for a in subcontractor_assignments if a['horse_id'] == horse_id]
                print(f"  Subcontractor assignments for this horse: {horse_assignments}")

                if training_status not in ['in_training', 'turned_out', 'broodmare']:
                    print(f"⏭️ Skipping horse '{horse_name}' for owner '{owner_name}' – training status: '{training_status}'")
                    continue
                
                horse_races = [r for r in races_this_month if r['horse_id'] == horse_id]
                earnings_item = calculate_earnings_credit(horse, owner_detail, horse_races, pct)
                if earnings_item:
                    earnings_item['horse_id'] = horse_id
                    earnings_item['horse_name'] = horse_name
                    owner_totals[owner_id]['items'].append(earnings_item)
                    
                # Calculate billable days for the horse
                billed_days = calculate_billed_days(
                    horse, 
                    start_date, 
                    end_date, 
                    overrides.get(horse_id, days_in_month)
                )
                
                if billed_days == 0:
                    print(f"⏭️ Skipping horse '{horse_name}' - no billable days race/board fees")
                    continue
                
                # Track which subcontractor expense IDs are consumed by board calculations
                subcontract_consumed_ids = set()
                
                # Check for board split configuration
                split_config, split_config_expense_id = process_board_split_config(horse_id, expense_details)
                
                non_allin_days = billed_days - len(all_in_covered_days)
                if non_allin_days == 0:
                    board_items = []
                else:
                    board_items = determine_board_cost(
                        horse, 
                        owner_name, 
                        non_allin_days,   # ← Only days not covered by all-in
                        split_config, 
                        expense_details, 
                        subcontract_consumed_ids
                    )
                
                # Calculate owner's share for each board item
                for item in board_items:
                    item_share = (item['amount'] * (pct / decimal.Decimal('100'))).quantize(
                        decimal.Decimal('0.01'), rounding=decimal.ROUND_HALF_UP
                    )
                    
                    if not item_share.is_zero():
                        owner_totals[owner_id]['items'].append({
                            'horse_id': horse_id,
                            'horse_name': horse_name,
                            'item_type': item['item_type'],
                            'description': item['description'],
                            'owner_share': item_share
                        })
                
                # Only include races NOT on all-in days
                horse_races = [r for r in races_this_month if r['horse_id'] == horse_id]
                filtered_horse_races = [
                    r for r in horse_races
                    if r.get('race_date') not in all_in_covered_days
                ]

                # Add race start fees only for races NOT on all-in days
                race_items = calculate_race_fees(horse_id, horse_name, filtered_horse_races, pct)
                for item in race_items:
                    owner_totals[owner_id]['items'].append(item)                
                
                # Calculate shipping costs
                shipping_cost = horse_total_shipping.get(horse_id, decimal.Decimal('0.0'))
                if shipping_cost > decimal.Decimal('0.0'):
                    ship_owner_share = (shipping_cost * (pct / decimal.Decimal('100'))).quantize(
                        decimal.Decimal('0.01'), rounding=decimal.ROUND_HALF_UP
                    )
                    
                    if not ship_owner_share.is_zero():
                        owner_totals[owner_id]['items'].append({
                            'horse_id': horse_id,
                            'horse_name': horse_name,
                            'item_type': 'Shipping',
                            'description': f"Shipping Costs for {month:02d}/{year}",
                            'owner_share': ship_owner_share
                        })
                
                # Add other expenses (excluding those already consumed by board calculations)
                for exp in expense_details.get(horse_id, []):
                    exp_id = exp.get('expense_id')
                    exp_date = exp.get('expense_date')

                    # --- NEW: Skip if expense date is covered by an all-in day ---
                    if exp_date in all_in_covered_days:
                        continue

                    # Skip if this expense was already used in board calculations
                    if exp_id and (exp_id in subcontract_consumed_ids or exp_id == split_config_expense_id):
                        continue

                    # Skip BOARD_SPLIT_CONFIG expense type
                    if exp.get('expense_type') == 'BOARD_SPLIT_CONFIG':
                        continue

                    etype = (exp.get('expense_type') or 'other').lower().strip()
                    exp_amount = to_decimal(exp.get('amount'))

                    if exp_amount.is_zero():
                        continue

                    exp_notes = exp.get('notes', etype.title())
                    item_share = (exp_amount * (pct / decimal.Decimal('100'))).quantize(
                        decimal.Decimal('0.01'), rounding=decimal.ROUND_HALF_UP
                    )

                    if item_share.is_zero():
                        continue

                    item_type = "Manual Credit" if etype == 'manual_credit' else etype.title()

                    owner_totals[owner_id]['items'].append({
                        'horse_id': horse_id,
                        'horse_name': horse_name,
                        'item_type': item_type,
                        'description': exp_notes,
                        'owner_share': item_share
                    })

                    print(f"🧾 Added {item_type} of ${item_share} for {horse_name} → Owner: {owner_name}")

                # Calculate horse subtotal for this owner
                horse_subtotal = sum(
                    to_decimal(item.get('owner_share'))
                    for item in owner_totals[owner_id]['items']
                    if item.get('horse_id') == horse_id
                )
                
                # Add to owner's grand total
                owner_grand_total_share += horse_subtotal
            
            # Set the final total for the owner
            owner_totals[owner_id]['total'] = owner_grand_total_share.quantize(
                decimal.Decimal('0.01'), rounding=decimal.ROUND_HALF_UP
            )
        
        # Remove owners with no billable horses/items
        owners_to_remove = []
        for owner_id, data in owner_totals.items():
            items = data['items']
            # Filter items that belong to horses (exclude summary/adjustments with horse_id None)
            horse_items = [item for item in items if item.get('horse_id') is not None]
            if not horse_items:
                print(f"Skipping owner {owner_id} because no active horses to bill.")
                owners_to_remove.append(owner_id)

        for owner_id in owners_to_remove:
            del owner_totals[owner_id]

        # --- Insert into Billing and BillingItem tables ---
        insert_billing_data(cursor, conn, owner_totals, month, year, bill_date, due_date)
        
    except mysql.connector.Error as err:
        print(f"❌ Database Connection Error: {err}")
        if conn:
            conn.rollback()  # Rollback if connection failed mid-process
        sys.exit(1)
    
    except Exception as e:
        print(f"❌ An unexpected error occurred during invoice generation: {e}")
        if conn:
            conn.rollback()
        sys.exit(1)
    
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()
            print("Database connection closed.")

# --- Main Execution Block ---
if __name__ == "__main__":
    try:
        billing_month, billing_year = parse_arguments()
        generate_invoices(month=billing_month, year=billing_year)
    except ValueError as e:
        print(f"Error: {e}")
        import sys
        sys.exit(1)