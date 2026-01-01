import mysql.connector
from datetime import date, timedelta, datetime
from calendar import monthrange
from collections import defaultdict
import sys  # For exiting on critical errors
import decimal  # Use Decimal for financial calculations
import os  # For environment variables
import argparse
from decimal import Decimal, ROUND_HALF_UP

# --- CONFIGURE THIS ---
# Use environment variables for security
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': 'Sweetlou47',  # Consider using environment variables or a config file for security
    'database': 'horse_stable'
}

# --- RATES AND CONSTANTS ---
DISCOUNTED_OWNERS = ['Tim Betts', 'Andrew Betts', 'Scott Betts', 'Kim Betts']
DEFAULT_RATE = decimal.Decimal('75.00')
DISCOUNT_RATE = decimal.Decimal('65.00')
TURNOUT_RATE = decimal.Decimal('25.00')
BROODMARE_RATE = decimal.Decimal('45.00')
RACE_FEE = decimal.Decimal('200.00')  # Fee per race start
STATE_CONFIG = {
    'in_training':                   {'owner_rate_rule': 'normal',          'internal_cost': None},
    'turned_out':                    {'owner_rate_rule': 'flat_25',         'internal_cost': None},
    'rehab_in_stable':               {'owner_rate_rule': 'normal_minus_10', 'internal_cost': None},
    'rehab_center':                  {'owner_rate_rule': 'normal_minus_10', 'internal_cost': {'type': 'per_day',             'amount': 50,   'vendor': 'Rehab Center'}},
    'swimming':                      {'owner_rate_rule': 'normal',          'internal_cost': {'type': 'per_day',             'amount': 50,   'vendor': 'Swimming Program'}},
    'out_with_trainer_pantaleano':   {'owner_rate_rule': 'flat_80',         'internal_cost': {'type': 'per_month_prorated',  'amount': 2400, 'vendor': 'Jim Pantaleano'}},
    'breaking_with_leon':            {'owner_rate_rule': 'flat_45',          'internal_cost': {'type': 'per_day',             'amount': 45,   'vendor': 'Colt Breaking'}},
    'broodmare':                     {'owner_rate_rule': 'flat_40',        'internal_cost': None},
    'inactive':                      {'owner_rate_rule': 'zero',            'internal_cost': None},
    'sold':                          {'owner_rate_rule': 'zero',            'internal_cost': None},
}
TRACK_SHIPPING_COSTS = {
    'PCD': decimal.Decimal('655.00'),
    'NFLD': decimal.Decimal('315.00'),
    'MVR': decimal.Decimal('530.00'),
    'MEA': decimal.Decimal('0.00'),
    'SCD': decimal.Decimal('500.00'),
    'DELA': decimal.Decimal('500.00')
}

FEE_CONFIG = {
    'LASIX': {'DEFAULT': decimal.Decimal('25.00')},
    'WARMUP': {'DEFAULT': decimal.Decimal('10.00')},
    'PADDOCK': {
        'PCD': decimal.Decimal('100.00'), 
        'NFLD': decimal.Decimal('100.00'), 
        'MVR': decimal.Decimal('100.00'), 
        'MEA': decimal.Decimal('0.00'),
        'SCD': decimal.Decimal('100.00'),
        'DELA': decimal.Decimal('100.00'),
    },
    'OVERNIGHT': {'PCD': decimal.Decimal('10.00')}
}
TEN = Decimal('10.00')

def state_label_for_invoice(status):
    """Convert horse status to human-readable label for invoices."""
    status_labels = {
        'in_training': 'Training & Board',
        'turned_out': 'Turnout',
        'rehab_in_stable': 'Rehabilitation (In-Stable)',
        'rehab_center': 'Rehabilitation (Center)', 
        'swimming': 'Swimming Program',
        'out_with_trainer_pantaleano': 'Training with Jim Pantaleano',
        'broodmare': 'Broodmare Care',
        'breaking_with_leon': 'Colt Breaking',
        'inactive': 'Inactive',
        'sold': 'Sold',
    }
    return status_labels.get(status, 'Training & Board')

# --- END CONFIGURATION ---
OPEN_DATE = date(9999, 12, 31)  # ok to keep for other queries, not used here

SPAN_SQL = """
SELECT
  status_code,
  CAST(GREATEST(start_date, %s)          AS DATE) AS eff_start,
  CAST(LEAST(COALESCE(end_date, %s), %s) AS DATE) AS eff_end
FROM horse_status_history
WHERE
  horse_id = %s
  AND LOWER(TRIM(status_group)) = 'billing'   -- hardened
  AND start_date <= %s
  AND COALESCE(end_date, %s) >= %s
ORDER BY eff_start
"""

STATUS_ALIASES = {
    'training': 'in_training',
    'in stable': 'in_training',
    'in_stable': 'in_training',
    'in-training': 'in_training',
    'pantaleano': 'out_with_trainer_pantaleano',
    'swimming_in_stable': 'swimming',
}
def normalize_status(code: str) -> str:
    c = (code or '').strip().lower()
    return STATUS_ALIASES.get(c, c)

def get_billing_spans(conn, horse_id, period_start, period_end):
    """
    Return [(status_code, eff_start, eff_end, days)] intersecting the month.
    """
    temp_cursor = conn.cursor(dictionary=True, buffered=True)
    try:
        # Convert Python date objects to strings for MySQL
        ps_str = period_start.strftime('%Y-%m-%d') if isinstance(period_start, date) else str(period_start)
        pe_str = period_end.strftime('%Y-%m-%d') if isinstance(period_end, date) else str(period_end)
        
        # DEBUG: Print for specific horses
        if horse_id in (8, 11):
            print(f"   DEBUG get_billing_spans: horse_id={horse_id}")
            print(f"   DEBUG: period_start={ps_str}, period_end={pe_str}")
        
        temp_cursor.execute(
            SPAN_SQL,
            (ps_str, pe_str, pe_str, horse_id, pe_str, ps_str, ps_str)
        )
        rows = temp_cursor.fetchall()
        
        # DEBUG: Show what we got
        if horse_id in (8, 11):
            print(f"   DEBUG: Query returned {len(rows)} rows")
            for r in rows:
                print(f"      → {r['status_code']}: {r['eff_start']} to {r['eff_end']}")

        spans = []
        for r in rows:
            s = as_date(r['eff_start'])
            e = as_date(r['eff_end'])
            if not (s and e and s <= e):
                continue
            code = normalize_status(r['status_code'])
            spans.append((code, s, e, (e - s).days + 1))

        if spans:
            return spans

        # Optional safety net: carry forward last known billable status
        temp_cursor.execute("""
            SELECT status_code, start_date, end_date
            FROM horse_status_history
            WHERE horse_id = %s
              AND LOWER(TRIM(status_group)) = 'billing'
              AND start_date <= %s
            ORDER BY start_date DESC
            LIMIT 1
        """, (horse_id, pe_str))
        last = temp_cursor.fetchone()
        if last:
            code = normalize_status(last['status_code'])
            if code not in ('inactive', 'sold'):
                s = period_start
                e = min(as_date(last['end_date']) or period_end, period_end)
                if s <= e:
                    return [(code, s, e, (e - s).days + 1)]

        return []
    finally:
        temp_cursor.close()

def as_date(v):
    """Convert DB values to date. Accepts date, datetime, 'YYYY-MM-DD', or 'YYYY-MM-DD ...'."""
    if v is None:
        return None
    if isinstance(v, date):
        return v
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, str):
        s = v.strip()
        # Handle 'YYYY-MM-DD' and 'YYYY-MM-DD HH:MM:SS'
        try:
            return datetime.strptime(s[:10], "%Y-%m-%d").date()
        except Exception as e:
            raise ValueError(f"Could not parse date string {v!r}") from e
    raise TypeError(f"Expected date/datetime/str, got {type(v).__name__}: {v!r}")

def month_window(year: int, month: int):
    from calendar import monthrange
    start = date(year, month, 1)
    end = date(year, month, monthrange(year, month)[1])
    return start, end

def owner_daily_rate_from_state(owner_name: str, status_code: str) -> Decimal:
    """Resolve the daily board rate from STATE_CONFIG owner_rate_rule."""
    meta = STATE_CONFIG.get(status_code, {'owner_rate_rule': 'zero'})
    return owner_rate_from_rule(meta['owner_rate_rule'], owner_name)

def vendor_charge_for_span(status_code: str, s: date, e: date) -> tuple[Decimal, str] | None:
    """
    If STATE_CONFIG[status]['internal_cost'] is set, compute the vendor charge for this (s..e) span.
    Returns (amount, description) or None.
    """
    meta = STATE_CONFIG.get(status_code, {})
    ic = meta.get('internal_cost')
    if not ic:
        return None
    days = (e - s).days + 1
    if ic['type'] == 'per_day':
        amt = Decimal(str(ic['amount'])) * days
        desc = f"{ic['vendor']}: {state_label_for_invoice(status_code)} {s:%Y-%m-%d}–{e:%Y-%m-%d} ({days}d)"
        return amt, desc
    if ic['type'] == 'per_month_prorated':
        # span is already clipped to the month, so just pro-rate by days in that month
        from calendar import monthrange
        month_days = monthrange(s.year, s.month)[1]
        daily = Decimal(str(ic['amount'])) / Decimal(month_days)
        amt = (daily * days).quantize(Decimal('0.01'))
        desc = f"{ic['vendor']}: {state_label_for_invoice(status_code)} {s:%b %Y} prorated ({days}/{month_days}d)"
        return amt, desc
    return None

def days_overlap(start1, end1, start2, end2):
    """Returns (number of overlapping days, latest_start, earliest_end) between two date ranges."""
    latest_start = max(start1, start2)
    earliest_end = min(end1, end2)
    delta = (earliest_end - latest_start).days + 1
    return max(0, delta), latest_start, earliest_end

def owner_base_rate(owner_name: str) -> Decimal:
    # Uses your existing DISCOUNTED_OWNERS, DEFAULT_RATE, DISCOUNT_RATE
    return DISCOUNT_RATE if owner_name in DISCOUNTED_OWNERS else DEFAULT_RATE

def owner_rate_from_rule(rule: str, owner_name: str) -> Decimal:
    base = owner_base_rate(owner_name)
    rule_normalized = (rule or '').strip().lower()

    if rule_normalized == 'normal':
        return base
    if rule_normalized == 'normal_minus_10':
        return max(base - TEN, Decimal('0.00'))
    if rule_normalized == 'flat_25':
        return TURNOUT_RATE
    if rule_normalized == 'flat_45':
        return BROODMARE_RATE
    if rule_normalized == 'zero':
        return Decimal('0.00')

    if rule_normalized.startswith('flat_'):
        try:
            flat_value = Decimal(rule_normalized.split('_', 1)[1])
        except (IndexError, decimal.InvalidOperation) as exc:
            raise ValueError(f'Unhandled owner_rate_rule: {rule}') from exc
        return flat_value.quantize(Decimal('0.01'))

    raise ValueError(f'Unhandled owner_rate_rule: {rule}')

def prorate_amount(total_amount, total_days, overlap_days):
    """Return amount prorated to overlap_days of total_days."""
    if total_days <= 0:
        return decimal.Decimal('0.00')
    fraction = decimal.Decimal(overlap_days) / decimal.Decimal(total_days)
    return (decimal.Decimal(total_amount) * fraction).quantize(decimal.Decimal('0.01'))

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

def get_owner_balance(cursor, owner_id, up_to_date):
    """
    Owner balance prior to up_to_date:
    charges = sum(BillingItem.item_amount) for owner's bills before up_to_date
    applied = sum(PaymentApplications.amount_applied) applied to those bills
    previous_balance = charges - applied
    NOTE: Uses < up_to_date so you don't double-count the current invoice.
    """

    # Charges on owner's bills strictly before up_to_date
    cursor.execute("""
        SELECT COALESCE(SUM(bi.item_amount), 0) AS prior_charges
        FROM Billing b
        JOIN BillingItem bi ON bi.bill_id = b.bill_id
        WHERE b.owner_id = %s
          AND b.bill_date < %s
    """, (owner_id, up_to_date))
    prior_charges = cursor.fetchone()['prior_charges'] or 0

    # Payments APPLIED to those prior bills (not all owner payments)
    cursor.execute("""
        SELECT COALESCE(SUM(pa.amount_applied), 0) AS prior_applied
        FROM PaymentApplications pa
        JOIN Billing b ON b.bill_id = pa.bill_id
        WHERE b.owner_id = %s
          AND b.bill_date < %s
    """, (owner_id, up_to_date))
    prior_applied = cursor.fetchone()['prior_applied'] or 0

    return (
        decimal.Decimal(prior_charges).quantize(decimal.Decimal("0.01"))
        - decimal.Decimal(prior_applied).quantize(decimal.Decimal("0.01"))
    )


# --- Helper to convert DB values to Decimal ---
def is_all_in_on(check_date, spans):
    """True if any span covering check_date has an internal_cost (i.e., all-in day)."""
    for code, s, e, _ in spans:
        if s <= check_date <= e and STATE_CONFIG.get(code, {}).get('internal_cost'):
            return True
    return False

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
    
    cursor.execute("SELECT owner_id, name, receives_purse_checks, vet_billing_mode FROM owners")
    owners = {o['owner_id']: o for o in cursor.fetchall()}
    
    cursor.execute("SELECT horse_id, name, sale_date, inactive_date, exempt_from_earnings_credit FROM horses")
    all_horses_data = {h['horse_id']: h for h in cursor.fetchall()}
    
    cursor.execute("SELECT horse_id, training_days FROM TrainingDaysOverride WHERE year = %s AND month = %s", (year, month))
    overrides = {row['horse_id']: row['training_days'] for row in cursor.fetchall()}
    
    # --- Ownership (date-overlap if columns exist; otherwise timeless) ---
    try:
        cursor.execute("""
            SELECT 
                o.owner_id, 
                o.horse_id, 
                SUM(o.percentage_ownership) AS percentage_ownership
            FROM ownership o
            WHERE 
                o.start_date <= %s
                AND COALESCE(o.end_date, %s) >= %s
            GROUP BY o.owner_id, o.horse_id
            HAVING SUM(o.percentage_ownership) > 0
        """, (end_date, end_date, start_date))
        ownership_data = cursor.fetchall()
    except mysql.connector.Error as err:
        # 1054 = unknown column; fall back to timeless ownership
        if err.errno == 1054:
            print("ℹ️ Ownership has no start/end dates; using timeless ownership.")
            cursor.execute("""
                SELECT owner_id, horse_id, SUM(percentage_ownership) AS percentage_ownership
                FROM ownership
                GROUP BY owner_id, horse_id
                HAVING SUM(percentage_ownership) > 0
            """)
            ownership_data = cursor.fetchall()
        else:
            raise

    
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
        race_dt = as_date(race['race_date'])
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
        race_date_obj = as_date(r['race_date'])
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
    from collections import defaultdict
    race_grouped = defaultdict(list)
    for race in horse_races:
        race_track = (race.get("track") or "UNKNOWN").strip().upper()
        race_date  = as_date(race.get("race_date"))
        race_grouped[(race_track, race_date)].append(race)
    return race_grouped

def calculate_billed_days(horse, start_date, end_date, default_days):
    billed_days = default_days

    sale_date = as_date(horse.get('sale_date'))
    if sale_date:
        if sale_date < start_date:
            return 0
        if start_date <= sale_date <= end_date:
            billed_days = (sale_date - start_date).days + 1

    inactive_date = as_date(horse.get('inactive_date'))
    if inactive_date:
        if inactive_date < start_date:
            return 0
        if start_date <= inactive_date <= end_date:
            inactive_days = (inactive_date - start_date).days + 1
            if billed_days == default_days or inactive_days < billed_days:
                billed_days = inactive_days

    return billed_days

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

def insert_billing_data(cursor, conn, owner_totals, month, year, bill_date, due_date, horse_spans_for_costs, owner_horses):
    """Insert the calculated billing data into the database."""
    print("Inserting invoice data into database...")
    created_count = 0
    skipped_owners = 0
    
    for owner_id, data in owner_totals.items():
        # Get the items for this owner and recompute THIS-INVOICE total from the items alone
        owner_billing_items = data['items']  # List of item dicts

        # --- NEW STEP 1: Pre-filter items and calculate the true insertion total ---
        billing_item_inserts = []
        final_insert_total = Decimal('0.00')
        bill_id = None # Initialize bill_id before try/except block for safety

        for item in owner_billing_items:
            item_amount = to_decimal(item.get('owner_share'))

            # Key: Only process and include non-zero items
            if item_amount.is_zero():
                continue

            # Original logic to format item description:
            raw_type = (item.get('item_type') or 'Item').strip()
            raw_desc = (item.get('description') or '').strip()

            if raw_desc[:20].lower().startswith((
                'board:', 'race starts:', 'race_day_fee:', 'earnings credit:', 'shipping:', 'manual credit'
            )):
                item_desc = raw_desc or raw_type
            else:
                item_desc = (f"{raw_type}: {raw_desc}".strip(': ').strip() or raw_type)

            # Store as a tuple *without* the bill_id for now:
            billing_item_inserts.append((
                item.get('horse_id'),
                item.get('horse_name', 'Summary'),
                item_desc,
                item_amount
            ))
            final_insert_total += item_amount # Accumulate the total of non-zero items

        # Quantize the final total for the 'billing' table
        owner_final_total_amount = final_insert_total.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
                
        # Skip creating invoice if total is zero AND no items
        if owner_final_total_amount.is_zero() and not owner_billing_items:
            print(f"Skipping invoice for owner {owner_id} (zero balance and no items).")
            skipped_owners += 1
            continue
                                        
        # --- Start Transaction for this Owner ---
        try:
            billing_sql = """
                INSERT INTO billing (owner_id, bill_date, due_date, total_amount, status, billing_period_month, billing_period_year)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            billing_values = (owner_id, bill_date, due_date, owner_final_total_amount, 'pending', month, year)

            cursor.execute(billing_sql, billing_values)
            bill_id = cursor.lastrowid
            print(f"  Created Billing record ID: {bill_id} for Owner ID: {owner_id}")
            
            # === INSERT into BillingItem table ===
            final_billing_item_inserts = [
                (bill_id,) + item_tuple
                for item_tuple in billing_item_inserts # <-- Uses the list filtered in Step 2
            ]

            item_sql = """
                INSERT INTO BillingItem (bill_id, horse_id, horse_name, item_description, item_amount)
                VALUES (%s, %s, %s, %s, %s)
            """

            if final_billing_item_inserts:
                cursor.executemany(item_sql, final_billing_item_inserts)
                print(f"    Inserted {len(final_billing_item_inserts)} non-zero billing items for Billing ID: {bill_id}")
            elif not owner_final_total_amount.is_zero():
                # This check remains, but now confirms if the calculated total had an error
                print(f"    No non-zero billing items were generated to insert for Bill ID: {bill_id}, although total is ${owner_final_total_amount:.2f}")

            horse_ids_for_owner = {h.get('horse_id') for h in owner_horses.get(owner_id, []) if h and h.get('horse_id')}
            for horse_id in horse_ids_for_owner:
                spans = horse_spans_for_costs.get(horse_id, [])
                

                if spans:
                    insert_internal_costs(cursor, bill_id, horse_id, month, year, spans)
                        
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

def insert_internal_costs(cursor, bill_id, horse_id, month, year, status_spans):
    """
    Insert internal costs from STATE_CONFIG for this billing period.
    This tracks what things ACTUALLY COST US (vs what we bill owners).
    """
    internal_cost_inserts = []
    
    for status_code, start_date, end_date, days in status_spans:
        # Check if this status has an internal cost
        meta = STATE_CONFIG.get(status_code, {})
        ic = meta.get('internal_cost')
        
        if ic:
            vendor = ic['vendor']
            
            if ic['type'] == 'per_day':
                daily_cost = Decimal(str(ic['amount']))
                total_cost = daily_cost * days
                description = f"{vendor}: {state_label_for_invoice(status_code)} ({start_date} to {end_date})"
                
            elif ic['type'] == 'per_month_prorated':
                from calendar import monthrange
                month_days = monthrange(start_date.year, start_date.month)[1]
                daily_cost = Decimal(str(ic['amount'])) / Decimal(month_days)
                total_cost = (daily_cost * days).quantize(Decimal('0.01'))
                description = f"{vendor}: {state_label_for_invoice(status_code)} ({days}/{month_days} days prorated)"
            
            internal_cost_inserts.append((
                horse_id,
                bill_id,
                'vendor_service',
                vendor,
                description,
                total_cost,
                days,
                daily_cost,
                month,
                year
            ))
    
    if internal_cost_inserts:
        cursor.executemany("""
            INSERT INTO InternalCosts 
            (horse_id, bill_id, cost_type, vendor_name, description, amount, 
             days, daily_rate, billing_period_month, billing_period_year)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, internal_cost_inserts)
        print(f"    Inserted {len(internal_cost_inserts)} internal cost records")
# --- Main Function ---
def generate_invoices(month, year):
    """
    Generates invoices for the specified month and year based on horse ownership,
    training status, races, and expenses.
    """
    conn = None
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor(dictionary=True, buffered=True)
        cursor.execute("SET SESSION sql_mode = 'TRADITIONAL'")
        print(f"--- Generating invoices for Billing Period: {month:02d}/{year} ---")

        # --- Calculate Dates ---
        try:
            days_in_month = monthrange(year, month)[1]
            start_date = date(year, month, 1)
            end_date = date(year, month, days_in_month)
            bill_date = end_date
            due_date  = bill_date + timedelta(days=15)

            print(f"Billing Period: {start_date} to {end_date}")
            print(f"Bill Date: {bill_date}, Due Date: {due_date}")
        except ValueError as e:
            print(f"❌ Error: Invalid month ({month}) or year ({year}). {e}")
            return

        # --- Fetch data ---
        owners, all_horses_data, overrides, ownership_data, races_this_month, expenses_this_month = fetch_billing_data(
            cursor, month, year, start_date, end_date
        )

        # --- Auto-insert race day fees (non-MEA) then refresh expenses ---
        if insert_race_day_fees(cursor, races_this_month, expenses_this_month):
            conn.commit()
            cursor.execute("SELECT * FROM Expenses WHERE expense_date BETWEEN %s AND %s", (start_date, end_date))
            expenses_this_month = cursor.fetchall()
            print("Refreshed expenses data.")

        # --- Shipping (split per track+date across horses) ---
        horse_shipping_cost_per_trip, horse_total_shipping = calculate_shipping_costs(
            races_this_month, TRACK_SHIPPING_COSTS
        )

        # --- Expenses grouped ---
        expense_map, expense_details = group_expenses(expenses_this_month)

        # --- Ownership grouped by owner ---
        print("Grouping ownership...")
        owner_horses = defaultdict(list)
        for entry in ownership_data:
            percentage = to_decimal(entry.get('percentage_ownership'), default=None)
            if percentage is not None and percentage > 0:
                owner_horses[entry['owner_id']].append({
                    'horse_id': entry['horse_id'],
                    'percentage': percentage
                })

        # --- Per-horse spans (for internal costs & all-in days) ---
        horse_spans_for_costs = {}
        for hid in all_horses_data.keys():
            horse_spans_for_costs[hid] = get_billing_spans(conn, hid, start_date, end_date)
                        
        # --- Build owner_totals structure ---
        owner_totals = defaultdict(lambda: {'total': decimal.Decimal('0.00'), 'items': []})

        
        for owner_id, owner_horse_list in owner_horses.items():
            if owner_id not in owners:
                print(f"⚠️ Warning: Ownership data found for unknown owner_id {owner_id}. Skipping.")
                continue

            owner_detail = owners[owner_id]
            owner_name = owner_detail.get('name', 'Unknown Owner')

            # (Optional) True running balance as of this bill date (not used for inserts here)
            _true_balance_due = get_owner_balance(cursor, owner_id, bill_date)

            # ----- Owner-level Expenses (horse_id is NULL) -----
            # Only if you store owner_id on those Expenses rows:
            for exp in expense_details.get(None, []):
                if exp.get('owner_id') != owner_id:
                    continue
                etype = (exp.get('expense_type') or 'other').lower().strip()
                exp_amount = to_decimal(exp.get('amount'))
                if exp_amount.is_zero():
                    continue

                item_type = "Manual Credit" if etype == 'manual_credit' else etype.title()
                exp_notes = (exp.get('notes') or etype.title()).strip()

                owner_totals[owner_id]['items'].append({
                    'horse_id': None,
                    'horse_name': 'Summary',
                    'item_type': item_type,
                    'description': exp_notes if exp_notes else item_type,
                    'owner_share': exp_amount
                })
                print(f"🧾 Added {item_type} of ${exp_amount} to {owner_name}")

            # ----- Per-horse processing -----
            for horse_entry in owner_horse_list:
                horse_id = horse_entry['horse_id']
                pct = horse_entry['percentage']  # Decimal

                horse = all_horses_data.get(horse_id)
                if not horse:
                    print(f"⚠️ Warning: Horse {horse_id} not found. Skipping.")
                    continue
                horse_name = horse.get('name', 'Unknown Horse')

                print(f"Processing: Owner: {owner_name}, Horse: {horse_name}, Horse ID: {horse_id}")

                spans = horse_spans_for_costs.get(horse_id, [])
                # ---- Clip spans to horse sale/inactive dates (safety guard) ----
                sale_date = as_date(horse.get('sale_date'))
                inactive_date = as_date(horse.get('inactive_date'))
                print(f"   DEBUG: sale_date={sale_date}, inactive_date={inactive_date}")

                clipped_spans = []
                for status_code, s, e, days in spans:
                    hard_end = e
                    if sale_date and sale_date <= hard_end:
                        hard_end = min(hard_end, sale_date - timedelta(days=1))
                    if inactive_date and inactive_date <= hard_end:
                        hard_end = min(hard_end, inactive_date - timedelta(days=1))

                    if hard_end < s:
                        continue
                    new_days = (hard_end - s).days + 1
                    if new_days <= 0:
                        continue
                    clipped_spans.append((status_code, s, hard_end, new_days))

                spans = clipped_spans

                # Always define all_in_set so later filters (races/shipping/expenses) are safe
                all_in_set = set()

                if spans:
                    # Mark all "all-in" days (Pantaleano, rehab_center, breaking, swimming)
                    for status_code, s, e, days in spans:
                        if STATE_CONFIG.get(status_code, {}).get('internal_cost'):
                            d = s
                            while d <= e:
                                all_in_set.add(d)
                                d += timedelta(days=1)

                    # ---- Board: one line per span (handles multi-span months correctly) ----
                    for status_code, s, e, days in spans:
                        print(f"   DEBUG: Processing span: status={status_code}, days={days}, start={s}, end={e}")

                        if days <= 0:
                            print(f"   DEBUG: Skipping span with days <= 0")

                            continue

                        daily_rate = owner_daily_rate_from_state(owner_name, status_code)
                        print(f"   DEBUG: daily_rate for {status_code} = {daily_rate}")
                        if daily_rate.is_zero():
                            print(f"   DEBUG: Skipping span with zero daily rate")
                            continue  # skip non-billable statuses

                        label = state_label_for_invoice(status_code)

                        span_amount = (Decimal(days) * daily_rate).quantize(Decimal('0.01'))
                        owner_share = (span_amount * (pct / Decimal('100'))).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
                        if owner_share.is_zero():
                            continue

                        

                        item_description = f"Board: {label} {s:%m/%d/%Y}-{e:%m/%d/%Y} ({days}d @ ${daily_rate}/day)"
                        owner_totals[owner_id]['items'].append({
                            'horse_id': horse_id,
                            'horse_name': horse_name,
                            'item_type': 'Board',
                            'description': item_description,
                            'owner_share': owner_share
                        })
                else:
                    # No billable status spans this month -> skip board only, but still allow races/shipping/expenses
                    print(f"   No billable status spans for {horse_name} in {month:02d}/{year}; board will be skipped.")

                # ---- Races (exclude all-in days) ----
                horse_races = [r for r in races_this_month if r['horse_id'] == horse_id]
                for r in horse_races:
                    r['race_date'] = as_date(r.get('race_date'))
                filtered_horse_races = [r for r in horse_races if r['race_date'] not in all_in_set]

                for it in calculate_race_fees(horse_id, horse_name, filtered_horse_races, pct):
                    owner_totals[owner_id]['items'].append(it)

                earn = calculate_earnings_credit(horse, owner_detail, horse_races, pct)
                if earn:
                    earn.update({'horse_id': horse_id, 'horse_name': horse_name})
                    owner_totals[owner_id]['items'].append(earn)

                # ---- Shipping (exclude all-in days; covers Pantaleano) ----
                shipping_days = set(horse_shipping_cost_per_trip.get(horse_id, {}).keys())
                billable_shipping_days = shipping_days - all_in_set
                if billable_shipping_days:
                    shipping_cost = sum(horse_shipping_cost_per_trip[horse_id][d] for d in billable_shipping_days)
                    if shipping_cost > Decimal('0.00'):
                        ship_owner_share = (shipping_cost * (pct / Decimal('100'))).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
                        if not ship_owner_share.is_zero():
                            owner_totals[owner_id]['items'].append({
                                'horse_id': horse_id,
                                'horse_name': horse_name,
                                'item_type': 'Shipping',
                                'description': f"Shipping Costs for {month:02d}/{year}",
                                'owner_share': ship_owner_share
                            })

                # ---- Direct expenses (skip race_day_fee on all-in days; vet only if billed via stable) ----
                for exp in expense_details.get(horse_id, []):
                    exp_date = as_date(exp.get('expense_date'))

                    if exp.get('expense_type') == 'race_day_fee' and exp_date in all_in_set:
                        continue

                    etype = (exp.get('expense_type') or 'other').lower().strip()
                    exp_amount = to_decimal(exp.get('amount'))
                    if exp_amount.is_zero():
                        continue

                    if etype == 'vet' and owner_detail.get('vet_billing_mode') != 'stable':
                        continue

                    exp_notes = (exp.get('notes') or etype.title()).strip()
                    item_share = (exp_amount * (pct / Decimal('100'))).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
                    if item_share.is_zero():
                        continue

                    item_type = "Manual Credit" if etype == 'manual_credit' else etype.title()
                    owner_totals[owner_id]['items'].append({
                        'horse_id': horse_id,
                        'horse_name': horse_name,
                        'item_type': item_type,
                        'description': exp_notes if exp_notes else item_type,
                        'owner_share': item_share
                    })
                    print(f"🧾 Added {item_type} of ${item_share} for {horse_name} → Owner: {owner_name}")

            # Owner total (for display/log sanity)
            current_items_total = decimal.Decimal('0.00')
            for it in owner_totals[owner_id]['items']:
                current_items_total += to_decimal(it.get('owner_share'))
            owner_totals[owner_id]['total'] = current_items_total.quantize(decimal.Decimal('0.01'), rounding=decimal.ROUND_HALF_UP)

        # --- Insert into Billing and BillingItem tables ---
        insert_billing_data(cursor, conn, owner_totals, month, year, bill_date, due_date, horse_spans_for_costs, owner_horses)

    except mysql.connector.Error as err:
        print(f"❌ Database Connection Error: {err}")
        if conn:
            conn.rollback()
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