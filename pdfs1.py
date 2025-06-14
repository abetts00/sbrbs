import os
import argparse
from fpdf import FPDF
import mysql.connector
from datetime import datetime, timedelta
from collections import defaultdict
import decimal
import calendar
import uuid  # For generating invoice numbers

# --- Database Configuration ---
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': 'Sweetlou47',
    'database': 'horse_stable'
}

# --- Constants ---
LOGO_PATH = r"C:\Users\abett\Documents\Billing\static\stablelogo.jpg"
OUTPUT_DIR = "invoices"
HEADER_TITLE_TEMPLATE = "Invoice – Scott Betts Racing Stable"
PAGE_WIDTH = 210
MARGIN_LR = 10  # Left/Right margin in mm
EFFECTIVE_WIDTH = PAGE_WIDTH - (2 * MARGIN_LR)

# Company Information
COMPANY_NAME = "Scott Betts Racing Stable"
COMPANY_ADDRESS = "9930 Hidden Hollow Trail, Broadview Heights, OH 44147"
COMPANY_PHONE = "724-986-1416"  # Replace with actual phone
COMPANY_EMAIL = "abetts00@gmail.com"  # Replace with actual email
PAYMENT_TERMS = "Due within 30 days of receipt"

# Colors (RGB)
PRIMARY_COLOR = (0, 51, 102)  # Dark blue
SECONDARY_COLOR = (230, 230, 230)  # Light grey
ACCENT_COLOR = (242, 242, 242)  # Very light grey for alternating rows

# --- Ensure output directory exists ---
if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

def sanitize_text(text):
    """Removes characters incompatible with FPDF's standard fonts."""
    if text is None: 
        return ''
    
    if not isinstance(text, str):
        text = str(text)
    
    # Replace common problematic characters
    replacements = {
        '\u2013': '-',    # en dash to hyphen
        '\u2014': '-',    # em dash to hyphen
        '\u2018': "'",    # curly quotes
        '\u2019': "'",
        '\u201C': '"',    # curly double quotes
        '\u201D': '"',
        '\u2022': '*',    # bullet to asterisk
        '\u2026': '...',  # ellipsis
    }
    
    for old, new in replacements.items():
        text = text.replace(old, new)
    
    # Convert to ASCII only
    result = ''
    for c in text:
        if ord(c) < 128:  # ASCII only
            result += c
        else:
            result += '?'
    
    return result

def get_latest_billing_month_and_year():
    """Fetches the month and year OF THE MOST RECENT bill_date found."""
    conn = None
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        cursor.execute("SELECT MAX(bill_date) FROM Billing")
        result = cursor.fetchone()
        if result and result[0]:
            latest_date = result[0]
            return latest_date.month, latest_date.year
        else:
            today = datetime.today()
            first_day_current_month = today.replace(day=1)
            bill_date_for_prev_month = first_day_current_month
            return bill_date_for_prev_month.month, bill_date_for_prev_month.year
    except mysql.connector.Error as e:
        print(f"Error fetching latest billing month: {e}")
        today = datetime.today()
        first_day_current_month = today.replace(day=1)
        bill_date_for_prev_month = first_day_current_month
        return bill_date_for_prev_month.month, bill_date_for_prev_month.year
    finally:
        if conn and conn.is_connected(): cursor.close(); conn.close()

class InvoicePDF(FPDF):
    def __init__(self, owner_name, invoice_number, bill_date, due_date, period_label):
        super().__init__()
        self.owner_name     = owner_name
        self.invoice_number = invoice_number
        self.bill_date      = bill_date
        self.due_date       = due_date
        self.period_label   = period_label
        self.page_count     = 0

    def header(self):
        # increment page counter & set header text color
        self.page_count += 1
        self.set_text_color(*PRIMARY_COLOR)

        # — Logo on the left —
        logo_w = 30
        if os.path.exists(LOGO_PATH):
            try:
                self.image(LOGO_PATH, x=MARGIN_LR, y=10, w=logo_w)
            except Exception as img_err:
                print(f"⚠️ Logo loading failed: {img_err}")

        # — Company details on the right —
        self.set_font('Arial', 'B', 14)
        self.cell(logo_w + 5)  # move past logo
        self.cell(0, 6, COMPANY_NAME, ln=1)

        self.set_font('Arial', '', 9)
        self.cell(logo_w + 5)
        self.cell(0, 5, COMPANY_ADDRESS, ln=1)

        self.cell(logo_w + 5)
        self.cell(0, 5, f"Phone: {COMPANY_PHONE} | Email: {COMPANY_EMAIL}", ln=1)
        self.ln(5)

        # — Invoice title/details on first page only —
        if self.page_count == 1:
            self.set_font('Arial', 'B', 16)
            self.cell(0, 10, "INVOICE", ln=1, align='C')
            self.ln(5)

            self.set_font('Arial', 'B', 10)
            self.cell(40, 6, "BILLED TO:", ln=0)
            self.set_font('Arial', '', 10)
            self.cell(0, 6, sanitize_text(self.owner_name), ln=1)

            self.set_font('Arial', 'B', 10)
            self.cell(40, 6, "INVOICE PERIOD:", ln=0)
            self.set_font('Arial', '', 10)
            self.cell(0, 6, self.period_label, ln=1)

            # right-side details
            self.set_font('Arial', 'B', 10)
            self.set_xy(PAGE_WIDTH - MARGIN_LR - 70, self.get_y() - 12)
            self.cell(30, 6, "INVOICE #:", ln=0)
            self.set_font('Arial', '', 10)
            self.cell(40, 6, self.invoice_number, ln=1)

            self.set_font('Arial', 'B', 10)
            self.set_xy(PAGE_WIDTH - MARGIN_LR - 70, self.get_y())
            self.cell(30, 6, "DATE:", ln=0)
            self.set_font('Arial', '', 10)
            self.cell(40, 6, self.bill_date, ln=1)

            self.set_font('Arial', 'B', 10)
            self.set_xy(PAGE_WIDTH - MARGIN_LR - 70, self.get_y())
            self.cell(30, 6, "DUE DATE:", ln=0)
            self.set_font('Arial', '', 10)
            self.cell(40, 6, self.due_date, ln=1)

            self.ln(10)
        else:
            # continuation pages
            self.ln(5)
            self.set_xy(MARGIN_LR + logo_w + 5, self.get_y())
            self.set_font('Arial', 'B', 10)
            self.cell(0, 6, f"Invoice #{self.invoice_number} - {self.owner_name} (continued)", ln=1)
            self.ln(15)

    def footer(self):
        # position 15mm from bottom
        self.set_y(-15)
        self.set_font('Arial', 'I', 8)
        self.set_text_color(128, 128, 128)
        self.cell(0, 10, f"Page {self.page_count}", align='C')

        # payment terms
        self.set_y(-10)
        self.cell(0, 6, f"Payment Terms: {PAYMENT_TERMS}", align='C')

    def add_payment_section(self, billing_id, owner_id, conn):
        """Add a section showing all payments for this owner."""
        cursor = conn.cursor(dictionary=True)
        
        # Get payments for this owner since last invoice
        cursor.execute("""
            SELECT p.payment_date, p.amount, p.method, p.notes
            FROM Payments p
            WHERE p.owner_id = %s 
            AND p.payment_date >= (
                SELECT MAX(bill_date) FROM Billing 
                WHERE owner_id = %s AND bill_id < %s
            )
            ORDER BY p.payment_date
        """, (owner_id, owner_id, billing_id))
        
        payments = cursor.fetchall()
        cursor.close()
        
        if not payments:
            return decimal.Decimal('0.0')  # No payments to show
            
        # Add payment section header
        self.ln(5)
        self.set_font("Arial", 'B', 11)
        self.cell(0, 8, "Payments Received", ln=1)
        
        # Add payment table
        desc_width = EFFECTIVE_WIDTH * 0.75
        amt_width = EFFECTIVE_WIDTH * 0.25
        
        # Payment table header
        self.set_font("Arial", 'B', 10)
        self.set_text_color(255, 255, 255)  # White text
        self.set_fill_color(PRIMARY_COLOR[0], PRIMARY_COLOR[1], PRIMARY_COLOR[2])  # Primary color background
        self.cell(desc_width, 8, "Payment Details", border=1, ln=0, align='C', fill=True)
        self.cell(amt_width, 8, "Amount ($)", border=1, ln=1, align='C', fill=True)
        
        # Reset text color to black
        self.set_text_color(0, 0, 0)
        
        total_payments = decimal.Decimal('0.0')
        
        # List each payment
        for i, payment in enumerate(payments):
            # Alternate row colors
            fill = (i % 2 == 0)
            if fill:
                self.set_fill_color(ACCENT_COLOR[0], ACCENT_COLOR[1], ACCENT_COLOR[2])
            
            payment_date = payment['payment_date'].strftime('%B %d, %Y') if payment['payment_date'] else 'N/A'
            payment_desc = f"  {payment_date}"
            
            # Add payment method if available
            if payment.get('method'):
                payment_desc += f" - {payment['method']}"
                
            # Add notes if available
            if payment.get('notes'):
                payment_desc += f" ({payment['notes']})"
                
            payment_amount = decimal.Decimal(str(payment['amount']))
            total_payments += payment_amount
            
            self.cell(desc_width, 8, payment_desc, border='L', ln=0, align='L', fill=fill)
            self.cell(amt_width, 8, f"{payment_amount:,.2f}", border='R', ln=1, align='R', fill=fill)
        
        # Payment total row
        self.set_font("Arial", 'B', 10)
        self.set_fill_color(240, 240, 240)  # Light grey for subtotal
        self.cell(desc_width, 8, "Total Payments", border="LTB", ln=0, align='R', fill=True)
        self.cell(amt_width, 8, f"{total_payments:,.2f}", border="RTB", ln=1, align='R', fill=True)
        
        return total_payments

# --- Consolidation Functions ---
def consolidate_billing_items(items):
    """Group and consolidate billing items by horse_id and horse_name for correct PDF grouping."""
    from collections import defaultdict
    import decimal

    horse_items = defaultdict(list)

    if not items:
        return horse_items

    # First pass - organize by horse_id AND horse_name (as a tuple key)
    for item in items:
        amount = decimal.Decimal(item.get('item_amount', 0) or 0)
        if not amount.is_zero():
            horse_id = item.get('horse_id')
            horse_name = sanitize_text(item.get('horse_name') or "Unspecified Horse")
            description = sanitize_text(item.get('item_description') or "No Description")
            # Use a composite key of (horse_id, horse_name)
            horse_key = (horse_id, horse_name)
            horse_items[horse_key].append((horse_name, description, amount, item))

    # Second pass - consolidate by track and type
    consolidated_items = defaultdict(list)

    for horse_key, item_list in horse_items.items():
        horse_id, horse_name = horse_key

        # Organize items by category
        board_items = []
        override_items = []
        race_starts_by_track = defaultdict(list)
        race_day_fees_by_track = defaultdict(list)
        shipping_by_track = defaultdict(list)
        other_items = []

        for tpl in item_list:
            desc = tpl[1]
            amount = tpl[2]
            # For future use: item_data = tpl[3]
            if 'Board:' in desc or 'Training & Board' in desc:
                board_items.append((horse_name, desc, amount))
            elif 'Override:' in desc:
                override_items.append((horse_name, desc, amount))
            elif 'Race Starts:' in desc:
                # Extract track from description
                track = None
                if ' at ' in desc:
                    track = desc.split(' at ')[-1].split(' ')[0]
                elif ' - ' in desc:
                    parts = desc.split(' - ')
                    for part in parts:
                        if any(t in part for t in ['MEA', 'PCD', 'YR', 'POC', 'MVR', 'SCD', 'NFLD']):
                            track = next((t for t in ['MEA', 'PCD', 'YR', 'POC', 'MVR', 'SCD', 'NFLD'] if t in part), None)
                            break

                if track:
                    race_starts_by_track[track].append((horse_name, desc, amount))
                else:
                    other_items.append((horse_name, desc, amount))
            elif 'Race_Day_Fee:' in desc:
                parts = desc.split(' - ')
                if len(parts) >= 2:
                    track = parts[-1]
                    if track != 'MEA':
                        fee_type = parts[0].replace('Race_Day_Fee: ', '')
                        race_day_fees_by_track[track].append((horse_name, fee_type, amount))
                else:
                    other_items.append((horse_name, desc, amount))
            elif 'Shipping' in desc:
                track = None
                if ' - ' in desc:
                    parts = desc.split(' - ')
                    for part in parts:
                        if any(t in part for t in ['MEA', 'PCD', 'YR', 'POC', 'MVR', 'SCD', 'NFLD']):
                            track = next((t for t in ['MEA', 'PCD', 'YR', 'POC', 'MVR', 'SCD', 'NFLD'] if t in part), None)
                            break

                if track:
                    shipping_by_track[track].append((horse_name, desc, amount))
                else:
                    shipping_by_track['Other'].append((horse_name, desc, amount))
            else:
                other_items.append((horse_name, desc, amount))

        # Add board items (keep as is)
        for entry in board_items:
            consolidated_items[horse_key].append(entry)

        # Add override items
        for entry in override_items:
            consolidated_items[horse_key].append(entry)

        # Consolidate race starts by track
        for track, starts in race_starts_by_track.items():
            race_count = len(starts)
            total_amount = sum(amount for _, _, amount in starts)
            if track == 'MEA':
                consolidated_desc = f"Race Starts: {race_count} Race Start(s) at MEA @ $200.00/start (all-inclusive fee)"
            else:
                consolidated_desc = f"Race Information: {race_count} Race(s) at {track}"
            consolidated_items[horse_key].append((horse_name, consolidated_desc, total_amount))

        # Consolidate race day fees by track
        for track, fees in race_day_fees_by_track.items():
            if track == 'MEA':
                continue
            fee_types = sorted(set(fee_type for _, fee_type, _ in fees))
            fee_str = "lasix, overnight, paddock, warm up"
            total_fee_amount = sum(amount for _, _, amount in fees)
            consolidated_desc = f"Race Day Fees: {track} (includes {fee_str})"
            consolidated_items[horse_key].append((horse_name, consolidated_desc, total_fee_amount))

        # Consolidate shipping by track
        for track, shipping_items in shipping_by_track.items():
            total_shipping = sum(amount for _, _, amount in shipping_items)
            if track == 'Other':
                consolidated_desc = "Shipping"
            else:
                consolidated_desc = f"Shipping: {track}"
            consolidated_items[horse_key].append((horse_name, consolidated_desc, total_shipping))

        # Add remaining items
        for entry in other_items:
            consolidated_items[horse_key].append(entry)

    # Final pass - convert back to dictionary keyed by horse_name
    result = {}
    for (horse_id, horse_name), items in consolidated_items.items():
        result[horse_id, horse_name] = items

    return result

# --- Main PDF Generation Function ---
def generate_all_pdfs(target_month=None, target_year=None):
    # Use passed arguments if provided; otherwise, fall back to last billing period
    if target_month and target_year:
        billing_period_month = target_month
        billing_period_year = target_year
    else:
        latest_bill_month, latest_bill_year = get_latest_billing_month_and_year()
        billing_period_month = latest_bill_month - 1 if latest_bill_month > 1 else 12
        billing_period_year = latest_bill_year if latest_bill_month > 1 else latest_bill_year - 1

    print(f"--- Generating PDFs for Billing Period: {billing_period_month:02d}/{billing_period_year} ---")

    conn = None
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor(dictionary=True, buffered=True)
        cursor.execute("SET SESSION sql_mode = 'TRADITIONAL'")

        cursor.execute("""
            SELECT b.bill_id, b.owner_id, o.name AS owner_name, b.bill_date, b.due_date, b.total_amount
            FROM Billing b JOIN Owners o ON b.owner_id = o.owner_id
            WHERE b.billing_period_month = %s AND b.billing_period_year = %s
            AND b.bill_id = (
                SELECT MAX(bill_id) 
                FROM Billing 
                WHERE owner_id = b.owner_id 
                AND billing_period_month = %s AND billing_period_year = %s
            )
        """, (billing_period_month, billing_period_year, billing_period_month, billing_period_year))

        invoices = cursor.fetchall()

        if not invoices: print(f"No invoices found with Bill Date {billing_period_month:02d}/{billing_period_year}."); return

        print(f"Found {len(invoices)} invoice(s) to generate.")
        count = 0; generation_errors = 0

        for inv in invoices:
            bill_id = inv['bill_id']; owner_name = inv['owner_name']
            filename = f"invoice_{owner_name.replace(' ', '_')}_{billing_period_month}_{billing_period_year}_id{bill_id}.pdf"
            filepath = os.path.join(OUTPUT_DIR, filename)
            print(f"  Generating: {filename} (Bill ID: {bill_id})")

            # Generate a unique invoice number if not already in the database
            invoice_number = f"SB-{billing_period_year}{billing_period_month:02d}-{bill_id:04d}"
            
            # Format dates
            bill_date_str = inv['bill_date'].strftime('%B %d, %Y') if inv['bill_date'] else 'N/A'
            due_date_str = inv['due_date'].strftime('%B %d, %Y') if inv['due_date'] else 'N/A'
            period_label = f"{calendar.month_name[billing_period_month]} {billing_period_year}"
            
            # Initialize our custom PDF class
            pdf = InvoicePDF(owner_name, invoice_number, bill_date_str, due_date_str, period_label)
            pdf.add_page()
            pdf.set_auto_page_break(auto=True, margin=25)  # Larger margin for footer
                        
            # Reset text color to black for table content
            pdf.set_text_color(0, 0, 0)

            # --- Fetch & Group Items ---
            try:
                cursor.execute("SELECT horse_id, horse_name, item_description, item_amount FROM BillingItem WHERE billing_id = %s ORDER BY horse_name, item_description", (bill_id,))
                items = cursor.fetchall()
            except mysql.connector.Error as item_err: 
                print(f"❌ Error fetching items for Bill ID {bill_id}: {item_err}")
                items = []

            # Consolidate items for cleaner display
            horse_items_display = consolidate_billing_items(items)
            # Remove/pull summary items (opening balances) so they are not treated as horses
            summary_items = horse_items_display.pop((None, 'Summary'), [])  # for tuple keys
            if not summary_items:
                summary_items = horse_items_display.pop('Summary', [])      # fallback if string key used
            # Calculate the opening balance subtotal
            # summary_subtotal = sum(amount for (_, _, amount) in summary_items) if summary_items else decimal.Decimal('0.00')                    
            has_any_items = any(len(items) > 0 for items in horse_items_display.values())
            
            pdf.set_font("Arial", 'B', 10)
            pdf.set_text_color(255, 255, 255)  # White text
            pdf.set_fill_color(PRIMARY_COLOR[0], PRIMARY_COLOR[1], PRIMARY_COLOR[2])  # Primary color background

            desc_width = EFFECTIVE_WIDTH * 0.75
            amt_width = EFFECTIVE_WIDTH * 0.25
            line_height = 8

            pdf.cell(desc_width, line_height, "Description", border=1, ln=0, align='C', fill=True)
            pdf.cell(amt_width, line_height, "Amount ($)", border=1, ln=1, align='C', fill=True)            
            pdf.set_text_color(0, 0, 0)

            # --- Print Items or "No Charges" ---
            total_amount_decimal = decimal.Decimal(inv.get('total_amount', 0) or 0)

            # --- NEW: Calculate Previous Balance by Querying Previous Invoice ---
            cursor.execute("""
                SELECT balance_due
                FROM Billing
                WHERE owner_id = %s
                  AND bill_id < %s
                ORDER BY bill_id DESC
                LIMIT 1
            """, (inv['owner_id'], inv['bill_id']))
            row = cursor.fetchone()
            if row and row['balance_due'] is not None:
                prev_balance = decimal.Decimal(row['balance_due'])
            else:
                prev_balance = decimal.Decimal('0.00')
            
            # Get payments made since last invoice (matching invoice.py logic)
            cursor.execute("""
                SELECT COALESCE(SUM(amount), 0) AS total_payments
                FROM Payments
                WHERE owner_id = %s
                AND payment_date >= COALESCE(
                    (SELECT MAX(bill_date) FROM Billing WHERE owner_id = %s AND bill_id < %s),
                    '1900-01-01'
                )
                AND payment_date < %s  -- Before current invoice date
            """, (inv['owner_id'], inv['owner_id'], inv['bill_id'], inv['bill_date']))
            
            payment_result = cursor.fetchone()
            payments_since_last_invoice = decimal.Decimal(str(payment_result['total_payments']))
            
            # Apply payments to previous balance
            adjusted_prev_balance = max(prev_balance - payments_since_last_invoice, decimal.Decimal('0.00'))
            # --- END NEW ---
            
            # Calculate new charges explicitly (exclude payments and opening balance)
            new_charges_decimal = decimal.Decimal('0.00')
            for it in items:
                desc = it['item_description']
                amt = decimal.Decimal(str(it['item_amount']))
                if not desc.startswith('Payment') and not desc.startswith('Opening Balance'):
                    new_charges_decimal += amt

            new_charges = new_charges_decimal.quantize(decimal.Decimal('0.01'), rounding=decimal.ROUND_HALF_UP)


            if total_amount_decimal.is_zero() and not has_any_items:
                pdf.set_font("Arial", 'I', 10)
                pdf.cell(0, 10, "No charges for this billing period.", ln=1, border="LR")
            elif not has_any_items and not total_amount_decimal.is_zero():
                pdf.set_font("Arial", 'I', 10)
                pdf.multi_cell(0, 8, f"Note: Billing items not found or all zero, but Total Due is ${total_amount_decimal:.2f}.", border="LR")
            else:
                pdf.set_font("Arial", '', 10)  # Font for items
                item_line_height = 6  # Slightly smaller line height for items
                row_index = 0  # For alternating row colors
                
                # Pull out the Prior Balance summary so it won't be printed as a horse
                # summary_items = horse_items_display.pop('Summary', [])
                # summary_subtotal = sum(amount for (_, amount) in summary_items)
                
                # Iterate through horses (grouped by horse_id)
                for (horse_id, horse_name), item_list in sorted(horse_items_display.items()):
                    pdf.set_fill_color(220, 230, 241)
                    pdf.set_font("Arial", 'B', 11)
                    pdf.cell(0, 8, horse_name, ln=1, fill=True, border="LTR")
                    pdf.set_font("Arial", '', 10)
                    # ...rest of the code...


                    horse_subtotal = decimal.Decimal('0.0')
                    
                    # Check if we need a new page before starting items
                    if pdf.get_y() > 260:
                        pdf.add_page()
                    
                    # Now item_list is a list of (horse_name, desc, amt)
                    for _, desc, amt in item_list:
                        # (your row color, cell drawing logic here; unchanged)
                        row_index += 1
                        if row_index % 2 == 0:
                            pdf.set_fill_color(ACCENT_COLOR[0], ACCENT_COLOR[1], ACCENT_COLOR[2])
                            fill = True
                        else:
                            fill = False
                        
                        desc_lines = pdf.multi_cell(desc_width, item_line_height, f"  {desc}", split_only=True)
                        cell_height = max(item_line_height, len(desc_lines) * item_line_height)
                        
                        if pdf.get_y() + cell_height > 260:
                            pdf.add_page()
                            row_index = 1
                            fill = False
                        
                        start_x = pdf.get_x()
                        start_y = pdf.get_y()
                        pdf.multi_cell(desc_width, item_line_height, f"  {desc}", border='L', align='L', fill=fill)
                        after_multicell_y = pdf.get_y()
                        
                        pdf.set_xy(start_x + desc_width, start_y)
                        pdf.cell(amt_width, cell_height, f"{amt:,.2f}", border='R', ln=1, align='R', fill=fill)
                        
                        pdf.set_y(max(after_multicell_y, start_y + cell_height))
                        
                        horse_subtotal += amt
                    
                    # Print Subtotal Per Horse
                    pdf.set_font("Arial", 'B', 10)
                    pdf.set_fill_color(240, 240, 240)
                    pdf.cell(desc_width, line_height, "Subtotal for " + sanitize_text(horse_name), border="LTB", ln=0, align='R', fill=True)
                    pdf.cell(amt_width, line_height, f"{horse_subtotal:,.2f}", border="RTB", ln=1, align='R', fill=True)
                    pdf.ln(5)
                    pdf.set_font("Arial", '', 10)

                    horse_subtotal = decimal.Decimal('0.0')
                    
                    # Check if we need a new page before starting items
                                        
                    # Add earnings notes if applicable
                    has_earnings_credit = any("Earnings Credit" in desc for _, desc, _ in item_list)
                    if has_earnings_credit:
                        all_positive_earnings = all(amt >= 0 for _, desc, amt in item_list if "Earnings Credit" in desc)

                        pdf.set_font("Arial", 'I', 9)
                        if all_positive_earnings:
                            pdf.multi_cell(0, 5, "Note: This owner receives purse checks directly. No race earnings credited.", border=0)
                        else:
                            pdf.multi_cell(0, 5, "Note: 90% of race earnings credited (5% driver / 5% trainer withheld)", border=0)
                        pdf.ln(2)
                        pdf.set_font("Arial", '', 10)

            payment_total = pdf.add_payment_section(bill_id, inv['owner_id'], conn)

            # --- Calculate Adjusted Total ---
            # Calculate Total Due properly
            total_due = adjusted_prev_balance + new_charges - payment_total
            actual_invoice_balance_to_store = total_due

            # Show zero if credit balance, but store actual balance
            display_total_due = max(total_due, decimal.Decimal('0.00'))

            # Print Summary Boxes
            pdf.ln(5)

            # Previous Balance
            pdf.set_fill_color(220, 220, 220)
            pdf.set_text_color(0, 0, 0)
            pdf.set_font("Arial", 'B', 10)
            pdf.cell(desc_width, 8, "Previous Balance", border=1, align='R', fill=True)
            pdf.cell(amt_width, 8, f"${prev_balance:,.2f}", border=1, ln=1, align='R', fill=True)

            # Less: Payments Applied to Previous Balance
            if payments_since_last_invoice > 0:
                pdf.set_fill_color(235, 235, 235)
                pdf.cell(desc_width, 8, "Less: Payments Applied", border=1, align='R', fill=True)
                pdf.cell(amt_width, 8, f"-${payments_since_last_invoice:,.2f}", border=1, ln=1, align='R', fill=True)

            # New Charges
            pdf.set_fill_color(245, 245, 245)
            pdf.cell(desc_width, 8, "New Charges This Period", border=1, align='R', fill=True)
            pdf.cell(amt_width, 8, f"${new_charges:,.2f}", border=1, ln=1, align='R', fill=True)

            # Less: Payments This Period
            if payment_total > 0:
                pdf.set_fill_color(ACCENT_COLOR[0], ACCENT_COLOR[1], ACCENT_COLOR[2])
                pdf.cell(desc_width, 8, "Less: Payments This Period", border=1, align='R', fill=True)
                pdf.cell(amt_width, 8, f"-${payment_total:,.2f}", border=1, ln=1, align='R', fill=True)

            # TOTAL DUE
            pdf.set_fill_color(*PRIMARY_COLOR)
            pdf.set_text_color(255, 255, 255)
            pdf.set_font("Arial", 'B', 12)
            pdf.cell(desc_width + amt_width, 10,
                     f"TOTAL DUE   ${display_total_due:,.2f}",
                     border=1, ln=1, align='C', fill=True)

            # Reset text color and font for any following content
            pdf.set_text_color(0, 0, 0)
            pdf.set_font("Arial", '', 10)


            # Reset for any following text
            pdf.set_text_color(0, 0, 0)
            pdf.set_font("Arial", '', 10)

            # Update the balance_due field in the Billing table
            
            try:
                update_cursor = conn.cursor()
                update_cursor.execute("""
                    UPDATE Billing 
                    SET balance_due = %s
                    WHERE bill_id = %s
                """, (actual_invoice_balance_to_store, bill_id))
                conn.commit()
                update_cursor.close()
            except Exception as e:
                print(f"Warning: Could not update balance_due for bill_id {bill_id}: {e}")
            
            # Reset text color to black
            pdf.set_text_color(0, 0, 0)
            
            # Add payment section
            pdf.ln(10)
            pdf.set_font("Arial", 'B', 11)
            pdf.cell(0, 8, "Payment Information", ln=1)
            pdf.set_font("Arial", '', 10)
            pdf.multi_cell(0, 5, sanitize_text(
                f"Please make checks payable to: Betts Equine Performance\n"
                f"Mail to: {COMPANY_ADDRESS}\n\n"
                f"Thank you for your business!"
            ))

            # --- Output PDF ---
            try: 
                pdf.output(filepath)
                count += 1
            except Exception as pdf_err: 
                print(f"❌ Error saving PDF {filename}: {pdf_err}")
                generation_errors += 1

        print(f"--- PDF Generation Complete ---")
        print(f"✅ {count} PDF(s) generated. {generation_errors} errors.")

    except mysql.connector.Error as db_err: 
        print(f"❌ Database Error: {db_err}")
    except Exception as e: 
        print(f"❌ Unexpected error: {e}")
    finally:
        if conn and conn.is_connected(): 
            cursor.close()
            conn.close()
            print("Database connection closed.")

# --- Main Execution ---
def parse_args():
    parser = argparse.ArgumentParser(description="Generate PDF invoices for a specific billing month and year.")
    parser.add_argument('-m', '--month', type=int, required=False, help='Billing period month (1-12)')
    parser.add_argument('-y', '--year', type=int, required=False, help='Billing period year (e.g. 2025)')
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_args()
    # Pass to the main PDF generation function
    if args.month and args.year:
        generate_all_pdfs(target_month=args.month, target_year=args.year)
    else:
        generate_all_pdfs()
