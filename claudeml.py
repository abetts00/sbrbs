import re
import sqlite3
import math
import datetime
import smtplib
from email.message import EmailMessage
from fpdf import FPDF
import os

# ------------------------------
# Helper Function to Parse a Horse Line
# ------------------------------

# Compile once at module-load
LINE_RE = re.compile(
    r'''
    ^\s*(?P<hn>\d+)\s+                              # HN
    (?P<horse>.+?)\s+                               # Horse name (lazy up to next number)
    (?P<pp>\d+)\s+                                  # PP
    (?P<med>\d+)\s+                                 # Med.
    (?P<sts>\d+)\s+
    (?P<w>\d+)\s+
    (?P<p>\d+)\s+
    (?P<s>\d+)\s+
    \$?(?P<earnings>[\d,]+)\s+                      # Earnings ($0 or $4,300)
    (?P<driver>[A-Za-z][A-Za-z\. ]+?)\s+            # Driver (at least one letter, lazy)
    (?P<trainer>[A-Za-z][A-Za-z\. ]+?)\s+           # Trainer
    (?P<odds>\d+-\d+)\s+                            # Odds (e.g. 9-2) 
    \$?(?P<claim>[\d,]+)\s*                         # Claim ($0)
    ''',
    re.VERBOSE
)

def parse_horse_line(line):
    """
    Given a line like:
      1 DIAMOND DESTINY 1 0 0 0 0 $0 Shawn Johnston Neil Balcerak 9-2 $0
    returns a dict with horse_name, driver_name, trainer_name (all lowercase),
    or None if it doesn’t look like a valid starter line.
    """
    cols = re.split(r'\s+', line.strip())
    if not cols or not re.match(r'^\d+$', cols[0]):
        return None

    # 1) Extract horse name tokens up to the first numeric (PP)
    j = 1
    name_tokens = []
    while j < len(cols) and not re.match(r'^\d+$', cols[j]):
        name_tokens.append(cols[j])
        j += 1
    horse_name = " ".join(name_tokens).lower()

    # 2) Find the earnings token (e.g. “$0” or “$4,300”)
    earnings_idx = next(
        (i for i, t in enumerate(cols) if t.startswith('$') and re.match(r'^\$\d', t)),
        None
    )
    if earnings_idx is None:
        return {"horse_name": horse_name, "driver_name": None, "trainer_name": None}

    # 3) Find the odds token (e.g. “9-2”)
    odds_idx = next(
        (i for i, t in enumerate(cols) if re.match(r'^\d+-\d+$', t)),
        None
    )

    # 4) Everything between earnings and odds is driver/trainer
    name_tokens = cols[earnings_idx+1 : (odds_idx or len(cols))]
    driver_name = None
    trainer_name = None
        # Handle suffixes in the driver’s name (jr, sr, ii, iii, etc.)
    suffixes = {"jr", "sr", "ii", "iii"}
    driver_name = trainer_name = None

    if len(name_tokens) >= 5 and name_tokens[2].lower().rstrip(".") in suffixes:
        # e.g. ['ronnie','wrenn','jr.','johnny','neil'] → driver = ['ronnie','wrenn','jr.'], trainer = ['johnny','neil']
        driver_name  = " ".join(name_tokens[:3]).lower()
        trainer_name = " ".join(name_tokens[3:5]).lower()
    elif len(name_tokens) >= 4:
        driver_name  = " ".join(name_tokens[:2]).lower()
        trainer_name = " ".join(name_tokens[2:4]).lower()
    elif len(name_tokens) >= 2:
        driver_name  = name_tokens[0].lower()
        trainer_name = name_tokens[1].lower()

    return {
        "horse_name":   horse_name,
        "driver_name":  driver_name,
        "trainer_name": trainer_name
    }


# ------------------------------
# Race Parsing (based on mass.py logic)
# ------------------------------

def parse_races_from_text(results_text):
    """
    Parses a text file containing race entries into a list of race dictionaries.
    Each race dictionary contains:
      - "race_number": the race number
      - "horses": a list of dictionaries with key "horse_name" for each horse in the race.
      - Now includes driver_name and trainer_name when available
    
    This function assumes that races are delimited by a header like "RACE <num>" and that
    the race table is marked by a header line containing "HN  Horse  PP".
    """
    # Split the text into blocks using "RACE <num>" as a delimiter.
    race_blocks = re.split(r'(RACE\s+\d+)', results_text)
    races = []
    
    for i in range(1, len(race_blocks), 2):
        race_label = race_blocks[i].strip()  # e.g., "RACE 1"
        race_content = race_blocks[i+1].strip()  # content for this race
        
        # 1) parse race number
        m = re.search(r'RACE\s+(\d+)', race_label)
        if not m:
            continue
        race_number = int(m.group(1))

        # 2) grab the very first line (where the date is printed)
        first_line = race_content.split('\n', 1)[0]
        date_match = re.search(
            r'\b(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun),\s+\w+\s+\d{1,2},\s+\d{4}',
            first_line
        )
        if date_match:
            race_date = datetime.datetime.strptime(
                date_match.group(0),
                '%a, %B %d, %Y'
            )
        else:
            race_date = None

        # 3) scan for the HN Horse PP header, then parse each line
        lines = race_content.split('\n')
        table_started = False
        horses = []
        for line in lines:
            line = line.strip()
            if line.lower().startswith("http"):
                continue
            if re.search(r'HN\s+Horse\s+PP', line, re.IGNORECASE):
                table_started = True
                continue
            if table_started and line:
                horse_info = parse_horse_line(line)
                if horse_info:
                    horses.append(horse_info)

        # 4) if we found horses, record this race
        if horses:
            races.append({
                "race_number": race_number,
                "horses":      horses,
                "date":        race_date
            })
                    
    return races

# ------------------------------
# Database Lookup (Combined for Both Databases)
# ------------------------------

def get_horse_rating(horse_name):
    """
    Fetch a horse's Mu and Sigma from the databases.
    Checks "pacers.db" first and then "trotters.db".
    Returns (mu, sigma) if found; otherwise returns default values.
    """
    for db in ["pacers", "trotters"]:
        conn = sqlite3.connect(f"{db}.db")
        cursor = conn.cursor()
        cursor.execute("SELECT mu, sigma FROM player_ratings WHERE player_name = ?", (horse_name,))
        result = cursor.fetchone()
        conn.close()
        if result:
            return result[0], result[1]
    # If not found, use default values:
    print(f"⚠️ Horse '{horse_name}' not found in either database. Using default Mu=1000, Sigma=333.")
    return 1000, 333

def get_driver_rating(driver_name):
    """
    Fetch a driver's Mu and Sigma from the databases.
    Checks "pacers.db" first and then "trotters.db".
    Returns (mu, sigma) if found; otherwise returns default values.
    """
    if not driver_name:
        return 1000, 333  # Default values if no driver specified
        
    for db in ["pacers", "trotters"]:
        conn = sqlite3.connect(f"{db}.db")
        cursor = conn.cursor()
        cursor.execute("SELECT mu, sigma FROM driver_ratings WHERE driver_name = ?", (driver_name,))
        result = cursor.fetchone()
        conn.close()
        if result:
            return result[0], result[1]
    # If not found, use default values:
    print(f"ℹ️ Driver '{driver_name}' not found in either database. Using default Mu=1000, Sigma=333.")
    return 1000, 333

def get_trainer_rating(trainer_name):
    """
    Fetch a trainer's Mu and Sigma from the databases.
    Checks "pacers.db" first and then "trotters.db".
    Returns (mu, sigma) if found; otherwise returns default values.
    """
    if not trainer_name:
        return 1000, 333  # Default values if no trainer specified
        
    for db in ["pacers", "trotters"]:
        conn = sqlite3.connect(f"{db}.db")
        cursor = conn.cursor()
        cursor.execute("SELECT mu, sigma FROM trainer_ratings WHERE trainer_name = ?", (trainer_name,))
        result = cursor.fetchone()
        conn.close()
        if result:
            return result[0], result[1]
    # If not found, use default values:
    print(f"ℹ️ Trainer '{trainer_name}' not found in either database. Using default Mu=1000, Sigma=333.")
    return 1000, 333

def calculate_combined_rating(horse_name, driver_name, trainer_name):
    """
    Calculate a combined rating using weighted average of horse, driver, and trainer ratings.
    """
    # Get individual ratings
    horse_mu, horse_sigma = get_horse_rating(horse_name)
    
    driver_mu, driver_sigma = 1000, 333
    if driver_name:
        driver_mu, driver_sigma = get_driver_rating(driver_name)
        
    trainer_mu, trainer_sigma = 1000, 333
    if trainer_name:
        trainer_mu, trainer_sigma = get_trainer_rating(trainer_name)
    
    # Calculate weights based on available data
    if driver_name and trainer_name:
        # Full data available
        horse_weight = 0.6
        driver_weight = 0.25
        trainer_weight = 0.15
    elif driver_name:
        # No trainer data
        horse_weight = 0.7
        driver_weight = 0.3
        trainer_weight = 0.0
    elif trainer_name:
        # No driver data
        horse_weight = 0.8
        driver_weight = 0.0
        trainer_weight = 0.2
    else:
        # Only horse data
        horse_weight = 1.0
        driver_weight = 0.0
        trainer_weight = 0.0
    
    # Calculate weighted average for mu and sigma
    combined_mu = (horse_mu * horse_weight + 
                   driver_mu * driver_weight + 
                   trainer_mu * trainer_weight)
                   
    combined_sigma = (horse_sigma * horse_weight + 
                      driver_sigma * driver_weight + 
                      trainer_sigma * trainer_weight)
                      
    return combined_mu, combined_sigma, horse_mu, horse_sigma, driver_mu, driver_sigma, trainer_mu, trainer_sigma

# ------------------------------
# Get Last 5 Races
# ------------------------------

def get_last_5_races(horse_name):
    """
    Enhanced function to get a horse's race history.
    - Returns up to 5 recent races if available
    - Handles single-race history (no delta calculation possible)
    - Provides a placeholder entry for horses with ratings but no history
    """
    history = []
    current_mu = None
    last_race_date = None
    
    # First check if the horse exists and get its current rating
    for db in ["pacers", "trotters"]:
        conn = sqlite3.connect(f"{db}.db")
        cursor = conn.cursor()
        cursor.execute("SELECT mu, last_played FROM player_ratings WHERE player_name = ?", (horse_name,))
        rating_result = cursor.fetchone()
        conn.close()
        
        if rating_result:
            current_mu = rating_result[0]
            last_race_date = rating_result[1] if rating_result[1] else None
            break  # Found the horse, no need to check other DB
    
    # If we didn't find the horse at all, return empty history
    if current_mu is None:
        return []
    
    # Now get race history if available
    for db in ["pacers", "trotters"]:
        conn = sqlite3.connect(f"{db}.db")
        cursor = conn.cursor()
        cursor.execute("""
            SELECT race_date, last_track, mu, finish_position, race_class
            FROM horse_history
            WHERE player_name = ?
            ORDER BY race_date DESC
            LIMIT 6
        """, (horse_name,))
        rows = cursor.fetchall()
        conn.close()
        
        if rows:
            # Case 1: Multiple rows - compute deltas between races
            if len(rows) > 1:
                for i in range(len(rows) - 1):
                    r_after = rows[i]       # current row (post-race mu)
                    r_before = rows[i + 1]  # prior row (pre-race mu)
                    
                    try:
                        race_date = datetime.datetime.strptime(r_after[0], "%Y-%m-%d %H:%M:%S")
                    except (ValueError, TypeError):
                        race_date = datetime.datetime.now()  # Fallback if date parsing fails
                        
                    mu_before = round(r_before[2]) if r_before[2] is not None else 0
                    mu_after = round(r_after[2]) if r_after[2] is not None else 0
                    delta_mu = mu_after - mu_before
                    
                    history.append({
                        "date": f"{race_date.month}/{race_date.day}",
                        "track": r_after[1] if r_after[1] else "-",
                        "finish": r_after[3] if r_after[3] else "-",
                        "mu": mu_before,
                        "delta": delta_mu,
                        "class": r_after[4] if r_after[4] else "-"
                    })
            
            # Case 2: Only one history record - can't calculate delta
            elif len(rows) == 1:
                r = rows[0]
                try:
                    race_date = datetime.datetime.strptime(r[0], "%Y-%m-%d %H:%M:%S")
                except (ValueError, TypeError):
                    race_date = datetime.datetime.now()
                    
                mu_value = round(r[2]) if r[2] is not None else 0
                
                history.append({
                    "date": f"{race_date.month}/{race_date.day}",
                    "track": r[1] if r[1] else "-",
                    "finish": r[3] if r[3] else "-",
                    "mu": mu_value,
                    "delta": 0,  # No delta available
                    "class": r[4] if r[4] else "-"
                })
            
            # We found something in this database, stop searching
            if history:
                break
    
    # Case 3: Horse exists in ratings but has no history
    if not history and current_mu is not None:
        # Try to parse the last race date if available
        race_date = None
        if last_race_date:
            try:
                race_date = datetime.datetime.strptime(last_race_date, "%Y-%m-%d %H:%M:%S")
            except (ValueError, TypeError):
                pass
                
        if race_date is None:
            race_date = datetime.datetime.now()
            
        history.append({
            "date": f"{race_date.month}/{race_date.day}",
            "track": "No prior races",
            "finish": "-",
            "mu": round(current_mu),
            "delta": 0,
            "class": "New Entry"
        })
    
    return history

# To use this function in the PDF generator, modify the display code like this:
def get_last_3_races_person(person_name, person_type):
    """
    Get the last 3 races for a driver or trainer.
    """
    if not person_name:
        return []
        
    history = []
    for db in ["pacers", "trotters"]:
        conn = sqlite3.connect(f"{db}.db")
        cursor = conn.cursor()
        cursor.execute(f"""
            SELECT race_date, last_track, mu, finish_position, race_class, horse_name
            FROM {person_type}_history
            WHERE {person_type}_name = ?
            ORDER BY race_date DESC
            LIMIT 4
        """, (person_name,))
        rows = cursor.fetchall()
        conn.close()

        if len(rows) > 1:
            for i in range(len(rows) - 1):
                r_after = rows[i]       # current row (post-race mu)
                r_before = rows[i + 1]  # prior row (pre-race mu)

                race_date = datetime.datetime.strptime(r_after[0], "%Y-%m-%d %H:%M:%S")
                mu_before = round(r_before[2])
                mu_after = round(r_after[2])
                delta_mu = mu_after - mu_before

                history.append({
                    "date": f"{race_date.month}/{race_date.day}",
                    "track": r_after[1] if r_after[1] else "-",
                    "finish": r_after[3] if r_after[3] else "-",
                    "mu": mu_before,
                    "delta": delta_mu,
                    "class": r_after[4] if r_after[4] else "-",
                    "horse": r_after[5] if r_after[5] else "-"
                })
            break
    return history[:3]  # Limit to 3 races

# ------------------------------
# Morning Line Odds Calculation
# ------------------------------

def calculate_win_probabilities(horses):
    """
    Given a list of horses (each a dict with horse_name, driver_name, and trainer_name keys),
    calculate the win probabilities and decimal odds for each horse using Log-Sum-Exp normalization.
    Now uses combined ratings from horse, driver, and trainer.
    """
    ratings = []
    for horse in horses:
        name = horse["horse_name"]
        driver_name = horse.get("driver_name")
        trainer_name = horse.get("trainer_name")
        
        combined_mu, combined_sigma, horse_mu, horse_sigma, driver_mu, driver_sigma, trainer_mu, trainer_sigma = calculate_combined_rating(name, driver_name, trainer_name)
        beta = 166.5  # Recommended TrueSkill distance parameter
        
        ratings.append((name, combined_mu, combined_sigma, beta, horse_mu, driver_mu, trainer_mu, driver_name, trainer_name))
    
    max_mu = max(r[1] for r in ratings)
    exp_values = [(math.exp((mu - max_mu) / beta), name) for name, mu, sigma, beta, _, _, _, _, _ in ratings]
    total_exp = sum(e for e, _ in exp_values)
    
    horse_odds = []
    for exp_val, name in exp_values:
        win_probability = exp_val / total_exp
        decimal_odds = 1 / win_probability if win_probability > 0 else float("inf")
        
        # Find the corresponding full data
        horse_data = next((data for data in ratings if data[0] == name), None)
        if horse_data:
            _, combined_mu, combined_sigma, _, horse_mu, driver_mu, trainer_mu, driver_name, trainer_name = horse_data
            horse_odds.append((name, win_probability, decimal_odds, combined_mu, horse_mu, driver_mu, trainer_mu, driver_name, trainer_name))
    
    horse_odds.sort(key=lambda x: x[1], reverse=True)
    return horse_odds

def display_odds_table(race_number, horse_odds):
    """Display the enhanced odds table with horse, driver, and trainer information."""
    print(f"\n🏇 **Pre-Race StrideScore Odds for Race {race_number}** 🏇")
    print(f"{'Rank':<5}{'Horse Name':<20}{'Driver':<15}{'Trainer':<15}{'Win %':<8}{'Odds':<8}{'Combined':<10}{'Horse':<8}{'Driver':<8}{'Trainer':<8}")
    print("=" * 100)
    
    for idx, (name, win_prob, decimal_odds, combined_mu, horse_mu, driver_mu, trainer_mu, driver_name, trainer_name) in enumerate(horse_odds, start=1):
        driver_display = driver_name if driver_name else "Unknown"
        trainer_display = trainer_name if trainer_name else "Unknown"
        
        print(f"{idx:<5}{name:<20}{driver_display:<15}{trainer_display:<15}{win_prob * 100:<8.2f}{decimal_odds:<8.2f}{combined_mu:<10.2f}{horse_mu:<8.2f}{driver_mu:<8.2f}{trainer_mu:<8.2f}")
        
        # Show history for horse
        history = get_last_5_races(name)
        for h in history:
            print(f"{'':<5}{h['date']}, {h['track']}, {h['class']}, Finish: {h['finish']}, Mu: {h['mu']} ({h['delta']:+})")
        
        # Show history for driver if available
        if driver_name:
            driver_history = get_last_3_races_person(driver_name, "driver")
            if driver_history:
                print(f"{'':<5}Driver {driver_name} recent results:")
                for h in driver_history:
                    print(f"{'':<8}{h['date']}, {h['track']}, Horse: {h['horse']}, Finish: {h['finish']}, Mu: {h['mu']} ({h['delta']:+})")
        
        # Show history for trainer if available
        if trainer_name:
            trainer_history = get_last_3_races_person(trainer_name, "trainer")
            if trainer_history:
                print(f"{'':<5}Trainer {trainer_name} recent results:")
                for h in trainer_history:
                    print(f"{'':<8}{h['date']}, {h['track']}, Horse: {h['horse']}, Finish: {h['finish']}, Mu: {h['mu']} ({h['delta']:+})")
        
        print("-" * 100)  # Separator between horses

# ------------------------------
# PDF Generation Function
# ------------------------------

class PDF(FPDF):
    def __init__(self):
        super().__init__()
        self.race_date = None  # Will be set before adding the first page

    def header(self):
        # Format the race date with ordinal suffix
        if self.race_date:
            day = self.race_date.day
            suffix = "th" if 11 <= day <= 13 else {1: "st", 2: "nd", 3: "rd"}.get(day % 10, "th")
            formatted_date = self.race_date.strftime(f"%B {day}{suffix} %Y")
        else:
            formatted_date = "Unknown Date"

        self.set_font('Arial', 'B', 14)
        self.cell(0, 10, f'Stride Score Index: {formatted_date}', 0, 1, 'C')
        self.ln(5)

    def footer(self):
        self.set_y(-15)
        self.set_font('Arial', 'I', 8)
        self.cell(0, 10, f'Page {self.page_no()}', 0, 0, 'C')

import datetime

def generate_pdf(races, pdf_filename):
    """
    Creates one page per race, using each race's own date in the header.
    With improved handling of horse history display.
    """
    pdf = PDF()

    for race in races:
        # 1) set the header date for this race (falls back to now())
        pdf.race_date = race.get("date") 
        pdf.add_page()
        pdf.set_font('Arial', '', 12)

        race_number = race["race_number"]
        horses      = race["horses"]
        if not horses:
            continue

        # 2) compute odds
        horse_odds = calculate_win_probabilities(horses)

        # 3) print the race title
        pdf.set_font('Arial', 'B', 12)
        pdf.cell(0, 10, f'Race {race_number}', 0, 1)

        # 4) print the header row
        pdf.set_font('Arial', 'B', 8)
        pdf.cell(8, 8, "Rank",       1)
        pdf.cell(30, 8, "Horse Name", 1)
        pdf.cell(20, 8, "Driver",     1)
        pdf.cell(20, 8, "Trainer",    1)
        pdf.cell(15, 8, "Win %",      1)
        pdf.cell(15, 8, "Odds",       1)
        pdf.cell(15, 8, "Combined",   1)
        pdf.cell(15, 8, "Horse",      1)
        pdf.cell(15, 8, "Driver",     1)
        pdf.cell(15, 8, "Trainer",    1)
        pdf.ln(8)

        # 5) print each horse row + its history
        pdf.set_font('Arial', '', 8)
        for idx, (name, win_prob, decimal_odds,
                  combined_mu, horse_mu,
                  driver_mu, trainer_mu,
                  driver_name, trainer_name) in enumerate(horse_odds, start=1):

            driver_display  = driver_name  or "Unknown"
            trainer_display = trainer_name or "Unknown"

            pdf.cell(8,  8, str(idx),                       1)
            pdf.cell(30, 8, name,                          1)
            pdf.cell(20, 8, driver_display,                1)
            pdf.cell(20, 8, trainer_display,               1)
            pdf.cell(15, 8, f"{win_prob*100:.2f}",         1)
            pdf.cell(15, 8, f"{decimal_odds:.2f}",         1)
            pdf.cell(15, 8, f"{combined_mu:.2f}",          1)
            pdf.cell(15, 8, f"{horse_mu:.2f}",             1)
            pdf.cell(15, 8, f"{driver_mu:.2f}",            1)
            pdf.cell(15, 8, f"{trainer_mu:.2f}",           1)
            pdf.ln(8)

            # 6) print race history with improved handling
            history = get_last_5_races(name)
            if history:
                for h in history:
                    pdf.cell(8,  6, "", 0)
                    pdf.cell(
                        0, 6,
                        f"{h['date']} | {h['track']} | {h['class']} | "
                        f"Finish: {h['finish']} | Mu: {h['mu']} ({h['delta']:+})",
                        ln=True
                    )
            else:
                # Display a message when no race history is found
                pdf.cell(8, 6, "", 0)
                pdf.cell(0, 6, "No race history found for this horse", ln=True)
                
            pdf.ln(5)

    # 7) write out the PDF
    pdf.output(pdf_filename)
    print(f"PDF generated and saved as {pdf_filename}")


# ------------------------------
# Email Sending Function
# ------------------------------

def send_email(pdf_filename, recipient_email):
    """
    Sends an email with the given PDF file attached.
    Update the SMTP settings and sender details before use.
    """
    # Email configuration - update these with your actual details.
    smtp_server = "smtp.gmail.com"  # e.g., smtp.gmail.com
    smtp_port = 587  # Typically 587 for TLS
    smtp_username = "abetts00.com"
    smtp_password = "yhvu wpiz gfmf tajl"
    sender_email = smtp_username  # or another sender email if different
    subject = "Pre-Race StrideScore Odds PDF"
    body = "Please find attached the PDF containing the race odds."

    # Create the email message
    msg = EmailMessage()
    msg['Subject'] = "Stride Score PDF"
    msg['From'] = "abetts00@gmail.com"
    msg['To'] = "Kplowch@gmail.com"
    msg.set_content(body)
    
    # Read the PDF file and attach it
    with open(pdf_filename, 'rb') as f:
        file_data = f.read()
        file_name = pdf_filename
    msg.add_attachment(file_data, maintype='application', subtype='pdf', filename=file_name)
    
    # Send the email via SMTP
    try:
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls()  # Secure the connection
            server.login(smtp_username, smtp_password)
            server.send_message(msg)
        print(f"Email sent successfully to {recipient_email}.")
    except Exception as e:
        print(f"Error sending email: {e}")

# ------------------------------
# Main Execution
# ------------------------------

if __name__ == "__main__":
    # Read race data from file
    file_path = "ml.txt"
    with open(file_path, "r", encoding="utf-8") as f:
        text = f.read()
    
    races = parse_races_from_text(text)
    if not races:
        print("No races found in the file.")
        exit()
    
    # Process races and display odds on console
    for race in races:
        race_number = race.get("race_number")
        horses = race.get("horses", [])
        if not horses:
            continue
        print(f"\nProcessing Race {race_number}...")
        horse_odds = calculate_win_probabilities(horses)
        display_odds_table(race_number, horse_odds)
    
    # Generate PDF with odds tables
    script_dir   = os.path.dirname(os.path.abspath(__file__))
    pdf_filename = os.path.join(script_dir, "race_odds.pdf")
    generate_pdf(races, pdf_filename)

