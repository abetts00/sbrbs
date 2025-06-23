import re
import sqlite3
import datetime
import math
import logging
from typing import Optional, Tuple, Dict, Any, List
import trueskill
import argparse


# -------------------------------------------------------------------------------------
# Configuration and Global Constants
# -------------------------------------------------------------------------------------
DEFAULT_MU = 1000
DEFAULT_SIGMA = 333.333
MAX_DECAY = 0.50
MIN_DAYS_NO_DECAY = 28
MAX_DAYS_DECAY = 365

# Weights for combined rating calculation
HORSE_WEIGHT = 0.8
DRIVER_WEIGHT = 0.1
TRAINER_WEIGHT = 0.1

# Flag for dry run (set by command line arg)
DRY_RUN = False

# Precompiled regex patterns for performance
RE_DT_FORMATS = [
    "%Y-%m-%d %H:%M:%S.%f",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d %H:%M"
]
RE_ODDS = re.compile(r'^\*?\d+\.\d{2}$')
RE_FINISH_DIGITS = re.compile(r'^(\d+)')
RE_HEADER = re.compile(r'HN\s+(Horse|horsa)\s+PP', re.IGNORECASE)


# -------------------------------------------------------------------------------------
# Logging Configuration
# -------------------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


# -------------------------------------------------------------------------------------
# Database Functions Module
# -------------------------------------------------------------------------------------
def init_db(db_name: str) -> None:
    """
    Create a database and tables if they do not exist.
    Now includes tables for drivers and trainers alongside horses.
    """
    with sqlite3.connect(f'{db_name}.db') as conn:
        cursor = conn.cursor()
        
        # Original horse tables
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS player_ratings (
                player_name TEXT PRIMARY KEY,
                mu REAL,
                sigma REAL,
                last_played DATETIME,
                last_track TEXT
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS horse_history (
                player_name TEXT,
                mu REAL,
                sigma REAL,
                race_date DATETIME,
                last_track TEXT,
                finish_position TEXT,
                race_class TEXT,
                FOREIGN KEY(player_name) REFERENCES player_ratings(player_name)
            )
        ''')
        
        # New tables for drivers
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS driver_ratings (
                driver_name TEXT PRIMARY KEY,
                mu REAL,
                sigma REAL,
                last_raced DATETIME,
                last_track TEXT
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS driver_history (
                driver_name TEXT,
                mu REAL,
                sigma REAL,
                race_date DATETIME,
                last_track TEXT,
                horse_name TEXT,
                finish_position TEXT,
                race_class TEXT,
                FOREIGN KEY(driver_name) REFERENCES driver_ratings(driver_name)
            )
        ''')
        
        # New tables for trainers
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS trainer_ratings (
                trainer_name TEXT PRIMARY KEY,
                mu REAL,
                sigma REAL,
                last_raced DATETIME,
                last_track TEXT
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS trainer_history (
                trainer_name TEXT,
                mu REAL,
                sigma REAL,
                race_date DATETIME,
                last_track TEXT,
                horse_name TEXT,
                finish_position TEXT,
                race_class TEXT,
                FOREIGN KEY(trainer_name) REFERENCES trainer_ratings(trainer_name)
            )
        ''')
        
        # Table to store complete race entry information
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS race_entries (
                race_date DATETIME,
                track TEXT,
                race_number INTEGER,
                horse_name TEXT,
                driver_name TEXT,
                trainer_name TEXT,
                finish_position TEXT,
                race_class TEXT,
                gait TEXT,
                is_qualifier BOOLEAN,
                PRIMARY KEY(race_date, track, race_number, horse_name)
            )
        ''')
        
        conn.commit()
    
    # Ensure columns exist after potential creation
    add_missing_columns(db_name)
    add_indexes(db_name)


def add_missing_columns(db_name: str) -> None:
    """
    Ensure that all tables have the required columns.
    """
    with sqlite3.connect(f"{db_name}.db") as conn:
        cursor = conn.cursor()

        # Check player_ratings
        cursor.execute("PRAGMA table_info(player_ratings)")
        columns = [row[1] for row in cursor.fetchall()]
        if "last_track" not in columns:
            logging.info(f"Adding last_track column to player_ratings in {db_name}.db")
            cursor.execute("ALTER TABLE player_ratings ADD COLUMN last_track TEXT")

        # Check horse_history
        cursor.execute("PRAGMA table_info(horse_history)")
        columns = [row[1] for row in cursor.fetchall()]
        if "last_track" not in columns:
            logging.info(f"Adding last_track column to horse_history in {db_name}.db")
            cursor.execute("ALTER TABLE horse_history ADD COLUMN last_track TEXT")
        
        if "finish_position" not in columns:
            logging.info(f"Adding finish_position column to horse_history in {db_name}.db")
            cursor.execute("ALTER TABLE horse_history ADD COLUMN finish_position TEXT")

        if "race_class" not in columns:
            logging.info(f"Adding race_class column to horse_history in {db_name}.db")
            cursor.execute("ALTER TABLE horse_history ADD COLUMN race_class TEXT")

        conn.commit()


def add_indexes(db_name: str) -> None:
    """Add indexes to improve query performance."""
    with sqlite3.connect(f'{db_name}.db') as conn:
        cursor = conn.cursor()
        
        # Add indexes for player/horse lookups
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_player_name ON player_ratings (player_name)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_horse_history_name ON horse_history (player_name)')
        
        # Add indexes for driver lookups
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_driver_name ON driver_ratings (driver_name)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_driver_history_name ON driver_history (driver_name)')
        
        # Add indexes for trainer lookups
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_trainer_name ON trainer_ratings (trainer_name)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_trainer_history_name ON trainer_history (trainer_name)')
        
        # Add indexes for race entries
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_race_entries_horse ON race_entries (horse_name)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_race_entries_driver ON race_entries (driver_name)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_race_entries_trainer ON race_entries (trainer_name)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_race_entries_date ON race_entries (race_date)')
        
        conn.commit()


def add_horse(db_name: str, player_name: str, race_date: Optional[datetime.datetime] = None, race_track: Optional[str] = None) -> None:
    """
    Add a new horse with default Mu, Sigma, and set last_played (date) and last_track (track name).
    """
    if DRY_RUN:
        logging.info(f"DRY RUN: Would add horse '{player_name}' to {db_name}.db")
        return
        
    race_date_val = race_date if race_date else datetime.datetime.now()
    with sqlite3.connect(f'{db_name}.db') as conn:
        cursor = conn.cursor()
        cursor.execute('''
            INSERT OR IGNORE INTO player_ratings (player_name, mu, sigma, last_played, last_track)
            VALUES (?, ?, ?, ?, ?)
        ''', (player_name, DEFAULT_MU, DEFAULT_SIGMA, race_date_val, race_track))
        conn.commit()


def add_person(db_name: str, person_name: str, person_type: str, race_date: Optional[datetime.datetime] = None, race_track: Optional[str] = None) -> None:
    """
    Add a new driver or trainer with default Mu, Sigma.
    person_type should be either "driver" or "trainer"
    """
    if DRY_RUN:
        logging.info(f"DRY RUN: Would add {person_type} '{person_name}' to {db_name}.db")
        return
        
    table_name = f"{person_type}_ratings"
    date_field = "last_raced"
    name_field = f"{person_type}_name"
    
    race_date_val = race_date if race_date else datetime.datetime.now()
    with sqlite3.connect(f'{db_name}.db') as conn:
        cursor = conn.cursor()
        query = f'''
            INSERT OR IGNORE INTO {table_name} ({name_field}, mu, sigma, {date_field}, last_track)
            VALUES (?, ?, ?, ?, ?)
        '''
        cursor.execute(query, (person_name, DEFAULT_MU, DEFAULT_SIGMA, race_date_val, race_track))
        conn.commit()


def get_player_rating(db_name: str, player_name: str, race_date: Optional[datetime.datetime] = None
                     ) -> Tuple[Optional[trueskill.Rating], Optional[str]]:
    """
    Fetch a horse's current rating and last_played date from the DB.
    Applies log-based decay if a last_played timestamp exists.
    """
    with sqlite3.connect(f'{db_name}.db') as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT mu, sigma, last_played FROM player_ratings WHERE player_name=?', (player_name,))
        result = cursor.fetchone()

    if result:
        mu, sigma, last_played_str = result
        rating = trueskill.Rating(mu=mu, sigma=sigma)
        if last_played_str:
            last_played_dt = None
            for fmt in RE_DT_FORMATS:
                try:
                    last_played_dt = datetime.datetime.strptime(last_played_str, fmt)
                    break
                except ValueError:
                     continue
            if last_played_dt is None:
                logging.error(f"Time data {last_played_str!r} for player {player_name} does not match any expected format. Skipping decay.")
                return rating, last_played_str

            current_dt = race_date if race_date else datetime.datetime.now()
            days_since_last = (current_dt - last_played_dt).days
            decayed_mu = calculate_rating_decay(rating.mu, days_since_last)
            rating = trueskill.Rating(mu=decayed_mu, sigma=rating.sigma)
        return rating, last_played_str
    else:
        return None, None


def get_person_rating(db_name: str, person_name: str, person_type: str, race_date: Optional[datetime.datetime] = None
                     ) -> Optional[trueskill.Rating]:
    """
    Fetch a driver's or trainer's rating and apply decay.
    person_type should be either "driver" or "trainer"
    """
    if not person_name:
        return None
        
    table_name = f"{person_type}_ratings"
    date_field = "last_raced"
    name_field = f"{person_type}_name"
    
    with sqlite3.connect(f'{db_name}.db') as conn:
        cursor = conn.cursor()
        query = f'SELECT mu, sigma, {date_field} FROM {table_name} WHERE {name_field}=?'
        cursor.execute(query, (person_name,))
        result = cursor.fetchone()
    
    if result:
        mu, sigma, last_played_str = result
        rating = trueskill.Rating(mu=mu, sigma=sigma)
        
        # Apply decay if we have a last played date
        if last_played_str:
            last_played_dt = None
            for fmt in RE_DT_FORMATS:
                try:
                    last_played_dt = datetime.datetime.strptime(last_played_str, fmt)
                    break
                except ValueError:
                     continue
            
            if last_played_dt is None:
                logging.error(f"Time data {last_played_str!r} for {person_type} {person_name} does not match any expected format. Skipping decay.")
                return rating
                
            current_dt = race_date if race_date else datetime.datetime.now()
            days_since_last = (current_dt - last_played_dt).days
            decayed_mu = calculate_rating_decay(rating.mu, days_since_last)
            rating = trueskill.Rating(mu=decayed_mu, sigma=rating.sigma)
            
        return rating
    else:
        return None


def update_player_rating(db_name: str, player_name: str, new_rating: trueskill.Rating,
                         race_date: Optional[datetime.datetime] = None, race_track: Optional[str] = None) -> None:
    """
    Store the updated rating (Mu, Sigma), last_played (date), and last_track (track name)
    in the player_ratings table.
    """
    if DRY_RUN:
        logging.info(f"DRY RUN: Would update horse '{player_name}' in {db_name}.db to mu={new_rating.mu:.2f}, sigma={new_rating.sigma:.2f}")
        return
        
    last_played_date = race_date if race_date else datetime.datetime.now()
    with sqlite3.connect(f'{db_name}.db') as conn:
        cursor = conn.cursor()
        cursor.execute('''
            UPDATE player_ratings
            SET mu = ?, sigma = ?, last_played = ?, last_track = ?
            WHERE player_name = ?
        ''', (new_rating.mu, new_rating.sigma, last_played_date, race_track, player_name))
        conn.commit()


def update_person_rating(db_name: str, person_name: str, person_type: str, new_rating: trueskill.Rating,
                        race_date: Optional[datetime.datetime] = None, race_track: Optional[str] = None) -> None:
    """
    Store the updated rating for a driver or trainer.
    person_type should be either "driver" or "trainer"
    """
    if DRY_RUN:
        logging.info(f"DRY RUN: Would update {person_type} '{person_name}' in {db_name}.db to mu={new_rating.mu:.2f}, sigma={new_rating.sigma:.2f}")
        return
        
    table_name = f"{person_type}_ratings"
    date_field = "last_raced"
    name_field = f"{person_type}_name"
    
    race_date_val = race_date if race_date else datetime.datetime.now()
    with sqlite3.connect(f'{db_name}.db') as conn:
        cursor = conn.cursor()
        query = f'''
            UPDATE {table_name}
            SET mu = ?, sigma = ?, {date_field} = ?, last_track = ?
            WHERE {name_field} = ?
        '''
        cursor.execute(query, (new_rating.mu, new_rating.sigma, race_date_val, race_track, person_name))
        conn.commit()


def log_horse_race(db_name: str, player_name: str, mu: float, sigma: float,
                   race_date: Optional[datetime.datetime] = None,
                   race_track: Optional[str] = None,
                   finish_position: Optional[str] = None,
                   race_class: Optional[str] = None) -> None:
    """Log a horse's race result to the history table."""
    if DRY_RUN:
        return
        
    race_date_val = race_date if race_date else datetime.datetime.now()
    with sqlite3.connect(f'{db_name}.db') as conn:
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO horse_history (player_name, mu, sigma, race_date, last_track, finish_position, race_class)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (player_name, mu, sigma, race_date_val, race_track, finish_position, race_class))
        conn.commit()


def log_person_race(db_name: str, person_name: str, person_type: str, mu: float, sigma: float,
                   race_date: Optional[datetime.datetime] = None,
                   race_track: Optional[str] = None,
                   horse_name: Optional[str] = None,
                   finish_position: Optional[str] = None,
                   race_class: Optional[str] = None) -> None:
    """
    Log a driver's or trainer's race result to the history table.
    person_type should be either "driver" or "trainer"
    """
    if DRY_RUN:
        return
        
    table_name = f"{person_type}_history"
    name_field = f"{person_type}_name"
    
    race_date_val = race_date if race_date else datetime.datetime.now()
    with sqlite3.connect(f'{db_name}.db') as conn:
        cursor = conn.cursor()
        query = f'''
            INSERT INTO {table_name} 
            ({name_field}, mu, sigma, race_date, last_track, horse_name, finish_position, race_class)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        '''
        cursor.execute(query, (person_name, mu, sigma, race_date_val, race_track, 
                               horse_name, finish_position, race_class))
        conn.commit()


def store_race_entry(db_name: str, race_date: datetime.datetime, track: str, race_number: int,
                    horse_name: str, driver_name: Optional[str], trainer_name: Optional[str],
                    finish_position: Optional[str], race_class: Optional[str],
                    gait: str, is_qualifier: bool) -> None:
    """
    Store complete race entry information.
    """
    if DRY_RUN:
        return
        
    with sqlite3.connect(f'{db_name}.db') as conn:
        cursor = conn.cursor()
        cursor.execute('''
            INSERT OR REPLACE INTO race_entries
            (race_date, track, race_number, horse_name, driver_name, trainer_name, 
             finish_position, race_class, gait, is_qualifier)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (race_date, track, race_number, horse_name, driver_name, trainer_name, 
              finish_position, race_class, gait, is_qualifier))
        conn.commit()


def calculate_rating_decay(mu: float, days_since_last_played: int) -> float:
     """
     Apply a log-based decay to a rating value.
     - No decay if <= MIN_DAYS_NO_DECAY days.
     - Gradually decays up to MAX_DECAY at MAX_DAYS_DECAY days.
     """
     if days_since_last_played <= MIN_DAYS_NO_DECAY:
         return mu
     # Cap decay days at MAX_DAYS_DECAY
     if days_since_last_played > MAX_DAYS_DECAY:
         days_since_last_played = MAX_DAYS_DECAY

     # Ensure x is at least 1 for log calculation
     x = max(1, (days_since_last_played - MIN_DAYS_NO_DECAY) + 1)
     max_x = (MAX_DAYS_DECAY - MIN_DAYS_NO_DECAY) + 1
     # Basic check to prevent division by zero or log(1) issues if MAX_DAYS_DECAY == MIN_DAYS_NO_DECAY
     if max_x <= 1:
        return mu # Or apply full decay, depending on desired logic for this edge case

     ratio = math.log(x) / math.log(max_x)
     decayed_mu = mu * (1 - ratio * MAX_DECAY)
     return decayed_mu


def fetch_and_decay_rating(db_name: str, player_name: str,
                           race_date: Optional[datetime.datetime] = None,
                           race_track: Optional[str] = None) -> trueskill.Rating:
    """
    Retrieve a horse's rating, apply decay if needed, and return the decayed rating.
    If the horse doesn't exist, add it with a default rating, including the track.
    """
    rating, _ = get_player_rating(db_name, player_name, race_date)
    if rating is None:
        add_horse(db_name, player_name, race_date, race_track)
        rating = trueskill.Rating(DEFAULT_MU, DEFAULT_SIGMA)
    return rating


def fetch_and_decay_person_rating(db_name: str, person_name: str, person_type: str,
                                 race_date: Optional[datetime.datetime] = None,
                                 race_track: Optional[str] = None) -> trueskill.Rating:
    """
    Retrieve a driver or trainer rating, apply decay if needed.
    If the person doesn't exist, add them with a default rating.
    person_type should be either "driver" or "trainer"
    """
    if not person_name:
        return trueskill.Rating(DEFAULT_MU, DEFAULT_SIGMA)
        
    rating = get_person_rating(db_name, person_name, person_type, race_date)
    if rating is None:
        add_person(db_name, person_name, person_type, race_date, race_track)
        rating = trueskill.Rating(DEFAULT_MU, DEFAULT_SIGMA)
    return rating


def calculate_adaptive_weights(has_driver: bool, has_trainer: bool) -> Dict[str, float]:
    """
    Calculate adaptive weights for horse, driver, and trainer
    based on what data is available.
    """
    if has_driver and has_trainer:
        # Complete data
        return {
            "horse": HORSE_WEIGHT,
            "driver": DRIVER_WEIGHT,
            "trainer": TRAINER_WEIGHT
        }
    elif has_driver and not has_trainer:
        # No trainer data
        return {
            "horse": 0.7,
            "driver": 0.3,
            "trainer": 0.0
        }
    elif has_trainer and not has_driver:
        # No driver data
        return {
            "horse": 0.8,
            "driver": 0.0,
            "trainer": 0.2
        }
    else:
        # Only horse data
        return {
            "horse": 1.0,
            "driver": 0.0,
            "trainer": 0.0
        }


def get_combined_rating(db_name: str, horse_name: str, driver_name: Optional[str], trainer_name: Optional[str], 
                        race_date: Optional[datetime.datetime] = None) -> trueskill.Rating:
    """
    Calculate a combined rating based on horse, driver, and trainer ratings.
    Applies weighted average to mu and sigma values.
    """
    # Get individual ratings
    horse_rating, _ = get_player_rating(db_name, horse_name, race_date)
    if horse_rating is None:
        horse_rating = trueskill.Rating(DEFAULT_MU, DEFAULT_SIGMA)
    
    driver_rating = None
    if driver_name:
        driver_rating = get_person_rating(db_name, driver_name, "driver", race_date)
    if driver_rating is None:
        driver_rating = trueskill.Rating(DEFAULT_MU, DEFAULT_SIGMA)
    
    trainer_rating = None
    if trainer_name:
        trainer_rating = get_person_rating(db_name, trainer_name, "trainer", race_date)
    if trainer_rating is None:
        trainer_rating = trueskill.Rating(DEFAULT_MU, DEFAULT_SIGMA)
    
    # Get adaptive weights based on available data
    weights = calculate_adaptive_weights(driver_name is not None, trainer_name is not None)
    
    # Calculate weighted mu and sigma
    combined_mu = (horse_rating.mu * weights["horse"] + 
                   driver_rating.mu * weights["driver"] + 
                   trainer_rating.mu * weights["trainer"])
    
    # For sigma, we use a weighted average of the sigmas
    combined_sigma = (horse_rating.sigma * weights["horse"] + 
                      driver_rating.sigma * weights["driver"] + 
                      trainer_rating.sigma * weights["trainer"])
    
    return trueskill.Rating(mu=combined_mu, sigma=combined_sigma)


def get_competitors_in_race(db_name: str, race_date: datetime.datetime, race_track: str, 
                           race_number: int, person_type: str) -> List[Tuple[str, trueskill.Rating]]:
    """
    Get all drivers or trainers competing in a specific race.
    Returns a list of (name, rating) tuples.
    """
    with sqlite3.connect(f'{db_name}.db') as conn:
        cursor = conn.cursor()
        query = f'''
            SELECT {person_type}_name 
            FROM race_entries
            WHERE race_date = ? AND track = ? AND race_number = ? AND {person_type}_name IS NOT NULL
        '''
        cursor.execute(query, (race_date, race_track, race_number))
        person_names = [row[0] for row in cursor.fetchall()]
    
    # Get rating for each person
    competitors = []
    for name in person_names:
        rating = fetch_and_decay_person_rating(db_name, name, person_type, race_date, race_track)
        competitors.append((name, rating))
    
    return competitors


# -------------------------------------------------------------------------------------
# Parsing Functions Module
# -------------------------------------------------------------------------------------
def parse_finish(token: str) -> Optional[int]:
    """Extract the numeric finish position from a finish token."""
    if not token: return None
    if "/" in token:
        parts = token.split("/")
        for part in parts:
            num_str = re.sub(r'\D', '', part)
            if num_str:
                try: return int(num_str)
                except ValueError: continue
        return None
    else:
        try: return int(re.sub(r'\D', '', token))
        except ValueError: return None


def parse_horse_line(tokens: List[str]) -> Optional[Dict[str, Any]]:
    """
    Parse a line of horse data including driver and trainer info with improved name handling.
    Better handles names with prefixes (Mc, Mac, Van, etc.) and suffixes (Jr, Sr, etc.).
    """
    if not tokens:
        return None

    # Extract basic horse info (same as before)
    hn = tokens[0]
    allowed_signifiers = {"A", "N", "F", "S","B","T", "C", "D", "E", "G", "H", "I", "J", "K", "L", "M", "O", "P", "Q", "R", "U", "V", "W", "Y", "Z"}
    name_tokens = []
    i = 1
    while i < len(tokens) and not (tokens[i].isdigit() or tokens[i].upper().startswith("SCR")):
        # Skip break indicators like "x4", "x5x", etc.
        if re.fullmatch(r'[xX]\d{1,2}[xX]?', tokens[i]):
            i += 1
            continue
        token_clean = re.sub(r'[^A-Za-z]', '', tokens[i])
        if (len(token_clean) > 1 and token_clean.isalpha()) or (len(token_clean) == 1 and token_clean.upper() in allowed_signifiers):
            name_tokens.append(tokens[i])
        i += 1
    horse_name = " ".join(name_tokens).lower() if name_tokens else None

    if i >= len(tokens) or not horse_name:
        return None
        
    pp = tokens[i]
    pp_clean = re.sub(r'[xX]', '', pp).strip(".,;:-")
    is_scratched = (pp_clean.upper() == "SCR")
    i += 1

    # Process finish position and odds (same as before)
    odds_index = None
    odds = None
    for idx, token in enumerate(tokens):
        if re.match(r'^\*?\d+\.\d{2}$', token):
            odds_index = idx
            odds = token
            break

    # ... rest of finish position code (unchanged) ...
    finish_candidate = None
    if odds_index is not None:
        start_idx = max(0, odds_index - 6)
        window = tokens[start_idx:odds_index]
        for j in range(len(window) - 1, 0, -1):
            if "/" in window[j]:
                finish_candidate = window[j - 1]
                break

    if finish_candidate is None and odds_index is not None:
        window = tokens[max(0, odds_index - 6):odds_index]
        for token in reversed(window):
            token_clean = re.sub(r'[xX]', '', token).strip(".,;:-")
            if token_clean.isdigit():
                finish_candidate = token_clean
                break

    if odds_index is None and i + 4 < len(tokens):
        finish_candidate = tokens[i+4]
        i += 5

    if finish_candidate is not None:
        finish_candidate = re.sub(r'[xX]', '', finish_candidate).strip(".,;:-")

    if finish_candidate is None:
        finish = None
    elif finish_candidate.upper() == "DNF":
        finish = "DNF"
    elif "/" in finish_candidate:
        m = re.match(r'^(\d+)', finish_candidate)
        finish = int(m.group(1)) if m else None
    else:
        m = re.match(r'^(\d+)', finish_candidate)
        finish = int(m.group(1)) if m else None

    if is_scratched:
        finish = None

    # Improved driver and trainer name extraction
    driver_name = None
    trainer_name = None
    
    # Known name prefixes and suffixes
    name_prefixes = {"mc", "mac", "o'", "de", "van", "von", "la", "le", "st", "ter", "di", "del"}
    name_suffixes = {"jr", "jr.", "sr", "sr.", "ii", "iii", "iv"}
    
    # Find earnings token (typically starts with $)
    earnings_idx = next(
        (i for i, t in enumerate(tokens) if t.startswith('$') and re.match(r'^\$\d', t)),
        None
    )
    
    # Better name detection function
    def is_name_like(token):
        # Names should have letters, not be purely numeric or money values
        return re.search(r'[a-zA-Z]{2}', token) and not token.startswith('$') and not re.match(r'^\d+$', token)
    
    # Extended name extraction with smarter handling of prefixes/suffixes
    def extract_full_name(start_idx, max_tokens=4):  # Increased max_tokens to handle longer names
        if start_idx >= len(tokens):
            return None, 0
            
        name_parts = []
        curr_idx = start_idx
        token_count = 0
        
        # Get first part of the name
        if curr_idx < len(tokens) and is_name_like(tokens[curr_idx]):
            name_parts.append(tokens[curr_idx])
            curr_idx += 1
            token_count += 1
        else:
            return None, 0
            
        # Handle last name, potentially with prefix
        if curr_idx < len(tokens) and is_name_like(tokens[curr_idx]):
            # Check if this might be a name prefix (Mc, Mac, etc.)
            prefix_detected = False
            if tokens[curr_idx-1].lower().rstrip('.') in name_prefixes or tokens[curr_idx-1].lower().endswith('mc'):
                if curr_idx + 1 < len(tokens) and is_name_like(tokens[curr_idx + 1]):
                    # We likely have a situation like "Ed Mc Neight Jr" - add both parts
                    name_parts.append(tokens[curr_idx])  # Add "Neight"
                    curr_idx += 1
                    token_count += 1
                    prefix_detected = True
            
            if not prefix_detected:
                name_parts.append(tokens[curr_idx])  # Add last name
                curr_idx += 1
                token_count += 1
                
        # Check for a suffix (Jr, Sr, etc.)
        if curr_idx < len(tokens) and token_count < max_tokens:
            if tokens[curr_idx].lower().rstrip('.') in name_suffixes:
                name_parts.append(tokens[curr_idx])  # Add suffix
                curr_idx += 1
                token_count += 1
                
        return " ".join(name_parts).lower() if name_parts else None, token_count
    
    # Look between earnings and odds (most common pattern)
    if earnings_idx is not None and odds_index is not None and odds_index > earnings_idx + 1:
        # We have a pattern like: $earnings driver_name trainer_name odds
        curr_idx = earnings_idx + 1
        
        # Try to extract driver name
        driver_name, tokens_used = extract_full_name(curr_idx)
        curr_idx += tokens_used
        
        # Try to extract trainer name if we have more tokens
        if curr_idx < odds_index:
            trainer_name, _ = extract_full_name(curr_idx)
    
    # If we still don't have names, try explicit markers
    if not driver_name or not trainer_name:
        for idx, token in enumerate(tokens):
            if idx < len(tokens) - 1:
                if token.lower() in ["dr.", "dr:", "driver:", "driver"]:
                    driver_name, _ = extract_full_name(idx + 1)
                elif token.lower() in ["tr.", "tr:", "trainer:", "trainer"]:
                    trainer_name, _ = extract_full_name(idx + 1)
    
    # One last attempt - look after odds
    if (not driver_name or not trainer_name) and odds_index is not None and odds_index + 1 < len(tokens):
        if not driver_name:
            driver_name, tokens_used = extract_full_name(odds_index + 1)
            if not trainer_name and driver_name and odds_index + 1 + tokens_used < len(tokens):
                trainer_name, _ = extract_full_name(odds_index + 1 + tokens_used)
    
    # Special case handling for common name patterns
    # This is where we can add specific handling for cases like "Ed Mc Neight Jr"
    if driver_name and not trainer_name and driver_name.startswith("mc "):
        # Handle "Mc" prefix specifically if it's at the start
        parts = driver_name.split()
        if len(parts) >= 3:  # We have something like "mc neight jr"
            driver_name = " ".join(parts[:2])  # "mc neight"
            trainer_name = " ".join(parts[2:])  # "jr" or whatever follows
    
    # Validate names - ensure they contain alphabetic characters
    if driver_name and not re.search(r'[a-z]', driver_name):
        driver_name = None
        
    if trainer_name and not re.search(r'[a-z]', trainer_name):
        trainer_name = None
        
    # Additional check for "Mc" names
    if driver_name and "mc " in driver_name and len(driver_name.split()) >= 2:
        # Let's make sure we're not cutting off part of a "Mc..." name
        name_parts = driver_name.split()
        if len(name_parts) >= 2 and name_parts[0].lower() in ["mc", "mac"]:
            # We need to examine what's after this position to see if part of name got cut off
            # This is complex logic that would need custom handling
            pass
    
    # Same check for trainer names
    if trainer_name and "mc " in trainer_name and len(trainer_name.split()) >= 2:
        name_parts = trainer_name.split()
        if len(name_parts) >= 2 and name_parts[0].lower() in ["mc", "mac"]:
            pass
    
    return {
        "hn": hn,
        "horse_name": horse_name,
        "pp": pp_clean,
        "is_scratched": is_scratched,
        "finish": finish,
        "odds": odds,
        "driver_name": driver_name,
        "trainer_name": trainer_name
    }
    
def parse_races_from_text(results_text: str) -> List[Dict[str, Any]]:
    """Parse a block of OCR-extracted text into a list of race dictionaries."""
    # Pre-processing steps
    results_text = re.sub(r'(\*?\d+\.\d+)([A-Z])', r'\1 \2', results_text) # Separate odds and letters
    results_text = results_text.replace('\f', '\n') # Replace form feeds with newlines

    # Split into potential race blocks based on "RACE #"
    # Using positive lookbehind to keep the delimiter
    race_blocks_with_labels = re.split(r'(?=RACE\s+\d+)', results_text)

    races = []
    current_race_content = ""

    for block in race_blocks_with_labels:
        block = block.strip()
        if not block: continue

        # Attempt to parse header info from the block
        m_num = re.search(r'^RACE\s+(\d+)', block, re.IGNORECASE)
        if m_num: # Start of a new race detected
            # Process the previous race's content if it exists
            if current_race_content:
                parsed_race = parse_single_race_content(current_race_content)
                if parsed_race: races.append(parsed_race)

            # Start accumulating content for the new race
            current_race_content = block
        else:
            # Append content to the current race block
            current_race_content += "\n" + block

    # Process the last accumulated race content
    if current_race_content:
        parsed_race = parse_single_race_content(current_race_content)
        if parsed_race: races.append(parsed_race)

    return races

# Helper function to parse content of a single race
def parse_single_race_content(race_content: str) -> Optional[Dict[str, Any]]:
    """Parses the accumulated text content for a single race."""
        # Strip out anything between "Conditions:" and the next "Gait:"
    race_content = re.sub(r'Conditions:.*?(?=Gait:)', '', race_content, flags=re.DOTALL | re.IGNORECASE)

    m_num = re.search(r'^RACE\s+(\d+)', race_content, re.IGNORECASE)
    if not m_num: return None
    race_number = int(m_num.group(1))

    gait_match = re.search(r'Gait:\s*(Trot|Pace)', race_content, re.IGNORECASE)
    gait_value = gait_match.group(1).title() if gait_match else "Unknown" # Default or log warning?

    date_time_pattern = (r'((?:January|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|June|July|Aug(?:ust)?|'
                         r'Sept(?:ember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)\s+\d{1,2},\s*\d{4}'
                         r'(?:\s+\d{1,2}:\d{1,2}\s*(?:AM|PM))?)') # Adjusted pattern
    date_match = re.search(date_time_pattern, race_content)
    race_datetime = None
    if date_match:
        date_str = date_match.group(1)
        # Try parsing with time first, then date only
        for fmt in ["%B %d, %Y %I:%M %p", "%B %d, %Y"]:
             try:
                 race_datetime = datetime.datetime.strptime(date_str, fmt)
                 if "%I:%M %p" not in fmt: # If date only, set default time (e.g., noon)
                      race_datetime = race_datetime.replace(hour=12, minute=0, second=0, microsecond=0)
                 break
             except ValueError:
                 continue

    # Attempt to refine time using 'Off:' time if date was found
    if race_datetime:
        off_match = re.search(r'Off:\s*(\d{1,2}:\d{1,2}(?:\s*[APap][Mm])?)', race_content)
        if off_match:
            off_time_str = off_match.group(1).replace(" ", "") # Remove spaces for parsing
            # Add PM if missing, assuming races are usually PM
            if not re.search(r'[APap][Mm]', off_time_str, re.IGNORECASE):
                off_time_str += "PM"
            try:
                off_time = datetime.datetime.strptime(off_time_str.upper(), "%I:%M%p").time()
                race_datetime = race_datetime.replace(hour=off_time.hour, minute=off_time.minute, second=0, microsecond=0)
            except ValueError:
                 logging.warning(f"Could not parse Off time '{off_match.group(1)}' for Race {race_number}")


    known_tracks = [
        "Hollywood Casino at The Meadows", "MEADOWLANDS", "Northfield Park",
        "Miami Valley Raceway", "Yonkers Raceway", "Harrah's Philadelphia",
        "Woodbine Mohawk Park", "Hoosier Park", "Pocono Downs" # Add more known tracks
    ]
    track_name = "Unknown Track"
    # Use word boundaries for more robust matching
    for known in known_tracks:
        if re.search(r'\b' + re.escape(known) + r'\b', race_content, re.IGNORECASE):
             track_name = known # Use the canonical name
             break

    starters_match = re.search(r'(?i)(Strs|Starters):\s*(\d+)', race_content)
    starters = int(starters_match.group(2)) if starters_match else None

    # Mark race as qualifier?
    is_qualifier = bool(re.search(r'(?m)^\s*Purse\s*[:=]?\s*\$0\b', race_content, re.IGNORECASE) or
                       re.search(r'\bqualifier\b', race_content, re.IGNORECASE))
        # Try to extract race class (between "Purse" and "1 Mile"/"Distance")
    class_match = re.search(r'Purse\s*[:=]?\s*\$[\d,]+\s+(.*?)\s+(?:1\s*Mile|1M|1\.0M|Distance)', race_content, re.IGNORECASE)
    race_class = class_match.group(1).strip() if class_match else None


    horses = []
    lines = race_content.split('\n')
    table_started = False
    for line in lines:
        line_stripped = line.strip()
        if not line_stripped or line_stripped.lower().startswith("http"): continue

        # More robust header check, allowing variations
        if RE_HEADER.search(line_stripped) or "Post" in line_stripped: # Added "Post" as potential indicator
            table_started = True
            continue

        if table_started:
            # More robust end-of-table check
            if line_stripped.startswith("Time:") or "Pool:" in line_stripped or "Total:" in line_stripped:
                break # Stop processing lines for horses

            cols = re.split(r'\s{2,}', line_stripped) # Split on 2+ spaces, might be better
            if len(cols) < 2: # Need at least HN and something else
                 # Try splitting by single space if multi-space failed
                 cols = re.split(r'\s+', line_stripped)
                 if len(cols) < 5: # Heuristic: need more columns if single space separated
                      continue

            try:
                horse_info = parse_horse_line(cols)
                if horse_info:
                    horses.append({
                        "horse_name": horse_info["horse_name"], # Already stripped in parse_horse_line
                        "finish": horse_info["finish"],
                        "odds": horse_info["odds"],
                        "is_scratched": horse_info["is_scratched"],
                        "driver_name": horse_info.get("driver_name"),  # Add driver name
                        "trainer_name": horse_info.get("trainer_name")  # Add trainer name
                    })
            except Exception as e:
                 logging.error(f"Error parsing line in Race {race_number}: '{line_stripped}'. Error: {e}")
                 continue # Skip problematic lines

    # Only return race if horses were found
    if not horses:
        return None

    return {
        "race_number": race_number,
        "date": race_datetime,
        "track": track_name,
        "gait": gait_value,
        "horses": horses,
        "is_qualifier": is_qualifier,
        "starters": starters, # May be None
        "race_class": race_class

    }

# -------------------------------------------------------------------------------------
# TrueSkill Processing Module
# -------------------------------------------------------------------------------------
def process_parsed_race(race: Dict[str, Any]) -> None:
    """
    Process a single race: determines DB, fetches/decays ratings, updates ratings using TrueSkill,
    and logs results. Handles qualifiers by only updating last_played/last_track.
    Now handles drivers and trainers as well as horses.
    """
    # Basic validation of the race dictionary
    if not all(k in race for k in ["gait", "horses", "date", "track", "race_number"]):
        logging.error(f"Skipping incomplete race data: {race.get('race_number', 'Unknown')}")
        return

    gait_value = race["gait"].strip().lower()
    # Allow for "Galt" -> "Trot" correction
    if gait_value.startswith("galt"):
        gait_value = "trot"
    race["gait"] = gait_value # Update dict if corrected

    # Determine database name
    db_name = "trotters" if gait_value == "trot" else "pacers"
    # Initialize DB (creates tables/columns if needed) - safe to call repeatedly
    init_db(db_name)

    # Filter out scratched horses and those without a valid integer finish position
    valid_horses = [h for h in race["horses"] if not h.get("is_scratched", False) and isinstance(h.get("finish"), int)]

    if len(valid_horses) < 2:
        logging.warning(f"Race {race.get('race_number')} at {race.get('track')} has less than 2 valid finishers. Skipping rating update.")
        # Optionally, still update last_played/last_track for all participants?
        # Decide if even single finishers should have their date/track updated.
        # For now, we just skip the whole race if < 2 finishers.
        return

    # Sort by finish position for TrueSkill ranks
    sorted_horses = sorted(valid_horses, key=lambda x: x["finish"])
    horse_names = [h["horse_name"].lower() for h in sorted_horses] # Use lowercase names consistently
    race_date = race["date"]
    race_track = race["track"] # Get track name

    # Handle Qualifier Races - No Rating Change, Just Update Activity
    if race.get("is_qualifier", False):
        logging.info(f"Processing Qualifier Race {race['race_number']} at {race_track}. Updating last played/track only.")
        for horse_info in race["horses"]:
            if horse_info.get("is_scratched"): continue # Skip scratched

            horse_name = horse_info["horse_name"].lower()
            # Fetch current rating just to pass to update function (mu/sigma won't change)
            rating, _ = get_player_rating(db_name, horse_name, race_date)
            if rating:
                # Update last_played date and last_track name
                update_player_rating(db_name, horse_name, rating, race_date, race_track)
            else:
                # Add horse if new, setting last_played and last_track
                add_horse(db_name, horse_name, race_date, race_track)
                
            # Update last_played for driver if available
            driver_name = horse_info.get("driver_name")
            if driver_name:
                driver_rating = get_person_rating(db_name, driver_name, "driver", race_date)
                if driver_rating:
                    update_person_rating(db_name, driver_name, "driver", driver_rating, race_date, race_track)
                else:
                    add_person(db_name, driver_name, "driver", race_date, race_track)

            # Update last_played for trainer if available
            trainer_name = horse_info.get("trainer_name")
            if trainer_name:
                trainer_rating = get_person_rating(db_name, trainer_name, "trainer", race_date)
                if trainer_rating:
                    update_person_rating(db_name, trainer_name, "trainer", trainer_rating, race_date, race_track)
                else:
                    add_person(db_name, trainer_name, "trainer", race_date, race_track)
                
        return # Stop processing for qualifiers

    # --- Process Regular Race for Rating Updates ---
    
    # Store race entries for future reference
    for horse_info in sorted_horses:
        horse_name = horse_info["horse_name"].lower()
        driver_name = horse_info.get("driver_name")
        trainer_name = horse_info.get("trainer_name")
        finish_position = str(horse_info["finish"])
        
        store_race_entry(
            db_name, race_date, race_track, race["race_number"],
            horse_name, driver_name, trainer_name,
            finish_position, race.get("race_class"),
            race["gait"], race.get("is_qualifier", False)
        )

    # Prepare ranks for TrueSkill (0-based index)
    # Handle potential ties if needed, TrueSkill's rate function takes ranks
    ranks_0_based = [h["finish"] - 1 for h in sorted_horses] # Assumes finish is 1, 2, 3...

    # Process horse ratings
    # Fetch existing ratings (with decay applied) or create new ones
    # Pass race_track to fetch_and_decay_rating in case it needs to add the horse
    decayed_ratings = [fetch_and_decay_rating(db_name, name, race_date, race_track) for name in horse_names]

    # Format for TrueSkill (each horse is a 'team' of one)
    teams = [(r,) for r in decayed_ratings]

    try:
        # Calculate new ratings
        updated_teams = trueskill.rate(teams, ranks=ranks_0_based)
    except Exception as e:
        logging.error(f"TrueSkill rating failed for Race {race['race_number']} at {race_track}. Error: {e}")
        return # Skip updating if rating calculation fails

    # Update database with new ratings and log history
    logging.info("--- Processed Race %d (%s) at %s on %s ---", race['race_number'], race['gait'].title(), race_track, race_date)
    for horse_info, old_rating_tuple, updated_team in zip(sorted_horses, teams, updated_teams):
        horse_name = horse_info["horse_name"].lower()
        new_rating = updated_team[0]
        old_rating = old_rating_tuple[0] # Get the rating before the update for logging comparison

        # Update player_ratings with new mu, sigma, last_played, last_track
        update_player_rating(db_name, horse_name, new_rating, race_date, race_track)

        # Log this result to horse_history, passing the track name
        log_horse_race(
            db_name,
            horse_name,
            new_rating.mu,
            new_rating.sigma,
            race_date,
            race_track,
            finish_position=str(horse_info.get("finish")),
            race_class=race.get("race_class")
        )

        # Log details
        logging.info("  [%s] %s (Fin: %s) -> Mu: %.2f -> %.2f, Sigma: %.2f -> %.2f",
                     horse_info.get("hn", "?"), # Include HN if available
                     horse_info['horse_name'],
                     horse_info['finish'],
                     old_rating.mu, new_rating.mu,
                     old_rating.sigma, new_rating.sigma)
    
    # Process driver ratings
    driver_entities = []
    for horse_info in sorted_horses:
        driver_name = horse_info.get("driver_name")
        if not driver_name:
            continue
            
        # Get or create driver rating
        driver_rating = fetch_and_decay_person_rating(db_name, driver_name, "driver", race_date, race_track)
        driver_rank = horse_info["finish"] - 1  # 0-based for TrueSkill
        
        driver_entities.append({
            "name": driver_name,
            "rating": driver_rating,
            "rank": driver_rank,
            "horse_name": horse_info["horse_name"]
        })

    # Update driver ratings if we have any
    if driver_entities:
        # Format for TrueSkill
        driver_teams = [(r["rating"],) for r in driver_entities]
        driver_ranks = [r["rank"] for r in driver_entities]
        
        try:
            # Calculate new ratings
            updated_driver_teams = trueskill.rate(driver_teams, ranks=driver_ranks)
            
            # Update database with new ratings and log history
            for driver_info, updated_team in zip(driver_entities, updated_driver_teams):
                driver_name = driver_info["name"]
                new_rating = updated_team[0]
                old_rating = driver_info["rating"]
                
                # Update driver_ratings
                update_person_rating(db_name, driver_name, "driver", new_rating, race_date, race_track)
                
                # Log history
                log_person_race(
                    db_name, driver_name, "driver", new_rating.mu, new_rating.sigma,
                    race_date, race_track, driver_info["horse_name"], 
                    str(driver_info["rank"] + 1), race.get("race_class")
                )
                
                # Log to console
                logging.info(f"Driver: {driver_name} (Finish: {driver_info['rank'] + 1}) -> "
                            f"Mu: {old_rating.mu:.2f} -> {new_rating.mu:.2f}")
                
        except Exception as e:
            logging.error(f"TrueSkill rating failed for drivers in Race {race['race_number']} at {race_track}. Error: {e}")
    
    # Process trainer ratings
    trainer_entities = []
    for horse_info in sorted_horses:
        trainer_name = horse_info.get("trainer_name")
        if not trainer_name:
            continue
            
        # Get or create trainer rating
        trainer_rating = fetch_and_decay_person_rating(db_name, trainer_name, "trainer", race_date, race_track)
        trainer_rank = horse_info["finish"] - 1  # 0-based for TrueSkill
        
        trainer_entities.append({
            "name": trainer_name,
            "rating": trainer_rating,
            "rank": trainer_rank,
            "horse_name": horse_info["horse_name"]
        })

    # Update trainer ratings if we have any
    if trainer_entities:
        # Format for TrueSkill
        trainer_teams = [(r["rating"],) for r in trainer_entities]
        trainer_ranks = [r["rank"] for r in trainer_entities]
        
        try:
            # Calculate new ratings
            updated_trainer_teams = trueskill.rate(trainer_teams, ranks=trainer_ranks)
            
            # Update database with new ratings and log history
            for trainer_info, updated_team in zip(trainer_entities, updated_trainer_teams):
                trainer_name = trainer_info["name"]
                new_rating = updated_team[0]
                old_rating = trainer_info["rating"]
                
                # Update trainer_ratings
                update_person_rating(db_name, trainer_name, "trainer", new_rating, race_date, race_track)
                
                # Log history
                log_person_race(
                    db_name, trainer_name, "trainer", new_rating.mu, new_rating.sigma,
                    race_date, race_track, trainer_info["horse_name"], 
                    str(trainer_info["rank"] + 1), race.get("race_class")
                )
                
                # Log to console
                logging.info(f"Trainer: {trainer_name} (Finish: {trainer_info['rank'] + 1}) -> "
                            f"Mu: {old_rating.mu:.2f} -> {new_rating.mu:.2f}")
                
        except Exception as e:
            logging.error(f"TrueSkill rating failed for trainers in Race {race['race_number']} at {race_track}. Error: {e}")

# -------------------------------------------------------------------------------------
# Main Script Module
# -------------------------------------------------------------------------------------
def main() -> None:
    global DRY_RUN
    parser = argparse.ArgumentParser(description="Update horse ratings from race file.")
    parser.add_argument("--dry-run", action="store_true", help="Run without writing to database")
    args = parser.parse_args()
    DRY_RUN = args.dry_run
    if DRY_RUN:
        logging.info("Running in DRY RUN mode – no changes will be written to the database.")

    # Setup TrueSkill environment
    # Using mpmath for potentially higher precision if needed, can revert to default if not required
    try:
        import mpmath
        mpmath.mp.dps = 50 # Set decimal places precision
        trueskill.setup(mu=DEFAULT_MU, sigma=DEFAULT_SIGMA, beta=DEFAULT_SIGMA/2, tau=DEFAULT_SIGMA/100, draw_probability=0.0, backend='mpmath')
        logging.info("Using mpmath backend for TrueSkill.")
    except ImportError:
        trueskill.setup(mu=DEFAULT_MU, sigma=DEFAULT_SIGMA, beta=DEFAULT_SIGMA/2, tau=DEFAULT_SIGMA/100, draw_probability=0.0)
        logging.info("mpmath not found. Using default backend for TrueSkill.")


    # File containing the race results text
    txt_file = "upload.txt" # Make sure this file exists in the same directory or provide full path
    try:
        with open(txt_file, "r", encoding="utf-8") as f:
            raw_text = f.read()
            logging.info(f"Successfully read {len(raw_text)} characters from {txt_file}")
    except FileNotFoundError:
        logging.error(f"Error: Input file '{txt_file}' not found.")
        return
    except Exception as e:
        logging.error(f"Error reading file '{txt_file}': {e}")
        return

    # Parse the raw text into structured race data
    all_races = parse_races_from_text(raw_text)
    logging.info(f"Parsed {len(all_races)} potential races from text.")

    # Process each parsed race
    processed_count = 0
    for race in all_races:
        # Basic check if race dictionary seems valid before processing
        if race and race.get("horses"):
            try:
                process_parsed_race(race)
                processed_count += 1
            except Exception as e:
                # Catch potential errors during processing of a single race
                logging.error(f"Unhandled error processing Race {race.get('race_number', 'Unknown')} at {race.get('track', 'Unknown')}: {e}", exc_info=True) # Log stack trace
        else:
             logging.warning("Skipping invalid or empty race data block.")

    logging.info(f"Finished processing. Attempted to process {processed_count} races.")


if __name__ == "__main__":
    main()