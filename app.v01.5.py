"""
Stable Management SaaS - Flask Application

This is a white-label ready Flask application for horse stable management.
Includes individual and CSV bulk upload for: Races, Expenses, Owners, Horses, Payments
"""

from flask import Flask, jsonify, render_template, request, session, redirect, url_for, flash
import mysql.connector
from mysql.connector import Error
from decimal import Decimal
import hashlib
import os
import csv
import io
from datetime import date, datetime, timedelta
from functools import wraps
from stable_saas.services import DashboardService
import logging
from logging.handlers import RotatingFileHandler

app = Flask(__name__)

# -----------------------------------------------------------------------------
# LOGGING (production-safe)
# -----------------------------------------------------------------------------
if not app.debug:
    handler = RotatingFileHandler("/var/log/stable_app/app.log", maxBytes=5_000_000, backupCount=3)
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter(
        "%(asctime)s %(levelname)s [%(name)s] %(message)s"
    )
    handler.setFormatter(formatter)
    app.logger.addHandler(handler)
    app.logger.setLevel(logging.INFO)

PUBLIC_PATHS = {
    "/login",
    "/logout",
    "/owner/login",
    "/owner/logout",
    "/health",
    "/health/db",
}

PUBLIC_PREFIXES = (
    "/static/",
)

@app.before_request
def require_login_and_tenant():
    path = request.path

    # allow static + public endpoints
    if path in PUBLIC_PATHS or any(path.startswith(p) for p in PUBLIC_PREFIXES):
        return None

    # allow auth + tenant picker pages to work
    if path.startswith("/tenant"):
        if not session.get("staff_id"):
            return redirect(url_for("login_page"))
        return None

    # everything else requires staff login
    if not session.get("staff_id"):
        return redirect(url_for("login_page"))

    # and requires tenant selection (except if you want certain pages to be tenantless)
    if not session.get("tenant_id"):
        return redirect(url_for("tenant_picker"))

    return None

from stable_saas.auth import auth_bp
app.register_blueprint(auth_bp)

# Load from environment variables for security (SaaS-ready)
app.secret_key = os.getenv('FLASK_SECRET_KEY', 'change-this-in-production')

# MySQL database configuration from environment
db_config = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': int(os.getenv('DB_PORT', '3306')),
    'user': os.getenv('DB_USER', 'root'),
    'password': os.getenv('DB_PASSWORD', ''),
    'database': os.getenv('DB_NAME', 'horse_stable')
}

# If you're behind nginx/gunicorn reverse proxy, this helps Flask understand HTTPS
try:
    from werkzeug.middleware.proxy_fix import ProxyFix
    app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1)
except Exception:
    pass

# ---- Session/Cookie hardening ----
app.config.update(
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SAMESITE="Lax",     # 'Lax' is usually best for dashboards
    PERMANENT_SESSION_LIFETIME=timedelta(hours=12),
)

# Only mark Secure cookies when you're actually on HTTPS.
# If you later force HTTPS via Cloudflare/nginx, set this to True.
app.config["SESSION_COOKIE_SECURE"] = os.getenv("COOKIE_SECURE", "false").lower() == "true"

# =============================================================================
# DATABASE CONNECTION
# =============================================================================

def get_db_connection():
    """Get a database connection."""
    try:
        connection = mysql.connector.connect(**db_config)
        return connection
    except Error as e:
        print("Error connecting to MySQL:", e)
        return None

import bcrypt

def staff_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get("staff_id"):
            return redirect(url_for("login"))
        return f(*args, **kwargs)
    return decorated

def tenant_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get("tenant_id"):
            return redirect(url_for("pick_tenant"))
        return f(*args, **kwargs)
    return decorated


@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "GET":
        return render_template("auth/login.html")

    email = (request.form.get("email") or "").strip().lower()
    password = request.form.get("password") or ""

    conn = get_db_connection()
    if not conn:
        flash("DB connection failed", "error")
        return redirect(url_for("login"))

    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("""
            SELECT staff_id, email, password_hash, display_name, is_active, is_superadmin
            FROM staff_users
            WHERE email = %s
            LIMIT 1
        """, (email,))
        user = cur.fetchone()

        if not user or not user.get("is_active"):
            flash("Invalid login", "error")
            return redirect(url_for("login"))

        stored = (user.get("password_hash") or "").encode("utf-8")
        ok = bcrypt.checkpw(password.encode("utf-8"), stored)
        if not ok:
            flash("Invalid login", "error")
            return redirect(url_for("login"))

        # Logged in
        session.clear()
        session["staff_id"] = int(user["staff_id"])
        session["staff_email"] = user["email"]
        session["staff_name"] = user.get("display_name") or user["email"]
        session["is_superadmin"] = int(user.get("is_superadmin") or 0)

        return redirect(url_for("pick_tenant"))
    finally:
        cur.close()
        conn.close()


@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login_page"))

from werkzeug.exceptions import HTTPException

@app.errorhandler(Exception)
def handle_exception(e):
    # Pass through HTTP errors (404, 403, etc.)
    if isinstance(e, HTTPException):
        return e

    # Log full stack trace server-side
    app.logger.exception("Unhandled exception: %s", e)

    # Return clean response to user
    return render_template("error_500.html"), 500


@app.route("/tenant", methods=["GET", "POST"])
@staff_required
def pick_tenant():
    conn = get_db_connection()
    if not conn:
        flash("DB connection failed", "error")
        return redirect(url_for("login"))

    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("""
            SELECT tc.tenant_id, tc.tenant_name
            FROM staff_tenant_access sta
            JOIN tenant_config tc ON tc.tenant_id = sta.tenant_id
            WHERE sta.staff_id = %s
              AND sta.is_active = 1
              AND tc.is_active = 1
            ORDER BY tc.tenant_name
        """, (session["staff_id"],))
        tenants = cur.fetchall() or []
    finally:
        cur.close()
        conn.close()

    if request.method == "POST":
        chosen = (request.form.get("tenant_id") or "").strip()
        allowed = {t["tenant_id"] for t in tenants}
        if chosen not in allowed:
            flash("Invalid tenant selection", "error")
            return redirect(url_for("pick_tenant"))

        session["tenant_id"] = chosen
        return redirect(url_for("index"))

    # Auto-pick if only one
    if len(tenants) == 1:
        session["tenant_id"] = tenants[0]["tenant_id"]
        return redirect(url_for("index"))

    return render_template("auth/tenant_picker.html", tenants=tenants)

# =============================================================================
# AUTHENTICATION DECORATORS
# =============================================================================

def admin_required(f):
    """Decorator to require admin authentication."""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not session.get('is_admin'):
            return jsonify({'error': 'Admin access required'}), 403
        return f(*args, **kwargs)
    return decorated_function

def staff_login_required(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        if not session.get("staff_id"):
            return redirect(url_for("login"))
        return f(*args, **kwargs)
    return wrapper


def tenant_required(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        if not session.get("tenant_id"):
            return redirect(url_for("tenant_picker"))
        return f(*args, **kwargs)
    return wrapper

def owner_required(f):
    """Decorator to require owner authentication."""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not session.get('is_owner'):
            return jsonify({'error': 'Owner access required'}), 401
        return f(*args, **kwargs)
    return decorated_function


def hash_password(password):
    """Hash a password using SHA256."""
    return hashlib.sha256(password.encode()).hexdigest()

def get_current_tenant_id():
    # set during tenant picker after login
    return session.get("tenant_id")

# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================

def parse_date(date_str):
    """Parse a date string in various formats."""
    if not date_str or date_str.strip() == '':
        return None
    
    formats = ['%Y-%m-%d', '%m/%d/%Y', '%m-%d-%Y', '%Y/%m/%d']
    for fmt in formats:
        try:
            return datetime.strptime(date_str.strip(), fmt).date()
        except ValueError:
            continue
    return None


def parse_decimal(value, default=0.0):
    """Parse a decimal value safely."""
    if value is None or value == '':
        return default
    try:
        # Remove currency symbols and commas
        cleaned = str(value).replace('$', '').replace(',', '').strip()
        return float(cleaned)
    except (ValueError, TypeError):
        return default


def validate_required_fields(data, required_fields):
    """Validate that required fields are present."""
    missing = []
    for field in required_fields:
        if field not in data or data[field] in [None, '', 'null']:
            missing.append(field)
    return missing

def require_tenant_id():
    tenant_id = session.get("tenant_id")
    if not tenant_id:
        return None, (jsonify({"error": "No tenant selected"}), 401)
    return tenant_id, None

# =============================================================================
# MAIN PAGES
# =============================================================================

@app.route('/')
@staff_required
@tenant_required
def index():
    """Main dashboard page."""
    return render_template('dashboard.html')


@app.route('/owners')
@staff_required
@tenant_required
def owners_page():
    """Owners listing page."""
    return render_template('owners.html')


@app.route('/reports')
@staff_required
@tenant_required
def reports_page():
    """Reports page."""
    return render_template('reports_dashboard.html')


@app.route('/expense_form')
@staff_required
@tenant_required
def expense_form_page():
    """Expense form page."""
    return render_template('expense_form.html')


@app.route('/profit_loss')
@staff_required
@tenant_required
def profit_loss_page():
    """Profit/Loss report page."""
    return render_template('profit_loss.html')


# =============================================================================
# OWNER API ROUTES
# =============================================================================

@app.route('/api/owners', methods=['GET'])
@staff_required
@tenant_required
def get_owners():
    """Get all owners."""
    tenant_id = get_current_tenant_id()
    conn = get_db_connection()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500
    
    cursor = conn.cursor(dictionary=True)
    try:
        cursor.execute("""
            SELECT owner_id, name, email, phone 
            FROM Owners
            WHERE tenant_id = %s
            ORDER BY name
        """, (tenant_id,))
        owners = cursor.fetchall()
        return jsonify(owners)
    finally:
        cursor.close()
        conn.close()


@app.route('/api/owners/<int:owner_id>', methods=['GET'])
@staff_required
@tenant_required
def get_owner(owner_id):
    """Get detailed owner information including horses."""
    tenant_id = get_current_tenant_id()
    conn = get_db_connection()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500
    
    cursor = conn.cursor(dictionary=True)
    try:
        # Get owner details
        cursor.execute("""
            SELECT * FROM Owners 
            WHERE owner_id = %s AND tenant_id = %s
        """, (owner_id, tenant_id))
        owner = cursor.fetchone()
        
        if not owner:
            return jsonify({'error': 'Owner not found'}), 404
        
        # Get horses owned
        cursor.execute("""
            SELECT h.*, o.percentage_ownership
            FROM Horses h
            JOIN Ownership o ON h.horse_id = o.horse_id
            WHERE o.owner_id = %s AND h.tenant_id = %s AND o.tenant_id = %s
        """, (owner_id, tenant_id, tenant_id))
        horses = cursor.fetchall()
        
        owner['horses'] = horses
        return jsonify(owner)
    finally:
        cursor.close()
        conn.close()


@app.route('/api/owners', methods=['POST'])
@staff_required
@tenant_required
def add_owner():
    """Add a new owner."""
    tenant_id = get_current_tenant_id()
    data = request.get_json()
    
    missing = validate_required_fields(data, ['name', 'email'])
    if missing:
        return jsonify({'error': f'Missing required fields: {", ".join(missing)}'}), 400
    
    conn = get_db_connection()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500
        
    cursor = conn.cursor()
    try:
        cursor.execute("""
            INSERT INTO Owners (tenant_id, name, email, phone, address, city, state, zip, 
                              password_hash)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            tenant_id,
            data['name'], 
            data['email'], 
            data.get('phone'),
            data.get('address'),
            data.get('city'),
            data.get('state'),
            data.get('zip'),
            hash_password(data.get('password', 'changeme'))
        ))
        conn.commit()
        return jsonify({'message': 'Owner added successfully', 'owner_id': cursor.lastrowid}), 201
    except Error as e:
        conn.rollback()
        return jsonify({'error': str(e)}), 500
    finally:
        cursor.close()
        conn.close()


@app.route('/api/owners/upload', methods=['POST'])
@staff_required
@tenant_required
def upload_owners():
    """Upload multiple owners from CSV."""
    tenant_id = get_current_tenant_id()
    if 'file' not in request.files:
        return jsonify({'error': 'No file uploaded'}), 400
    
    file = request.files['file']
    if not file.filename.endswith('.csv'):
        return jsonify({'error': 'File must be CSV format'}), 400
    
    conn = get_db_connection()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500
        
    cursor = conn.cursor()
    
    try:
        stream = io.StringIO(file.stream.read().decode("UTF8"), newline=None)
        csv_reader = csv.DictReader(stream)
        
        added = 0
        errors = []
        
        for row in csv_reader:
            try:
                # Allow multiple name fields
                name = row.get('name') or row.get('Name') or row.get('owner_name') or row.get('Owner')
                email = row.get('email') or row.get('Email')
                
                if not name or not email:
                    errors.append(f"Row missing name or email: {row}")
                    continue
                
                cursor.execute("""
                    INSERT INTO Owners (tenant_id, name, email, phone, address, city, state, zip, 
                                      password_hash)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    tenant_id,
                    name,
                    email,
                    row.get('phone', ''),
                    row.get('address', ''),
                    row.get('city', ''),
                    row.get('state', ''),
                    row.get('zip', ''),
                    hash_password(row.get('password', 'changeme'))
                ))
                added += 1
            except Exception as e:
                errors.append(f"Error adding {name}: {str(e)}")
        
        conn.commit()
        return jsonify({
            'message': f'Added {added} owners successfully',
            'errors': errors if errors else None
        }), 201
        
    except Exception as e:
        conn.rollback()
        return jsonify({'error': str(e)}), 500
    finally:
        cursor.close()
        conn.close()


# =============================================================================
# HORSE API ROUTES
# =============================================================================

@app.route('/api/horses', methods=['GET'])
@staff_required
@tenant_required
def get_horses():
    """Get all horses with optional status filter."""
    tenant_id = get_current_tenant_id()
    status = request.args.get('status')
    
    conn = get_db_connection()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500
        
    cursor = conn.cursor(dictionary=True)
    try:
        if status:
            cursor.execute("""
                SELECT horse_id, name, status, registration_number, breed, color, sex,
                       birth_date, sale_date, inactive_date, purchase_price, sale_price,
                       exempt_from_earnings_credit
                FROM Horses
                WHERE tenant_id = %s AND status = %s
                ORDER BY name
            """, (tenant_id, status))
        else:
            cursor.execute("""
                SELECT horse_id, name, status, registration_number, breed, color, sex,
                       birth_date, sale_date, inactive_date, purchase_price, sale_price,
                       exempt_from_earnings_credit
                FROM Horses
                WHERE tenant_id = %s
                ORDER BY name
            """, (tenant_id,))
        
        horses = cursor.fetchall()
        
        # Convert dates to strings
        for horse in horses:
            for date_field in ['birth_date', 'sale_date', 'inactive_date']:
                if horse[date_field]:
                    horse[date_field] = str(horse[date_field])
        
        return jsonify(horses)
    finally:
        cursor.close()
        conn.close()


@app.route('/api/horses', methods=['POST'])
@staff_required
@tenant_required
def add_horse():
    """Add a single horse."""
    tenant_id = get_current_tenant_id()
    data = request.get_json()
    
    # Validate required fields
    missing = validate_required_fields(data, ['name'])
    if missing:
        return jsonify({'error': f'Missing required fields: {", ".join(missing)}'}), 400
    
    conn = get_db_connection()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500
    
    cursor = conn.cursor()
    try:
        cursor.execute("""
            INSERT INTO Horses (tenant_id, name, status, registration_number, breed, color, sex,
                               birth_date, purchase_price, exempt_from_earnings_credit)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            tenant_id,
            data['name'],
            data.get('status', 'in_training'),
            data.get('registration_number'),
            data.get('breed'),
            data.get('color'),
            data.get('sex'),
            parse_date(data.get('birth_date')),
            parse_decimal(data.get('purchase_price')),
            bool(data.get('exempt_from_earnings_credit', False))
        ))
        conn.commit()
        return jsonify({'message': 'Horse added successfully', 'horse_id': cursor.lastrowid}), 201
    except Error as e:
        conn.rollback()
        return jsonify({'error': str(e)}), 500
    finally:
        cursor.close()
        conn.close()


@app.route('/api/horses/bulk-upload', methods=['POST'])
@staff_required
@tenant_required
def bulk_upload_horses():
    """Upload multiple horses from CSV."""
    tenant_id = get_current_tenant_id()
    if 'file' not in request.files:
        return jsonify({'error': 'No file uploaded'}), 400
    
    file = request.files['file']
    if not file.filename.endswith('.csv'):
        return jsonify({'error': 'File must be CSV format'}), 400
    
    conn = get_db_connection()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500
        
    cursor = conn.cursor()
    
    try:
        stream = io.StringIO(file.stream.read().decode("UTF8"), newline=None)
        csv_reader = csv.DictReader(stream)
        
        added = 0
        errors = []
        
        for row_num, row in enumerate(csv_reader, start=2):
            try:
                horse_name = row.get('name') or row.get('Name') or row.get('horse_name')
                
                if not horse_name:
                    errors.append(f"Row {row_num}: Missing horse name")
                    continue
                
                cursor.execute("""
                    INSERT INTO Horses (tenant_id, name, status, registration_number, breed, color, sex,
                                       birth_date, purchase_date, purchase_price, 
                                       sale_date, sale_price, inactive_date, 
                                       exempt_from_earnings_credit)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    tenant_id,
                    horse_name,
                    row.get('status', 'in_training'),
                    row.get('registration_number'),
                    row.get('breed'),
                    row.get('color'),
                    row.get('sex'),
                    parse_date(row.get('birth_date')),
                    parse_date(row.get('purchase_date')),
                    parse_decimal(row.get('purchase_price')),
                    parse_date(row.get('sale_date')),
                    parse_decimal(row.get('sale_price')),
                    parse_date(row.get('inactive_date')),
                    row.get('exempt_from_earnings_credit', '').lower() in ['true', '1', 'yes']
                ))
                
                horse_id = cursor.lastrowid
                
                # Handle ownership
                ownership_percentages = []
                owner_ids = []
                
                for i in range(1, 6):  # Support up to 5 owners
                    owner_name = row.get(f'owner{i}') or row.get(f'owner_{i}') or row.get(f'owner {i}')
                    percentage = row.get(f'percentage{i}') or row.get(f'ownership{i}') or row.get(f'percent{i}')
                    
                    if owner_name:
                        cursor.execute("""
                            SELECT owner_id FROM Owners 
                            WHERE name = %s AND tenant_id = %s
                            LIMIT 1
                        """, (owner_name.strip(), tenant_id))
                        
                        owner_result = cursor.fetchone()
                        
                        if owner_result:
                            owner_ids.append(owner_result[0])
                            ownership_percentages.append(parse_decimal(percentage, 100))
                        else:
                            errors.append(f"Row {row_num}: Owner '{owner_name}' not found")
                
                # Insert ownership records
                total_percentage = sum(ownership_percentages)
                if owner_ids and total_percentage <= 100:
                    for owner_id, percentage in zip(owner_ids, ownership_percentages):
                        cursor.execute("""
                            INSERT INTO Ownership (tenant_id, horse_id, owner_id, percentage_ownership)
                            VALUES (%s, %s, %s, %s)
                        """, (tenant_id, horse_id, owner_id, percentage))
                elif total_percentage > 100:
                    errors.append(f"Row {row_num}: Ownership percentages exceed 100%")
                
                added += 1
                
            except Exception as e:
                errors.append(f"Row {row_num}: Error - {str(e)}")
                continue
        
        conn.commit()
        
        return jsonify({
            'message': f'Added {added} horses successfully',
            'added': added,
            'errors': errors if errors else None
        }), 201
        
    except Exception as e:
        conn.rollback()
        return jsonify({'error': str(e)}), 500
    finally:
        cursor.close()
        conn.close()


@app.route('/api/horses/<int:horse_id>', methods=['PUT'])
@staff_required
@tenant_required
def update_horse(horse_id):
    """Update horse details."""
    tenant_id = get_current_tenant_id()
    data = request.get_json()
    
    conn = get_db_connection()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500
        
    cursor = conn.cursor()
    try:
        cursor.execute("""
            UPDATE Horses 
            SET name = %s, status = %s, registration_number = %s,
                breed = %s, color = %s, sex = %s, birth_date = %s,
                sale_date = %s, sale_price = %s, inactive_date = %s,
                exempt_from_earnings_credit = %s
            WHERE horse_id = %s AND tenant_id = %s
        """, (
            data.get('name'),
            data.get('status'),
            data.get('registration_number'),
            data.get('breed'),
            data.get('color'),
            data.get('sex'),
            parse_date(data.get('birth_date')),
            parse_date(data.get('sale_date')),
            parse_decimal(data.get('sale_price')),
            parse_date(data.get('inactive_date')),
            bool(data.get('exempt_from_earnings_credit', False)),
            horse_id,
            tenant_id
        ))
        
        if cursor.rowcount == 0:
            return jsonify({'error': 'Horse not found or access denied'}), 404
            
        conn.commit()
        return jsonify({'message': 'Horse updated successfully'})
    except Error as e:
        conn.rollback()
        return jsonify({'error': str(e)}), 500
    finally:
        cursor.close()
        conn.close()


@app.route('/api/horses/<int:horse_id>', methods=['DELETE'])
@staff_required
@tenant_required
def delete_horse(horse_id):
    """Delete a horse (requires admin)."""
    tenant_id = get_current_tenant_id()
    
    conn = get_db_connection()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500
        
    cursor = conn.cursor()
    try:
        # Check if horse exists and belongs to tenant
        cursor.execute("""
            SELECT horse_id FROM Horses 
            WHERE horse_id = %s AND tenant_id = %s
        """, (horse_id, tenant_id))
        
        if not cursor.fetchone():
            return jsonify({'error': 'Horse not found or access denied'}), 404
        
        # Delete related records first (due to foreign key constraints)
        cursor.execute("DELETE FROM Ownership WHERE horse_id = %s AND tenant_id = %s", (horse_id, tenant_id))
        cursor.execute("DELETE FROM Expenses WHERE horse_id = %s AND tenant_id = %s", (horse_id, tenant_id))
        cursor.execute("DELETE FROM RacePerformance WHERE horse_id = %s AND tenant_id = %s", (horse_id, tenant_id))
        
        # Delete the horse
        cursor.execute("DELETE FROM Horses WHERE horse_id = %s AND tenant_id = %s", (horse_id, tenant_id))
        
        conn.commit()
        return jsonify({'message': 'Horse deleted successfully'})
    except Error as e:
        conn.rollback()
        return jsonify({'error': str(e)}), 500
    finally:
        cursor.close()
        conn.close()


# =============================================================================
# EXPENSE API ROUTES
# =============================================================================

@app.route('/api/expenses', methods=['GET'])
@staff_required
@tenant_required

def get_expenses():
    """Get all expenses."""
    tenant_id = get_current_tenant_id()
    horse_id = request.args.get('horse_id')
    
    conn = get_db_connection()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500
        
    cursor = conn.cursor(dictionary=True)
    try:
        if horse_id:
            cursor.execute("""
                SELECT e.*, h.name as horse_name
                FROM Expenses e
                JOIN Horses h ON e.horse_id = h.horse_id
                WHERE e.tenant_id = %s AND e.horse_id = %s AND h.tenant_id = %s
                ORDER BY e.expense_date DESC
            """, (tenant_id, horse_id, tenant_id))
        else:
            cursor.execute("""
                SELECT e.*, h.name as horse_name
                FROM Expenses e
                LEFT JOIN Horses h ON e.horse_id = h.horse_id
                WHERE e.tenant_id = %s
                ORDER BY e.expense_date DESC
            """, (tenant_id,))
        
        expenses = cursor.fetchall()
        
        # Convert dates and decimals
        for expense in expenses:
            if expense['expense_date']:
                expense['expense_date'] = str(expense['expense_date'])
            if expense['amount']:
                expense['amount'] = float(expense['amount'])
        
        return jsonify(expenses)
    finally:
        cursor.close()
        conn.close()


@app.route('/api/expenses', methods=['POST'])
@staff_required
@tenant_required

def add_expense():
    """Add a single expense."""
    tenant_id = get_current_tenant_id()
    data = request.get_json()
    
    # Validate required fields
    missing = validate_required_fields(data, ['expense_date', 'category', 'amount'])
    if missing:
        return jsonify({'error': f'Missing required fields: {", ".join(missing)}'}), 400
    
    conn = get_db_connection()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500
        
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT horse_id FROM Horses WHERE horse_id = %s AND tenant_id = %s

            INSERT INTO Expenses (tenant_id, expense_date, category, vendor, description, amount, 
                                horse_id, allocate_to_all)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            tenant_id,
            parse_date(data['expense_date']),
            data['category'],
            data.get('vendor'),
            data.get('description'),
            parse_decimal(data['amount']),
            data.get('horse_id') if data.get('horse_id') else None,
            bool(data.get('allocate_to_all', False))
        ))
        conn.commit()
        return jsonify({'message': 'Expense added successfully', 'expense_id': cursor.lastrowid}), 201
    except Error as e:
        conn.rollback()
        return jsonify({'error': str(e)}), 500
    finally:
        cursor.close()
        conn.close()


@app.route('/api/expenses/bulk-upload', methods=['POST'])
@staff_required
@tenant_required

def bulk_upload_expenses():
    """Upload multiple expenses from CSV."""
    tenant_id = get_current_tenant_id()
    if 'file' not in request.files:
        return jsonify({'error': 'No file uploaded'}), 400
    
    file = request.files['file']
    if not file.filename.endswith('.csv'):
        return jsonify({'error': 'File must be CSV format'}), 400
    
    conn = get_db_connection()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500
        
    cursor = conn.cursor()
    
    try:
        stream = io.StringIO(file.stream.read().decode("UTF8"), newline=None)
        csv_reader = csv.DictReader(stream)
        
        added = 0
        errors = []
        
        for row_num, row in enumerate(csv_reader, start=2):
            try:
                expense_date = parse_date(row.get('expense_date') or row.get('date'))
                category = row.get('category') or row.get('expense_type')
                amount = parse_decimal(row.get('amount'))
                
                if not expense_date or not category or amount <= 0:
                    errors.append(f"Row {row_num}: Missing required fields (date, category, or amount)")
                    continue
                
                horse_id = None
                horse_name = row.get('horse_name') or row.get('horse')
                if horse_name:
                    cursor.execute("""
                        SELECT horse_id FROM Horses 
                        WHERE name = %s AND tenant_id = %s
                        LIMIT 1
                    """, (horse_name.strip(), tenant_id))
                    
                    horse_result = cursor.fetchone()
                    if horse_result:
                        horse_id = horse_result[0]
                    else:
                        errors.append(f"Row {row_num}: Horse '{horse_name}' not found")
                
                # Check if allocate_to_all
                allocate_to_all = row.get('allocate_to_all', '').lower() in ['true', '1', 'yes', 'y']
                
                cursor.execute("""
                    INSERT INTO Expenses (tenant_id, expense_date, category, vendor, description, amount, 
                                        horse_id, allocate_to_all)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    tenant_id,
                    expense_date,
                    category,
                    row.get('vendor'),
                    row.get('description'),
                    amount,
                    horse_id,
                    allocate_to_all
                ))
                
                added += 1
                
            except Exception as e:
                errors.append(f"Row {row_num}: Error - {str(e)}")
                continue
        
        conn.commit()
        
        return jsonify({
            'message': f'Added {added} expenses successfully',
            'added': added,
            'errors': errors if errors else None
        }), 201
        
    except Exception as e:
        conn.rollback()
        return jsonify({'error': str(e)}), 500
    finally:
        cursor.close()
        conn.close()


# =============================================================================
# RACE PERFORMANCE API ROUTES
# =============================================================================

@app.route('/api/races', methods=['GET'])
@staff_required
@tenant_required

def get_races():
    """Get all race performances."""
    tenant_id = get_current_tenant_id()
    horse_id = request.args.get('horse_id')
    
    conn = get_db_connection()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500
        
    cursor = conn.cursor(dictionary=True)
    try:
        if horse_id:
            cursor.execute("""
                SELECT r.*, h.name as horse_name
                FROM RacePerformance r
                JOIN Horses h ON r.horse_id = h.horse_id
                WHERE r.tenant_id = %s AND r.horse_id = %s AND h.tenant_id = %s
                ORDER BY r.race_date DESC
            """, (tenant_id, horse_id, tenant_id))
        else:
            cursor.execute("""
                SELECT r.*, h.name as horse_name
                FROM RacePerformance r
                JOIN Horses h ON r.horse_id = h.horse_id
                WHERE r.tenant_id = %s AND h.tenant_id = %s
                ORDER BY r.race_date DESC
            """, (tenant_id, tenant_id))
        
        races = cursor.fetchall()
        
        # Convert dates and decimals
        for race in races:
            if race['race_date']:
                race['race_date'] = str(race['race_date'])
            if race['earnings']:
                race['earnings'] = float(race['earnings'])
        
        return jsonify(races)
    finally:
        cursor.close()
        conn.close()


@app.route('/api/races', methods=['POST'])
@staff_required
@tenant_required

def add_race():
    """Add a single race performance."""
    tenant_id = get_current_tenant_id()
    data = request.get_json()
    
    # Validate required fields
    missing = validate_required_fields(data, ['race_date', 'horse_id'])
    if missing:
        return jsonify({'error': f'Missing required fields: {", ".join(missing)}'}), 400
    
    conn = get_db_connection()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500
        
    cursor = conn.cursor()
    try:
        # Verify horse belongs to tenant
        cursor.execute("""
            SELECT horse_id FROM Horses 
            WHERE horse_id = %s AND tenant_id = %s
        """, (data['horse_id'], tenant_id))
        
        if not cursor.fetchone():
            return jsonify({'error': 'Horse not found or access denied'}), 404
        
        cursor.execute("""
            INSERT INTO RacePerformance (tenant_id, horse_id, race_date, race_name, track, 
                                       finish_position, earnings)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            tenant_id,
            data['horse_id'],
            parse_date(data['race_date']),
            data.get('race_name'),
            data.get('track'),
            data.get('finish_position'),
            parse_decimal(data.get('earnings', 0))
        ))
        conn.commit()
        return jsonify({'message': 'Race added successfully', 'race_id': cursor.lastrowid}), 201
    except Error as e:
        conn.rollback()
        return jsonify({'error': str(e)}), 500
    finally:
        cursor.close()
        conn.close()


@app.route('/api/races/bulk-upload', methods=['POST'])
@staff_required
@tenant_required

def bulk_upload_races():
    """Upload multiple race performances from CSV."""
    tenant_id = get_current_tenant_id()
    if 'file' not in request.files:
        return jsonify({'error': 'No file uploaded'}), 400
    
    file = request.files['file']
    if not file.filename.endswith('.csv'):
        return jsonify({'error': 'File must be CSV format'}), 400
    
    conn = get_db_connection()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500
        
    cursor = conn.cursor()
    
    try:
        stream = io.StringIO(file.stream.read().decode("UTF8"), newline=None)
        csv_reader = csv.DictReader(stream)
        
        added = 0
        errors = []
        
        for row_num, row in enumerate(csv_reader, start=2):
            try:
                horse_name = row.get('horse_name') or row.get('horse')
                race_date = parse_date(row.get('race_date') or row.get('date'))
                
                if not horse_name or not race_date:
                    errors.append(f"Row {row_num}: Missing horse name or race date")
                    continue
                
                # Find horse_id
                cursor.execute("""
                    SELECT horse_id FROM Horses 
                    WHERE name = %s AND tenant_id = %s
                    LIMIT 1
                """, (horse_name.strip(), tenant_id))
                
                horse_result = cursor.fetchone()
                
                if not horse_result:
                    errors.append(f"Row {row_num}: Horse '{horse_name}' not found")
                    continue
                
                horse_id = horse_result[0]
                
                cursor.execute("""
                    INSERT INTO RacePerformance (tenant_id, horse_id, race_date, race_name, track, 
                                               finish_position, earnings)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (
                    tenant_id,
                    horse_id,
                    race_date,
                    row.get('race_name') or row.get('race'),
                    row.get('track'),
                    row.get('finish_position') or row.get('position') or row.get('place'),
                    parse_decimal(row.get('earnings', 0))
                ))
                
                added += 1
                
            except Exception as e:
                errors.append(f"Row {row_num}: Error - {str(e)}")
                continue
        
        conn.commit()
        
        return jsonify({
            'message': f'Added {added} races successfully',
            'added': added,
            'errors': errors if errors else None
        }), 201
        
    except Exception as e:
        conn.rollback()
        return jsonify({'error': str(e)}), 500
    finally:
        cursor.close()
        conn.close()


# =============================================================================
# PAYMENT API ROUTES
# =============================================================================

@app.route('/api/payments', methods=['GET'])
@staff_required
@tenant_required

def get_payments():
    """Get all payments."""
    tenant_id = get_current_tenant_id()
    owner_id = request.args.get('owner_id')
    
    conn = get_db_connection()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500
        
    cursor = conn.cursor(dictionary=True)
    try:
        if owner_id:
            cursor.execute("""
                SELECT p.*, o.name as owner_name
                FROM Payments p
                JOIN Owners o ON p.owner_id = o.owner_id
                WHERE p.tenant_id = %s AND p.owner_id = %s AND o.tenant_id = %s
                ORDER BY p.payment_date DESC
            """, (tenant_id, owner_id, tenant_id))
        else:
            cursor.execute("""
                SELECT p.*, o.name as owner_name
                FROM Payments p
                JOIN Owners o ON p.owner_id = o.owner_id
                WHERE p.tenant_id = %s AND o.tenant_id = %s
                ORDER BY p.payment_date DESC
            """, (tenant_id, tenant_id))
        
        payments = cursor.fetchall()
        
        # Convert dates and decimals
        for payment in payments:
            if payment['payment_date']:
                payment['payment_date'] = str(payment['payment_date'])
            if payment['amount']:
                payment['amount'] = float(payment['amount'])
        
        return jsonify(payments)
    finally:
        cursor.close()
        conn.close()


@app.route('/api/payments', methods=['POST'])
@staff_required
@tenant_required

def add_payment():
    """Add a single payment."""
    tenant_id = get_current_tenant_id()
    data = request.get_json()
    
    # Validate required fields
    missing = validate_required_fields(data, ['payment_date', 'owner_id', 'amount'])
    if missing:
        return jsonify({'error': f'Missing required fields: {", ".join(missing)}'}), 400
    
    conn = get_db_connection()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500
        
    cursor = conn.cursor()
    try:
        # Verify owner belongs to tenant
        cursor.execute("""
            SELECT owner_id FROM Owners 
            WHERE owner_id = %s AND tenant_id = %s
        """, (data['owner_id'], tenant_id))
        
        if not cursor.fetchone():
            return jsonify({'error': 'Owner not found or access denied'}), 404
        
        cursor.execute("""
            INSERT INTO Payments (tenant_id, owner_id, payment_date, amount, payment_method, 
                                reference_number, notes)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            tenant_id,
            data['owner_id'],
            parse_date(data['payment_date']),
            parse_decimal(data['amount']),
            data.get('payment_method'),
            data.get('reference_number'),
            data.get('notes')
        ))
        conn.commit()
        return jsonify({'message': 'Payment added successfully', 'payment_id': cursor.lastrowid}), 201
    except Error as e:
        conn.rollback()
        return jsonify({'error': str(e)}), 500
    finally:
        cursor.close()
        conn.close()


@app.route('/api/payments/bulk-upload', methods=['POST'])
@staff_required
@tenant_required

def bulk_upload_payments():
    """Upload multiple payments from CSV."""
    tenant_id = get_current_tenant_id()
    if 'file' not in request.files:
        return jsonify({'error': 'No file uploaded'}), 400
    
    file = request.files['file']
    if not file.filename.endswith('.csv'):
        return jsonify({'error': 'File must be CSV format'}), 400
    
    conn = get_db_connection()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500
        
    cursor = conn.cursor()
    
    try:
        stream = io.StringIO(file.stream.read().decode("UTF8"), newline=None)
        csv_reader = csv.DictReader(stream)
        
        added = 0
        errors = []
        
        for row_num, row in enumerate(csv_reader, start=2):
            try:
                owner_name = row.get('owner_name') or row.get('owner')
                payment_date = parse_date(row.get('payment_date') or row.get('date'))
                amount = parse_decimal(row.get('amount'))
                
                if not owner_name or not payment_date or amount <= 0:
                    errors.append(f"Row {row_num}: Missing owner name, date, or valid amount")
                    continue
                
                # Find owner_id
                cursor.execute("""
                    SELECT owner_id FROM Owners 
                    WHERE name = %s AND tenant_id = %s
                    LIMIT 1
                """, (owner_name.strip(), tenant_id))
                
                owner_result = cursor.fetchone()
                
                if not owner_result:
                    errors.append(f"Row {row_num}: Owner '{owner_name}' not found")
                    continue
                
                owner_id = owner_result[0]
                
                cursor.execute("""
                    INSERT INTO Payments (tenant_id, owner_id, payment_date, amount, payment_method, 
                                        reference_number, notes)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (
                    tenant_id,
                    owner_id,
                    payment_date,
                    amount,
                    row.get('payment_method') or row.get('method'),
                    row.get('reference_number') or row.get('check_number'),
                    row.get('notes')
                ))
                
                added += 1
                
            except Exception as e:
                errors.append(f"Row {row_num}: Error - {str(e)}")
                continue
        
        conn.commit()
        
        return jsonify({
            'message': f'Added {added} payments successfully',
            'added': added,
            'errors': errors if errors else None
        }), 201
        
    except Exception as e:
        conn.rollback()
        return jsonify({'error': str(e)}), 500
    finally:
        cursor.close()
        conn.close()


# =============================================================================
# BILLING API ROUTES
# =============================================================================

@app.route('/api/billing/generate', methods=['POST'])
@staff_required
@tenant_required

def generate_bills():
    """Generate bills for all owners based on their horses."""
    tenant_id = get_current_tenant_id()
    data = request.get_json()
    bill_date = parse_date(data.get('bill_date', str(date.today())))
    due_date = parse_date(data.get('due_date', str(date.today())))
    
    conn = get_db_connection()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500
        
    cursor = conn.cursor()
    
    try:
        # Get all owners with horses
        cursor.execute("""
            SELECT DISTINCT o.owner_id, o.name
            FROM Owners o
            JOIN Ownership os ON o.owner_id = os.owner_id
            JOIN Horses h ON os.horse_id = h.horse_id
            WHERE o.tenant_id = %s AND os.tenant_id = %s AND h.tenant_id = %s
                AND h.status != 'sold'
        """, (tenant_id, tenant_id, tenant_id))
        
        owners = cursor.fetchall()
        bills_created = 0
        
        for owner_id, owner_name in owners:
            # Calculate total amount for this owner
            # This is a simplified calculation - you may want to add more logic
            cursor.execute("""
                SELECT SUM(e.amount * os.percentage_ownership / 100) as total_expenses
                FROM Expenses e
                JOIN Horses h ON e.horse_id = h.horse_id
                JOIN Ownership os ON h.horse_id = os.horse_id
                WHERE os.owner_id = %s AND e.tenant_id = %s 
                    AND h.tenant_id = %s AND os.tenant_id = %s
                    AND e.expense_date >= DATE_SUB(%s, INTERVAL 30 DAY)
                    AND e.expense_date <= %s
            """, (owner_id, tenant_id, tenant_id, tenant_id, bill_date, bill_date))
            
            result = cursor.fetchone()
            total_amount = float(result[0]) if result[0] else 0
            
            if total_amount > 0:
                cursor.execute("""
                    INSERT INTO Billing (tenant_id, owner_id, bill_date, due_date, total_amount, 
                                       status, notes)
                    VALUES (%s, %s, %s, %s, %s, 'pending', %s)
                """, (
                    tenant_id,
                    owner_id,
                    bill_date,
                    due_date,
                    total_amount,
                    f'Monthly billing for {owner_name}'
                ))
                bills_created += 1
        
        conn.commit()
        return jsonify({
            'message': f'Created {bills_created} bills successfully',
            'bills_created': bills_created
        }), 201
        
    except Exception as e:
        conn.rollback()
        return jsonify({'error': str(e)}), 500
    finally:
        cursor.close()
        conn.close()


# =============================================================================
# OWNERSHIP API ROUTES
# =============================================================================

@app.route('/api/ownership', methods=['POST'])
@staff_required
@tenant_required

def add_ownership():
    """Add or update ownership percentage for a horse/owner combination."""
    tenant_id = get_current_tenant_id()
    data = request.get_json()
    
    missing = validate_required_fields(data, ['horse_id', 'owner_id', 'percentage_ownership'])
    if missing:
        return jsonify({'error': f'Missing required fields: {", ".join(missing)}'}), 400
    
    conn = get_db_connection()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500
        
    cursor = conn.cursor()
    try:
        # Verify horse and owner belong to tenant
        cursor.execute("""
            SELECT horse_id FROM Horses 
            WHERE horse_id = %s AND tenant_id = %s
        """, (data['horse_id'], tenant_id))
        
        if not cursor.fetchone():
            return jsonify({'error': 'Horse not found or access denied'}), 404
            
        cursor.execute("""
            SELECT owner_id FROM Owners 
            WHERE owner_id = %s AND tenant_id = %s
        """, (data['owner_id'], tenant_id))
        
        if not cursor.fetchone():
            return jsonify({'error': 'Owner not found or access denied'}), 404
        
        # Check total ownership doesn't exceed 100%
        cursor.execute("""
            SELECT SUM(percentage_ownership) as total
            FROM Ownership
            WHERE horse_id = %s AND tenant_id = %s AND owner_id != %s
        """, (data['horse_id'], tenant_id, data['owner_id']))
        
        result = cursor.fetchone()
        current_total = float(result[0]) if result[0] else 0
        
        if current_total + float(data['percentage_ownership']) > 100:
            return jsonify({'error': 'Total ownership would exceed 100%'}), 400
        
        # Insert or update ownership
        cursor.execute("""
            INSERT INTO Ownership (tenant_id, horse_id, owner_id, percentage_ownership, effective_date)
            VALUES (%s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE 
                percentage_ownership = VALUES(percentage_ownership),
                effective_date = VALUES(effective_date)
        """, (
            tenant_id,
            data['horse_id'],
            data['owner_id'],
            data['percentage_ownership'],
            parse_date(data.get('effective_date', str(date.today())))
        ))
        
        conn.commit()
        return jsonify({'message': 'Ownership updated successfully'}), 201
    except Error as e:
        conn.rollback()
        return jsonify({'error': str(e)}), 500
    finally:
        cursor.close()
        conn.close()


# =============================================================================
# REPORT API ROUTES
# =============================================================================

@app.route('/api/reports/profit-loss')
@staff_required
@tenant_required

def profit_loss_report():
    """Generate profit/loss report for horses."""
    tenant_id = get_current_tenant_id()
    horse_id = request.args.get('horse_id')
    owner_id = request.args.get('owner_id')
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    
    conn = get_db_connection()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500
        
    cursor = conn.cursor(dictionary=True)
    
    try:
        # Base query for horses
        query = """
            SELECT 
                h.horse_id,
                h.name as horse_name,
                h.status,
                COALESCE(SUM(r.earnings), 0) as total_earnings,
                COALESCE(
                    (SELECT SUM(e.amount) 
                     FROM Expenses e 
                     WHERE e.horse_id = h.horse_id AND e.tenant_id = %s
                     """ + (f" AND e.expense_date >= '{start_date}'" if start_date else "") + \
                     (f" AND e.expense_date <= '{end_date}'" if end_date else "") + """
                    ), 0
                ) as total_expenses
            FROM Horses h
            LEFT JOIN RacePerformance r ON h.horse_id = r.horse_id AND r.tenant_id = %s
        """
        
        params = [tenant_id, tenant_id]
        
        # Add date filters for races
        if start_date:
            query += " AND r.race_date >= %s"
            params.append(start_date)
        if end_date:
            query += " AND r.race_date <= %s"
            params.append(end_date)
        
        # Add owner filter if specified
        if owner_id:
            query += """
                JOIN Ownership o ON h.horse_id = o.horse_id AND o.tenant_id = %s
                WHERE h.tenant_id = %s AND o.owner_id = %s
            """
            params.extend([tenant_id, tenant_id, owner_id])
        else:
            query += " WHERE h.tenant_id = %s"
            params.append(tenant_id)
        
        # Add horse filter if specified
        if horse_id:
            query += " AND h.horse_id = %s"
            params.append(horse_id)
        
        query += " GROUP BY h.horse_id, h.name, h.status"
        
        cursor.execute(query, params)
        results = cursor.fetchall()
        
        # Calculate profit/loss
        for row in results:
            row['total_earnings'] = float(row['total_earnings'])
            row['total_expenses'] = float(row['total_expenses'])
            row['profit_loss'] = row['total_earnings'] - row['total_expenses']
        
        # Summary totals
        summary = {
            'total_earnings': sum(r['total_earnings'] for r in results),
            'total_expenses': sum(r['total_expenses'] for r in results),
            'net_profit_loss': sum(r['profit_loss'] for r in results),
            'horse_count': len(results)
        }
        
        return jsonify({
            'horses': results,
            'summary': summary
        })
        
    finally:
        cursor.close()
        conn.close()


@app.route('/api/reports/owner-statement/<int:owner_id>')
@staff_required
@tenant_required

def owner_statement(owner_id):
    """Generate detailed statement for an owner."""
    tenant_id = get_current_tenant_id()
    
    conn = get_db_connection()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500
        
    cursor = conn.cursor(dictionary=True)
    
    try:
        # Verify owner belongs to tenant
        cursor.execute("""
            SELECT * FROM Owners 
            WHERE owner_id = %s AND tenant_id = %s
        """, (owner_id, tenant_id))
        
        owner = cursor.fetchone()
        if not owner:
            return jsonify({'error': 'Owner not found or access denied'}), 404
        
        # Get horses with ownership percentages
        cursor.execute("""
            SELECT h.*, o.percentage_ownership
            FROM Horses h
            JOIN Ownership o ON h.horse_id = o.horse_id
            WHERE o.owner_id = %s AND h.tenant_id = %s AND o.tenant_id = %s
        """, (owner_id, tenant_id, tenant_id))
        
        horses = cursor.fetchall()
        
        # Get recent bills
        cursor.execute("""
            SELECT * FROM Billing
            WHERE owner_id = %s AND tenant_id = %s
            ORDER BY bill_date DESC
            LIMIT 12
        """, (owner_id, tenant_id))
        
        bills = cursor.fetchall()
        
        # Get recent payments
        cursor.execute("""
            SELECT * FROM Payments
            WHERE owner_id = %s AND tenant_id = %s
            ORDER BY payment_date DESC
            LIMIT 12
        """, (owner_id, tenant_id))
        
        payments = cursor.fetchall()
        
        # Convert dates and calculate balance
        for bill in bills:
            bill['bill_date'] = str(bill['bill_date'])
            bill['due_date'] = str(bill['due_date'])
            bill['total_amount'] = float(bill['total_amount'])
        
        for payment in payments:
            payment['payment_date'] = str(payment['payment_date'])
            payment['amount'] = float(payment['amount'])
        
        total_billed = sum(b['total_amount'] for b in bills)
        total_paid = sum(p['amount'] for p in payments)
        current_balance = total_billed - total_paid
        
        return jsonify({
            'owner': owner,
            'horses': horses,
            'bills': bills,
            'payments': payments,
            'summary': {
                'total_billed': total_billed,
                'total_paid': total_paid,
                'current_balance': current_balance
            }
        })
        
    finally:
        cursor.close()
        conn.close()


# =============================================================================
# DASHBOARD API ROUTES
# =============================================================================

@app.route('/api/dashboard/summary')
@staff_required
@tenant_required

def dashboard_summary():
    """Get dashboard summary statistics."""
    tenant_id = get_current_tenant_id()
    
    conn = get_db_connection()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500
        
    cursor = conn.cursor()
    
    try:
        # Count horses by status
        cursor.execute("""
            SELECT status, COUNT(*) as count
            FROM Horses
            WHERE tenant_id = %s
            GROUP BY status
        """, (tenant_id,))
        
        horse_counts = {row[0]: row[1] for row in cursor.fetchall()}
        
        # Total active horses
        active_statuses = ['in_training', 'racing', 'broodmare', 'turned_out']
        active_horses = sum(horse_counts.get(status, 0) for status in active_statuses)
        
        # Recent earnings (last 30 days)
        cursor.execute("""
            SELECT COALESCE(SUM(r.earnings), 0)
            FROM RacePerformance r
            JOIN Horses h ON r.horse_id = h.horse_id
            WHERE r.tenant_id = %s AND h.tenant_id = %s
                AND r.race_date >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
        """, (tenant_id, tenant_id))
        
        recent_earnings = float(cursor.fetchone()[0])
        
        # Outstanding invoices
        cursor.execute("""
            SELECT COUNT(*), COALESCE(SUM(total_amount), 0)
            FROM Billing
            WHERE tenant_id = %s AND status = 'pending'
        """, (tenant_id,))
        
        invoice_count, outstanding_amount = cursor.fetchone()
        outstanding_amount = float(outstanding_amount)
        
        # Owner count
        cursor.execute("""
            SELECT COUNT(DISTINCT o.owner_id)
            FROM Owners o
            JOIN Ownership os ON o.owner_id = os.owner_id
            WHERE o.tenant_id = %s AND os.tenant_id = %s
        """, (tenant_id, tenant_id))
        
        owner_count = cursor.fetchone()[0]
        
        return jsonify({
            'active_horses': active_horses,
            'horse_counts': horse_counts,
            'recent_earnings': recent_earnings,
            'outstanding_invoices': invoice_count,
            'outstanding_amount': outstanding_amount,
            'owner_count': owner_count
        })
        
    finally:
        cursor.close()
        conn.close()


@app.route('/api/recent_races')
@staff_required
@tenant_required

def get_recent_races():
    """Get recent race results."""
    tenant_id = get_current_tenant_id()
    limit = request.args.get('limit', 20, type=int)
    
    conn = get_db_connection()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500
        
    cursor = conn.cursor(dictionary=True)
    
    try:
        cursor.execute("""
            SELECT
              h.name AS horse_name,
              r.race_date,
              r.race_name,
              r.track,
              r.finish_position AS finishing_position,
              r.earnings
            FROM RacePerformance r
            JOIN Horses h ON r.horse_id = h.horse_id
            WHERE r.tenant_id = %s AND h.tenant_id = %s
            ORDER BY r.race_date DESC
            LIMIT %s;
        """, (tenant_id, tenant_id, limit))
        races = cursor.fetchall()
        
        for r in races:
            if r['earnings'] is None:
                r['earnings'] = 0.0
            else:
                r['earnings'] = float(r['earnings'])
            
            race_date = r['race_date']
            if hasattr(race_date, 'strftime'):
                r['race_date'] = race_date.strftime('%Y-%m-%d')
            elif isinstance(race_date, str):
                pass
            else:
                r['race_date'] = str(race_date)
                
        return jsonify(races)
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        cursor.close()
        conn.close()


@app.route('/api/top_earning_horses')
@staff_required
@tenant_required

def top_earning_horses():
    """Get top 5 earning horses in last 30 days."""
    tenant_id = get_current_tenant_id()
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)

    cursor.execute("""
        SELECT 
            h.name AS horse_name,
            SUM(r.earnings) AS total_earnings
        FROM RacePerformance r
        JOIN Horses h ON r.horse_id = h.horse_id
        WHERE r.race_date >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
          AND h.status != 'sold'
          AND r.tenant_id = %s AND h.tenant_id = %s
        GROUP BY h.horse_id, h.name
        ORDER BY total_earnings DESC
        LIMIT 5;
    """, (tenant_id, tenant_id))
    top5 = cursor.fetchall()
    
    for h in top5:
        if h.get('total_earnings'):
            h['total_earnings'] = float(h['total_earnings'])

    cursor.close()
    conn.close()
    return jsonify(top5)


@app.route('/api/broodmares_turnouts')
@staff_required
@tenant_required

def broodmares_turnouts():
    """Get count of horses in broodmare/turnout/rehab status."""
    tenant_id = get_current_tenant_id()
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT COUNT(*) FROM Horses
        WHERE status IN ('broodmare', 'turned_out','rehab_in_stable', 'rehab_center') 
          AND status != 'sold'
          AND tenant_id = %s;
    """, (tenant_id,))
    count = cursor.fetchone()[0]

    cursor.close()
    conn.close()

    return jsonify({'count': count})


# =============================================================================
# OWNER AUTHENTICATION & DASHBOARD
# =============================================================================

@app.route('/owner/login', methods=['GET', 'POST'])
def owner_login():
    """Owner login page and handler."""
    if request.method == 'POST':
        email = request.form.get('email')
        password = request.form.get('password')
        
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        
        cursor.execute("""
            SELECT owner_id, name, email, tenant_id
            FROM Owners 
            WHERE email = %s AND password_hash = %s
        """, (email, hash_password(password)))
        
        owner = cursor.fetchone()
        cursor.close()
        conn.close()
        
        if owner:
            session['owner_id'] = owner['owner_id']
            session['owner_name'] = owner['name']
            session['is_owner'] = True
            session['tenant_id'] = owner['tenant_id']  # Set tenant context for owner
            return redirect(url_for('owner_dashboard'))
        else:
            return render_template('owner_login.html', error='Invalid email or password')
    
    return render_template('owner_login.html')


@app.route('/owner/logout')
def owner_logout():
    """Log out owner."""
    session.clear()
    return redirect(url_for('owner_login'))


@app.route('/owner/dashboard')
@owner_required
def owner_dashboard():
    """Owner dashboard."""
    return render_template('owner_dashboard.html', 
                         owner_name=session.get('owner_name'),
                         owner_id=session.get('owner_id'))


@app.route('/owner/<int:owner_id>')
@staff_required
@tenant_required
def owner_detail(owner_id):
    """Owner detail page."""
    tenant_id = get_current_tenant_id()
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    
    cursor.execute("""
        SELECT * FROM Owners 
        WHERE owner_id = %s AND tenant_id = %s
    """, (owner_id, tenant_id))
    owner = cursor.fetchone()
    if not owner:
        return "Owner not found", 404

    # Get horses with ownership
    cursor.execute("""
        SELECT h.horse_id, h.name, h.status, h.purchase_price, h.sale_price,
               h.purchase_date, h.sale_date, o.percentage_ownership,
               COALESCE(SUM(r.earnings * o.percentage_ownership / 100), 0) AS earnings
        FROM Horses h
        JOIN Ownership o ON h.horse_id = o.horse_id
        LEFT JOIN RacePerformance r ON h.horse_id = r.horse_id AND r.tenant_id = %s
        WHERE o.owner_id = %s AND h.tenant_id = %s AND o.tenant_id = %s
        GROUP BY h.horse_id, h.name, h.status, h.purchase_price, h.sale_price,
                 h.purchase_date, h.sale_date, o.percentage_ownership
    """, (tenant_id, owner_id, tenant_id, tenant_id))
    earnings_data = {row['horse_id']: row for row in cursor.fetchall()}

    cursor.execute("""
        SELECT h.horse_id,
               COALESCE(SUM(e.amount * o.percentage_ownership / 100), 0) AS expenses
        FROM Horses h
        JOIN Ownership o ON h.horse_id = o.horse_id
        LEFT JOIN Expenses e ON h.horse_id = e.horse_id AND e.tenant_id = %s
        WHERE o.owner_id = %s AND h.tenant_id = %s AND o.tenant_id = %s
        GROUP BY h.horse_id
    """, (tenant_id, owner_id, tenant_id, tenant_id))
    expenses_data = {row['horse_id']: row['expenses'] for row in cursor.fetchall()}

    horses = []
    for horse_id, row in earnings_data.items():
        earnings = float(row['earnings']) if row['earnings'] else 0
        expenses = float(expenses_data.get(horse_id, 0))
        profit_loss = earnings - expenses
        row['earnings'] = earnings
        row['expenses'] = expenses
        row['profit_loss'] = profit_loss
        horses.append(row)

    # Current balance
    cursor.execute("""
        SELECT 
            COALESCE(SUM(b.total_amount), 0) - COALESCE(SUM(p.amount), 0) AS current_balance
        FROM Billing b
        LEFT JOIN Payments p ON b.owner_id = p.owner_id AND p.tenant_id = %s
        WHERE b.owner_id = %s AND b.tenant_id = %s
    """, (tenant_id, owner_id, tenant_id))
    row = cursor.fetchone()
    current_balance = float(row['current_balance']) if row and row['current_balance'] else 0.0

    cursor.execute("""
        SELECT * FROM Billing 
        WHERE owner_id = %s AND tenant_id = %s
    """, (owner_id, tenant_id))
    invoices = cursor.fetchall()

    cursor.close()
    conn.close()

    return render_template('owner_detail.html',
        owner=owner,
        horses=horses,
        invoices=invoices,
        current_balance=current_balance)


# =============================================================================
# ADDITIONAL PAGE ROUTES
# =============================================================================

@app.route('/horse_financials')
@staff_required
@tenant_required

def horse_financials_page():
    return render_template('horse_financials.html')


@app.route('/outstanding_invoices')
@staff_required
@tenant_required

def outstanding_invoices_page():
    return render_template('outstanding_invoices.html')


@app.route('/recent_races')
@staff_required
@tenant_required

def recent_races_page():
    return render_template('recent_races.html')


@app.route('/expense_allocations')
@staff_required
@tenant_required

def expense_allocations_page():
    return render_template('expense_allocations.html')


@app.route('/report/generate_invoices')
@staff_required
@tenant_required

def generate_invoices_page():
    return render_template('report/generate_invoices.html')

@app.route("/health")
@staff_required
@tenant_required

def health():
    return{"ok": True}, 200

@app.route("/health/db")
@staff_required
@tenant_required

def health_db():
    conn = get_db_connection()
    if not conn:
        return {"ok": False, "db": "down"}, 500

     try:
        cur = conn.cursor()
        cur.execute("SELECT 1")
        cur.fetchone()
        return {"ok": True, "db": "up"}, 200
    finally:
        conn.close()

@app.errorhandler(Exception)
@staff_required
@tenant_required

def handle_exception(e):
    # Log full stack trace to journalctl
    import traceback
    traceback.print_exc()

    # Don’t leak internals to the browser in prod
    debug_mode = os.getenv("FLASK_DEBUG", "false").lower() == "true"
    if debug_mode:
        return {"error": str(e)}, 500
    return {"error": "Internal server error"}, 500

# =============================================================================
# RUN APPLICATION
# =============================================================================

if __name__ == '__main__':
    # Use environment variables for production
    debug_mode = os.getenv('FLASK_DEBUG', 'true').lower() == 'true'
    host = os.getenv('FLASK_HOST', '0.0.0.0')
    port = int(os.getenv('FLASK_PORT', '5000'))
    
    app.run(debug=debug_mode, host=host, port=port)