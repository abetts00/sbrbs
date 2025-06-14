import os
import subprocess
import datetime
import sys

# Database configuration - keep in sync with invoice.py
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': 'Sweetlou47',  # Should be moved to environment variables
    'database': 'horse_stable'
}

def create_backup():
    """Create a database backup before running the invoice process."""
    try:
        # Create a backup directory if it doesn't exist
        backup_dir = 'database_backups'
        if not os.path.exists(backup_dir):
            os.makedirs(backup_dir)
        
        # Generate a timestamp for the backup filename
        timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_file = os.path.join(backup_dir, f"horse_stable_backup_{timestamp}.sql")
        
        # Build the mysqldump command
        cmd = [
            'mysqldump',
            f"--host={DB_CONFIG['host']}",
            f"--user={DB_CONFIG['user']}",
            f"--password={DB_CONFIG['password']}",
            '--routines',  # Include stored procedures
            '--triggers',  # Include triggers
            '--add-drop-table',  # Add DROP TABLE statements
            '--skip-comments',  # Skip comments for smaller files
            DB_CONFIG['database']
        ]
        
        # Run the command and redirect output to a file
        with open(backup_file, 'w') as f:
            result = subprocess.run(cmd, stdout=f, check=True)
        
        if result.returncode == 0:
            print(f"✅ Database backup created: {backup_file}")
            return True
        else:
            print(f"❌ Backup failed with code: {result.returncode}")
            return False
            
    except subprocess.CalledProcessError as e:
        print(f"❌ Backup failed: {e}")
        return False
    except Exception as e:
        print(f"❌ Unexpected error during backup: {e}")
        return False

# Create a master script to run the entire process
def create_master_script():
    """Create a master script to run the entire process."""
    with open('generate_invoices.py', 'w') as f:
        f.write("""#!/usr/bin/env python3
import os
import sys
import subprocess
from backup_db import create_backup

def run_command(cmd, description):
    """Run a command and print status."""
    print(f"\\n--- {description} ---")
    result = subprocess.run(cmd, shell=True)
    if result.returncode != 0:
        print(f"❌ {description} failed with code: {result.returncode}")
        return False
    print(f"✅ {description} completed successfully")
    return True

def main():
    # Step 1: Create database backup
    print("\\n=== Starting Invoice Generation Process ===")
    if not create_backup():
        print("Aborting process due to backup failure")
        sys.exit(1)
    
    # Step 2: Generate invoice data
    cmd_args = ' '.join(sys.argv[1:])  # Pass through any arguments
    if not run_command(f"python invoice.py {cmd_args}", "Invoice Data Generation"):
        print("Aborting process due to invoice calculation failure")
        sys.exit(1)
    
    # Step 3: Generate PDF invoices
    if not run_command("python pdfs1.py", "PDF Generation"):
        print("PDF generation failed, but invoice data has been created")
        sys.exit(1)
    
    print("\\n=== Invoice Process Completed Successfully ===")
    print("PDF invoices are available in the 'invoices' directory")

if __name__ == "__main__":
    main()
""")
    
    # Make the script executable
    os.chmod('generate_invoices.py', 0o755)
    print("✅ Created generate_invoices.py - a master script to run the entire process")

if __name__ == "__main__":
    create_backup()
    create_master_script()