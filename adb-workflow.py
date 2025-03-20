import os
import json
import pandas as pd
from datetime import datetime
import logging
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
import schedule
import time
from adb_scraper import ADBScraper

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("adb_workflow.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("ADB Workflow")

# Configuration
CONFIG_FILE = "config.json"

def load_config():
    """Load configuration from JSON file"""
    try:
        with open(CONFIG_FILE, 'r') as f:
            config = json.load(f)
        return config
    except Exception as e:
        logger.error(f"Error loading config: {str(e)}")
        # Return default config
        return {
            "email": {
                "enabled": False,
                "smtp_server": "smtp.gmail.com",
                "smtp_port": 587,
                "sender_email": "your-email@gmail.com",
                "sender_password": "your-app-password",
                "recipients": ["recipient@example.com"]
            },
            "search_filters": {
                "tenders": [
                    {
                        "name": "india_water_tenders",
                        "country": "India",
                        "status": "Active",
                        "sector": "Water and other urban infrastructure and services"
                    }
                ],
                "projects": [
                    {
                        "name": "india_transport_projects",
                        "country": "India",
                        "status": "Proposed",
                        "sector": "Transport"
                    }
                ]
            },
            "schedule": {
                "frequency": "daily",  # daily, weekly
                "time": "09:00"
            },
            "output_dir": "output"
        }

def send_email_notification(subject, body, attachments=None):
    """Send email notification with optional attachments"""
    config = load_config()
    email_config = config.get("email", {})
    
    if not email_config.get("enabled", False):
        logger.info("Email notifications are disabled")
        return
    
    try:
        # Create message
        msg = MIMEMultipart()
        msg['From'] = email_config.get("sender_email")
        msg['To'] = ", ".join(email_config.get("recipients", []))
        msg['Subject'] = subject
        
        # Add body
        msg.attach(MIMEText(body, 'plain'))
        
        # Add attachments
        if attachments:
            for file_path in attachments:
                if os.path.exists(file_path):
                    with open(file_path, 'rb') as file:
                        part = MIMEApplication(file.read(), Name=os.path.basename(file_path))
                        part['Content-Disposition'] = f'attachment; filename="{os.path.basename(file_path)}"'
                        msg.attach(part)
        
        # Connect to server and send
        server = smtplib.SMTP(email_config.get("smtp_server"), email_config.get("smtp_port", 587))
        server.starttls()
        server.login(email_config.get("sender_email"), email_config.get("sender_password"))
        server.send_message(msg)
        server.quit()
        
        logger.info(f"Email notification sent to {email_config.get('recipients')}")
    except Exception as e:
        logger.error(f"Error sending email notification: {str(e)}")

def compare_with_previous(current_df, previous_file):
    """Compare current results with previous to identify new items"""
    try:
        if os.path.exists(previous_file):
            previous_df = pd.read_csv(previous_file)
            
            # Get unique identifiers (using Title or other unique field)
            current_ids = set(current_df['Title'].values)
            previous_ids = set(previous_df['Title'].values)
            
            # Find new items
            new_ids = current_ids - previous_ids
            new_items = current_df[current_df['Title'].isin(new_ids)]
            
            return new_items
        else:
            # If no previous file, all are new
            return current_df
    except Exception as e:
        logger.error(f"Error comparing with previous results: {str(e)}")
        return pd.DataFrame()

def run_job():
    """Run the data fetching job"""
    config = load_config()
    output_dir = config.get("output_dir", "output")
    
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Get timestamp for filenames
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Initialize scraper
    scraper = ADBScraper(headless=True)
    
    try:
        notification_body = []
        attachments = []
        new_items_found = False
        
        # Process tender searches
        for tender_filter in config.get("search_filters", {}).get("tenders", []):
            filter_name = tender_filter.get("name", "unnamed")
            logger.info(f"Running tender search for {filter_name}")
            
            # Prepare filters
            filters = {
                'country': tender_filter.get("country"),
                'status': tender_filter.get("status"),
                'sector': tender_filter.get("sector")
            }
            
            # Search tenders
            results = scraper.search_tenders(filters)
            
            if not results.empty:
                # Save current results
                output_file = f"{output_dir}/tenders_{filter_name}_{timestamp}.csv"
                scraper.save_results_to_csv(results, output_file)
                attachments.append(output_file)
                
                # Compare with previous results
                previous_file = None
                for file in os.listdir(output_dir):
                    if file.startswith(f"tenders_{filter_name}_") and file.endswith(".csv") and file != os.path.basename(output_file):
                        file_timestamp = file.replace(f"tenders_{filter_name}_", "").replace(".csv", "")
                        if previous_file is None or file_timestamp > previous_file.replace(f"tenders_{filter_name}_", "").replace(".csv", ""):
                            previous_file = file
                
                if previous_file:
                    previous_file = os.path.join(output_dir, previous_file)
                    new_items = compare_with_previous(results, previous_file)
                    
                    if not new_items.empty:
                        new_items_found = True
                        notification_body.append(f"Found {len(new_items)} new tenders for {filter_name}:")
                        for _, row in new_items.iterrows():
                            notification_body.append(f"- {row['Title']}")
                        notification_body.append("")
                else:
                    new_items_found = True
                    notification_body.append(f"Found {len(results)} tenders for {filter_name} (first run)")
                    notification_body.append("")
        
        # Process project searches
        for project_filter in config.get("search_filters", {}).get("projects", []):
            filter_name = project_filter.get("name", "unnamed")
            logger.info(f"Running project search for {filter_name}")
            
            # Prepare filters
            filters = {
                'country': project_filter.get("country"),
                'status': project_filter.get("status"),
                'sector': project_filter.get("sector")
            }
            
            # Search projects
            results = scraper.search_projects(filters)
            
            if not results.empty:
                # Save current results
                output_file = f"{output_dir}/projects_{filter_name}_{timestamp}.csv"
                scraper.save_results_to_csv(results, output_file)
                attachments.append(output_file)
                
                # Compare with previous results (similar to tenders)
                previous_file = None
                for file in os.listdir(output_dir):
                    if file.startswith(f"projects_{filter_name}_") and file.endswith(".csv") and file != os.path.basename(output_file):
                        file_timestamp = file.replace(f"projects_{filter_name}_", "").replace(".csv", "")
                        if previous_file is None or file_timestamp > previous_file.replace(f"projects_{filter_name}_", "").replace(".csv", ""):
                            previous_file = file
                
                if previous_file:
                    previous_file = os.path.join(output_dir, previous_file)
                    new_items = compare_with_previous(results, previous_file)
                    
                    if not new_items.empty:
                        new_items_found = True
                        notification_body.append(f"Found {len(new_items)} new projects for {filter_name}:")
                        for _, row in new_items.iterrows():
                            notification_body.append(f"- {row['Title']}")
                        notification_body.append("")
                else:
                    new_items_found = True
                    notification_body.append(f"Found {len(results)} projects for {filter_name} (first run)")
                    notification_body.append("")
        
        # Send notification if new items found
        if new_items_found:
            send_email_notification(
                subject="ADB Monitoring Update - New Items Found",
                body="\n".join(notification_body),
                attachments=attachments
            )
        else:
            logger.info("No new items found")
        
    except Exception as e:
        logger.error(f"Error in job execution: {str(e)}")
        send_email_notification(
            subject="ADB Monitoring Error",
            body=f"An error occurred while running the ADB monitoring job:\n\n{str(e)}"
        )
    finally:
        # Clean up
        scraper.close()

def setup_schedule():
    """Set up the job schedule"""
    config = load_config()
    schedule_config = config.get("schedule", {})
    
    frequency = schedule_config.get("frequency", "daily")
    run_time = schedule_config.get("time", "09:00")
    
    if frequency == "daily":
        schedule.every().day.at(run_time).do(run_job)
        logger.info(f"Scheduled job to run daily at {run_time}")
    elif frequency == "weekly":
        schedule.every().monday.at(run_time).do(run_job)
        logger.info(f"Scheduled job to run weekly on Monday at {run_time}")
    else:
        logger.warning(f"Unknown frequency: {frequency}, defaulting to daily")
        schedule.every().day.at(run_time).do(run_job)

def main():
    """Main entry point"""
    logger.info("Starting ADB monitoring workflow")
    
    # Create default config if it doesn't exist
    if not os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE, 'w') as f:
            json.dump(load_config(), f, indent=2)
        logger.info(f"Created default config file: {CONFIG_FILE}")
    
    # Set up schedule
    setup_schedule()
    
    # Run job immediately once
    logger.info("Running initial job")
    run_job()
    
    # Keep the script running
    logger.info("Entering schedule loop")
    while True:
        schedule.run_pending()
        time.sleep(60)

if __name__ == "__main__":
    main()
