from airflow.decorators import dag, task
from airflow.operators.email import EmailOperator
from airflow.exceptions import AirflowException
import datetime
import pandas as pd
import logging
import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

# Import your pipeline functions
from extract_pipeline import fetch_main_page_url
from transformation_pipeline import transform_books_data

# Set up logger
logger = logging.getLogger(__name__)

# --- EMAIL CONFIG ---
SMTP_SERVER = 'mail.samyamsapkota.com.np'
SMTP_PORT = 587
SMTP_USERNAME = 'samyam@samyamsapkota.com.np'
SMTP_PASSWORD = ''  # <<<< ENTER YOUR PASSWORD HERE
EMAIL_FROM = 'samyam@samyamsapkota.com.np'
EMAIL_TO = 'samyamsapkota@gmail.com'


@dag(
    dag_id='books_etl_pipeline',
    description='ETL pipeline for scraping book data and sending email reports',
    schedule='*/10 * * * *',  # Every 10 minutes
    start_date=datetime.datetime(2025, 12, 9),
    catchup=False,
    tags=['webscraping', 'etl', 'books'],
    max_active_runs=1,  # Prevent concurrent runs
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'email': ['samyamsapkota@gmail.com'],
        'email_on_failure': True,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': datetime.timedelta(minutes=5),
        'execution_timeout': datetime.timedelta(minutes=30),  # Prevent hanging tasks
    },
)
def books_etl_pipeline():
    """
    Books ETL Pipeline DAG using TaskFlow API
    """
    
    @task()
    def extract_books():
        """
        Task to extract book data from the website
        """
        try:
            logger.info("Starting extraction process...")
            # Scrape 2 pages (40 books) - adjust as needed
            no_of_pages = 2
            fetch_main_page_url(no_of_pages)
            
            # Verify the CSV was created
            csv_path = 'books.csv'
            if not os.path.exists(csv_path):
                raise AirflowException(f"Expected file {csv_path} was not created during extraction")
            
            logger.info("Extraction completed successfully!")
            return csv_path
        except Exception as e:
            logger.error(f"Extraction failed: {str(e)}")
            raise AirflowException(f"Failed to extract books data: {str(e)}")

    @task()
    def transform_books(csv_file: str):
        """
        Task to transform the extracted data
        """
        try:
            logger.info("Starting transformation process...")
            
            # Verify input file exists
            if not os.path.exists(csv_file):
                raise AirflowException(f"Input file {csv_file} not found")
            
            # Run transformation
            result = transform_books_data(csv_file)
            
            if result is None or len(result) != 6:
                raise AirflowException("Transformation did not return expected results")
                
            df_cleaned, dim_book, dim_category, dim_price_tier, dim_stock_tier, fact_inventory = result
            
            # Verify data quality
            if df_cleaned.empty:
                raise AirflowException("Transformation resulted in empty dataset")
            
            logger.info("Transformation completed successfully!")
            
            # Return summary stats for email
            summary = {
                'total_books': int(len(df_cleaned)),
                'total_categories': int(len(dim_category)),
                'total_inventory_value': f"${float(fact_inventory['Inventory Value'].sum()):.2f}",
                'avg_rating': f"{float(fact_inventory['Rating'].mean()):.2f}",
                'books_in_stock': int(fact_inventory['In_Stock_Binary'].sum()),
            }
            return summary
        except Exception as e:
            logger.error(f"Transformation failed: {str(e)}")
            raise AirflowException(f"Failed to transform books data: {str(e)}")

    @task()
    def prepare_email(summary: dict):
        """
        Prepare email content with pipeline results
        """
        try:
            if not summary or not isinstance(summary, dict):
                raise AirflowException("Invalid summary data received")
            
            # Validate required keys
            required_keys = ['total_books', 'total_categories', 'total_inventory_value', 'avg_rating', 'books_in_stock']
            missing_keys = [key for key in required_keys if key not in summary]
            if missing_keys:
                raise AirflowException(f"Missing required summary keys: {missing_keys}")
            
            email_body = f"""
            <html>
            <head>
                <style>
                    body {{ font-family: Arial, sans-serif; }}
                    .header {{ background-color: #4CAF50; color: white; padding: 10px; text-align: center; }}
                    .content {{ padding: 20px; }}
                    .stats {{ background-color: #f2f2f2; padding: 15px; border-radius: 5px; }}
                    .stat-item {{ margin: 10px 0; }}
                    .label {{ font-weight: bold; color: #333; }}
                    .value {{ color: #4CAF50; font-size: 18px; }}
                </style>
            </head>
            <body>
                <div class="header">
                    <h2>📚 Books ETL Pipeline Report</h2>
                </div>
                <div class="content">
                    <p>Hello,</p>
                    <p>The ETL pipeline has completed successfully. Here's a summary of the results:</p>
                    
                    <div class="stats">
                        <div class="stat-item">
                            <span class="label">Total Books Processed:</span>
                            <span class="value">{summary['total_books']}</span>
                        </div>
                        <div class="stat-item">
                            <span class="label">Total Categories:</span>
                            <span class="value">{summary['total_categories']}</span>
                        </div>
                        <div class="stat-item">
                            <span class="label">Total Inventory Value:</span>
                            <span class="value">{summary['total_inventory_value']}</span>
                        </div>
                        <div class="stat-item">
                            <span class="label">Average Rating:</span>
                            <span class="value">{summary['avg_rating']} / 5.0</span>
                        </div>
                        <div class="stat-item">
                            <span class="label">Books Currently in Stock:</span>
                            <span class="value">{summary['books_in_stock']}</span>
                        </div>
                    </div>
                    
                    <p style="margin-top: 20px;">
                        <strong>Generated Files:</strong><br>
                        • books_cleaned.csv<br>
                        • dim_book.csv<br>
                        • dim_category.csv<br>
                        • dim_price_tier.csv<br>
                        • dim_stock_tier.csv<br>
                        • fact_inventory.csv
                    </p>
                    
                    <p style="margin-top: 20px; color: #666;">
                        Pipeline executed at: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
                    </p>
                </div>
            </body>
            </html>
            """
            
            logger.info("Email content prepared successfully")
            return email_body
        except Exception as e:
            logger.error(f"Failed to prepare email: {str(e)}")
            raise AirflowException(f"Failed to prepare email content: {str(e)}")

    @task()
    def send_email_task(email_content: str):
        """
        Send email using SMTP directly
        """
        try:
            if not SMTP_PASSWORD:
                raise AirflowException("SMTP_PASSWORD is not set. Please update SMTP_PASSWORD in the DAG file.")
            
            logger.info(f"Preparing to send email to {EMAIL_TO}")
            
            # Create message
            msg = MIMEMultipart('alternative')
            msg['Subject'] = f'Books ETL Pipeline - Execution Report - {datetime.datetime.now().strftime("%Y-%m-%d")}'
            msg['From'] = EMAIL_FROM
            msg['To'] = EMAIL_TO
            
            # Attach HTML content
            html_part = MIMEText(email_content, 'html')
            msg.attach(html_part)
            
            # Connect to SMTP and send
            with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
                server.starttls()
                logger.info(f"Connecting to SMTP server: {SMTP_SERVER}:{SMTP_PORT}")
                server.login(SMTP_USERNAME, SMTP_PASSWORD)
                logger.info("SMTP login successful")
                server.send_message(msg)
                logger.info(f"Email sent successfully to {EMAIL_TO}")
            
            return f"Email sent to {EMAIL_TO}"
        except Exception as e:
            logger.error(f"Failed to send email: {str(e)}")
            raise AirflowException(f"Failed to send email: {str(e)}")

    # Define tasks
    csv_file = extract_books()
    summary = transform_books(csv_file)
    email_content = prepare_email(summary)
    email_result = send_email_task(email_content)


# Instantiate the DAG
books_etl_pipeline()
