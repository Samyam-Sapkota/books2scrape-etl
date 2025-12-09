from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import pandas as pd

# Import your pipeline functions
from extract_pipeline import fetch_main_page_url
from transformation_pipeline import transform_books_data


# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['your-email@example.com'],  # UPDATE THIS
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'books_etl_pipeline',
    default_args=default_args,
    description='ETL pipeline for scraping book data and sending email reports',
    schedule_interval='*/10 * * * *',  # Every 10 minutes
    start_date=datetime(2025, 12, 9),
    catchup=False,
    tags=['webscraping', 'etl', 'books'],
)


def extract_task(**context):
    """
    Task to extract book data from the website
    """
    print("Starting extraction process...")
    # Scrape 2 pages (40 books) - adjust as needed
    no_of_pages = 2
    fetch_main_page_url(no_of_pages)
    print("Extraction completed successfully!")
    
    # Push the file path to XCom for next task
    context['ti'].xcom_push(key='csv_file', value='books.csv')
    return 'books.csv'


def transform_task(**context):
    """
    Task to transform the extracted data
    """
    print("Starting transformation process...")
    # Get the CSV file from previous task
    csv_file = context['ti'].xcom_pull(key='csv_file', task_ids='extract_books')
    
    # Run transformation
    df_cleaned, dim_book, dim_category, dim_price_tier, dim_stock_tier, fact_inventory = transform_books_data(csv_file)
    
    print("Transformation completed successfully!")
    
    # Push summary stats to XCom for email
    summary = {
        'total_books': len(df_cleaned),
        'total_categories': len(dim_category),
        'total_inventory_value': f"${fact_inventory['Inventory Value'].sum():.2f}",
        'avg_rating': f"{fact_inventory['Rating'].mean():.2f}",
        'books_in_stock': fact_inventory['In_Stock_Binary'].sum(),
    }
    context['ti'].xcom_push(key='summary', value=summary)
    return summary


def prepare_email_content(**context):
    """
    Prepare email content with pipeline results
    """
    summary = context['ti'].xcom_pull(key='summary', task_ids='transform_books')
    
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
                Pipeline executed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
            </p>
        </div>
    </body>
    </html>
    """
    
    return email_body


# Task 1: Extract data from website
extract_books = PythonOperator(
    task_id='extract_books',
    python_callable=extract_task,
    dag=dag,
)

# Task 2: Transform the extracted data
transform_books = PythonOperator(
    task_id='transform_books',
    python_callable=transform_task,
    dag=dag,
)

# Task 3: Prepare email content
prepare_email = PythonOperator(
    task_id='prepare_email',
    python_callable=prepare_email_content,
    dag=dag,
)

# Task 4: Send email notification
send_email = EmailOperator(
    task_id='send_email_report',
    to=['recipient@example.com'],  # UPDATE THIS
    subject='Books ETL Pipeline - Execution Report - {{ ds }}',
    html_content="{{ task_instance.xcom_pull(task_ids='prepare_email') }}",
    dag=dag,
)

# Define task dependencies
extract_books >> transform_books >> prepare_email >> send_email
