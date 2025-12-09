import pandas as pd
import re


def clean_currency(x):
    """Remove currency symbols and convert to float"""
    if isinstance(x, str):
        # Remove 'Â£' and any other non-numeric characters except the decimal point
        cleaned = re.sub(r'[^\d.]', '', x)
        return float(cleaned)
    return x


def clean_description(text):
    """Clean description text by removing suffixes and fixing encoding"""
    if pd.isna(text):
        return ""
    
    # Remove the " ...more" suffix
    text = text.replace(' ...more', '')

    # handling ����tre like texts 
    fixed = text.encode("cp1252", errors="ignore").decode("utf-8", errors="ignore")
    
    return fixed


def transform_books_data(input_csv='books.csv'):
    """
    Main transformation pipeline for books data.
    
    Args:
        input_csv (str): Path to input CSV file. Default is 'books.csv'
    
    Returns:
        tuple: (df_cleaned, dim_book, dim_category, dim_price_tier, dim_stock_tier, fact_book_inventory)
    """
    
    # Load data
    df = pd.read_csv(input_csv)
    
    # Clean price columns
    price_cols = ['Price (excl. tax)', 'Price (incl. tax)', 'Tax']
    for col in price_cols:
        df[col] = df[col].apply(clean_currency)
    
    # Clean description
    df['Description'] = df['Description'].apply(clean_description)
    
    # Feature engineering
    df['Inventory Value'] = df['Price (excl. tax)'] * df['No_of_books_in_Stock']
    
    # Availability flag - convert to binary
    df['In_Stock_Binary'] = df['Is_in_Stock'].apply(lambda x: 1 if x == True else 0)
    df.drop(['Is_in_Stock'], axis=1, inplace=True)
    
    # Binning 1: Stock Levels (Custom Bins)
    stock_bins = [0, 10, 18, 100000]
    stock_labels = ['Critical', 'Low', 'Healthy']
    df['Stock_Bin'] = pd.cut(df['No_of_books_in_Stock'], bins=stock_bins, labels=stock_labels, right=False)
    
    # Binning 2: Price Tiers (Quantile Binning)
    df['Price_Tier'] = pd.qcut(df['Price (excl. tax)'], q=3, labels=['Budget', 'Standard', 'Premium'])
    
    # Save cleaned data
    df.to_csv('books_cleaned.csv', index=False)
    print("Data cleaned and saved to 'books_cleaned.csv'")
    
    # --- CREATE STAR SCHEMA ---
    
    # Dimension 1: Book
    dim_book = df[['Title', 'Description', 'UPC', 'Product Type', 'Image_link']].drop_duplicates().reset_index(drop=True)
    dim_book['book_id'] = dim_book.index + 1
    dim_book.to_csv('dim_book.csv', index=False)
    print(f"Created dim_book with {len(dim_book)} records")
    
    # Dimension 2: Category
    dim_category = df[['Category']].drop_duplicates().reset_index(drop=True)
    dim_category['category_id'] = dim_category.index + 1
    dim_category.to_csv('dim_category.csv', index=False)
    print(f"Created dim_category with {len(dim_category)} records")
    
    # Dimension 3: Price Tier
    dim_price_tier = df[['Price_Tier']].drop_duplicates().reset_index(drop=True)
    dim_price_tier['price_tier_id'] = dim_price_tier.index + 1
    dim_price_tier.to_csv('dim_price_tier.csv', index=False)
    print(f"Created dim_price_tier with {len(dim_price_tier)} records")
    
    # Dimension 4: Stock Bin
    dim_stock_tier = df[['Stock_Bin']].drop_duplicates().reset_index(drop=True)
    dim_stock_tier['stock_tier_id'] = dim_stock_tier.index + 1
    dim_stock_tier.to_csv('dim_stock_tier.csv', index=False)
    print(f"Created dim_stock_tier with {len(dim_stock_tier)} records")
    
    # Create fact table by joining all dimensions
    df_fact = df.merge(dim_book, on=['Title', 'Description', 'UPC', 'Product Type', 'Image_link']) \
                .merge(dim_category, on='Category') \
                .merge(dim_price_tier, on='Price_Tier') \
                .merge(dim_stock_tier, on='Stock_Bin')
    
    # Select fact table columns
    fact_book_inventory = df_fact[[
        'book_id',
        'category_id',
        'price_tier_id', 
        'stock_tier_id',
        'Rating',                 
        'Price (excl. tax)',
        'Price (incl. tax)',
        'Tax',
        'No_of_books_in_Stock',
        'Inventory Value',
        'Number of reviews',
        'In_Stock_Binary',         
    ]].copy()
    
    fact_book_inventory.to_csv('fact_inventory.csv', index=False)
    print(f"Created fact_book_inventory with {len(fact_book_inventory)} records")
    
    print("\n✓ Data Transformed successfully.")
    print("✓ Star schema created with 4 dimension tables and 1 fact table.")
    
    return df, dim_book, dim_category, dim_price_tier, dim_stock_tier, fact_book_inventory


if __name__ == '__main__':
    # Run the transformation pipeline
    df_cleaned, dim_book, dim_category, dim_price_tier, dim_stock_tier, fact_inventory = transform_books_data()
    
    print("\n--- Summary ---")
    print(f"Cleaned data shape: {df_cleaned.shape}")
    print(f"Fact table shape: {fact_inventory.shape}")
