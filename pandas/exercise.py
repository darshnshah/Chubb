#create a pandas dataframe from csv sample.csv
#filter and subset rows/columns (sales from a store, date ranges, high value transactions)
#compute descriptive statistics (mean, median, sum of sales, total by store, average basket)
#perform simple cleaning (handle missing values, remove duplicates)
#build mini etl pipeline (extract from csv, transform data, output json)

import pandas as pd
import json 
def load_data(file_path):
    df = pd.read_csv(file_path)
    df['date'] = pd.to_datetime(df['date'])
    return df

def filter_by_store(df, store_name):
    return df[df['store_name'] == store_name]

def filter_by_date_range(df, start_date, end_date):
    return df[(df['date'] >= start_date) & (df['date'] <= end_date)]

def filter_high_value_transactions(df, threshold):
    return df[df['total'] > threshold]

def compute_statistics(df):
    stats = {
        'mean_sales': df['total'].mean(),
        'median_sales': df['total'].median(),
        'sum_sales': df['total'].sum(),
        'total_by_store': df.groupby('store_name')['total'].sum().to_dict(),
        'average_basket': df['total'].mean()
    }
    return stats

def clean_data(df):
    df = df.copy()  # Create a copy to avoid SettingWithCopyWarning
    df = df.drop_duplicates()
    df['total'] = df['total'].fillna(df['total'].mean())
    df['customer_id'] = df['customer_id'].fillna('Unknown')
    df['promo_code'] = df['promo_code'].fillna('No Code')
    return df

def etl_pipeline(input_file, output_file):
    print("Loading data...")
    df = load_data(input_file)
    
    print("Transforming data...")
    df = clean_data(df)
    stats = compute_statistics(df)
    
    # Add calculated fields
    df['revenue_per_item'] = df['total'] / df['quantity']
    df['month'] = df['date'].dt.month
    df['day_of_week'] = df['date'].dt.day_name()
    
    print("Saving transformed data...")
    resultdf = df.to_dict(orient='records')
    with open(output_file, 'w') as f:
        json.dump(resultdf, f, indent=4, default=str)  # Convert non-serializable objects (like Timestamp) to strings
    
    print("ETL pipeline completed.")
    return df

if __name__ == "__main__":
    # Load data
    df = load_data('pandas/sample.csv')
    print("Original Data:")
    print(df.head())
    print(f"\nShape: {df.shape}")
    
    # Filter examples
    print("\n" + "="*50)
    print("FILTERING EXAMPLES")
    print("="*50)
    
    downtown_sales = filter_by_store(df, 'Downtown')
    print(f"\nDowntown store sales: {len(downtown_sales)} transactions")
    print(downtown_sales)
    
    date_range = filter_by_date_range(df, '2025-10-02', '2025-10-04')
    print(f"\nSales from 2025-10-02 to 2025-10-04: {len(date_range)} transactions")
    print(date_range)
    
    high_value = filter_high_value_transactions(df, 5.0)
    print(f"\nHigh value transactions (>$5.00): {len(high_value)} transactions")
    print(high_value)
    
    # Statistics
    print("\n" + "="*50)
    print("DESCRIPTIVE STATISTICS")
    print("="*50)
    stats = compute_statistics(df)
    print(f"\nMean Sales: ${stats['mean_sales']:.2f}")
    print(f"Median Sales: ${stats['median_sales']:.2f}")
    print(f"Total Sales: ${stats['sum_sales']:.2f}")
    print("\nTotal by Store:")
    for store, total in stats['total_by_store'].items():
        print(f"  {store}: ${total:.2f}")
    print(f"\nAverage Basket: ${stats['average_basket']:.2f}")
    
    # Data Cleaning
    print("\n" + "="*50)
    print("DATA CLEANING")
    print("="*50)
    df_cleaned = clean_data(df)
    print("\nCleaned Data Info:")
    print(df_cleaned.info())
    
    # ETL Pipeline
    print("\n" + "="*50)
    print("MINI ETL PIPELINE")
    print("="*50)
    df_final = etl_pipeline('pandas/sample.csv', 'pandas/output.json')
    print("\nFinal transformed data sample:")
    print(df_final)