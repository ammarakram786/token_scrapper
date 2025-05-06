import pandas as pd
import subprocess

# Read input CSV
df = pd.read_csv('input.csv')
if 'ID' not in df.columns:
    df['ID'] = [f'row_{i + 1}' for i in range(len(df))]
df.to_csv('input_with_id.csv', index=False)

# Run Scrapy spider
subprocess.run(['scrapy', 'crawl', 'dextools', '-a', 'input_file=input_with_id.csv'])
