import pandas as pd
import sys

# Get the input and output file paths from command-line arguments
input_file = sys.argv[1]  # CSV file to clean
output_file = sys.argv[2]  # Cleaned CSV file output

# Load the CSV file
df = pd.read_csv(input_file)

# Cleaning steps
df_wun = df[~df['User_name'].isin(['Service'])]
df_wun = df_wun[~df_wun['Lab_name'].isin(['QC'])]
df_clean = df_wun.assign(Truelab_id=df_wun['Truelab_id'].apply(lambda x: x.partition('-')[0]))
df_clean['Chip_serial_no'] = df_clean['Chip_serial_no'].str[:2]
df_clean = df_clean.drop_duplicates(keep='first')
df_clean['Ct1'] = pd.to_numeric(df_clean['Ct1'], errors='coerce').fillna(0)
df_clean['Ct2'] = pd.to_numeric(df_clean['Ct2'], errors='coerce').fillna(0)
df_clean['Ct3'] = pd.to_numeric(df_clean['Ct3'], errors='coerce').fillna(0)
df_clean = df_clean[~df_clean['Chip_serial_no'].str[0].str.isdigit()]
df_clean = df_clean[df_clean['Chip_serial_no'].str[1].str.isdigit()]

# df_clean.head()

# Save the cleaned data
df_clean.to_csv(output_file, index=False)

print("Data cleaning completed and saved to", output_file)