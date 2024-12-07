import pandas as pd

# Load the large CSV file
file_path = 'nasdaq_exteral_data.csv'  # Update this with the path to your dataset
df = pd.read_csv(file_path)

# Remove the 'url' and 'author' columns
df_cleaned = df.drop(columns=['Url', 'Author'])

# Randomly sample 30,000 rows
df_sampled = df_cleaned.sample(n=30000, random_state=42)  # random_state for reproducibility

# Save the sampled dataset to a new file
df_sampled.to_csv(r'C:\Users\User\Desktop\Big Data\reduced_data.csv', index=False)

print(f"Sampled dataset saved with {df_sampled.shape[0]} rows.")

