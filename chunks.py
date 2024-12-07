import pandas as pd
import os
# Define file paths
input_file = "All_external.csv"
output_file = "reduced_news_dataset.csv"  # File to save the reduced dataset
target_size_gb = 1.5  # Target dataset size in GB

# Load the dataset in chunks to calculate approximate memory usage
sample_chunk = pd.read_csv(input_file, nrows=1000)
row_memory = sample_chunk.memory_usage(deep=True).sum() / 1000  # Approximate memory per row in bytes

# Calculate the number of rows to extract to reach the target size
rows_to_extract = int((target_size_gb * 1e9) / row_memory)
print(f"Approximate rows required for {target_size_gb} GB: {rows_to_extract}")

# Load only the required number of rows
reduced_data = pd.read_csv(input_file, nrows=rows_to_extract)

# Save the reduced dataset
reduced_data.to_csv(output_file, index=False)
print(reduced_data.head())  # Preview the first few rows
print(f"Shape of reduced data: {reduced_data.shape}")

print(f"Reduced dataset saved to {output_file}")