import sys
import pandas as pd

print('arguments', sys.argv)

month = int(sys.argv[1])

df = pd.DataFrame({"Day": [1, 2], "Number of Passengers": [3, 4]})
df['month'] = month
print(df.head())

output_file = f"output_{month}.parquet"
df.to_parquet(output_file)
print(f"Parquet file created: {output_file}")

print(f'hello pipeline, month={month}')