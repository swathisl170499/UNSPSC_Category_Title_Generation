import os
import openai
import pandas as pd
import datetime
import time

# Your OpenAI API key
OPENAI_API_KEY = "Your Open AI key"

def query_unspsc(service_description, openai_api_key):
    openai.api_key = openai_api_key
    try:
        response = openai.Completion.create(
            engine="gpt-3.5-turbo-instruct",
            prompt=f"Only provide the UNSPSC category title for the service description: '{service_description}'.No additional information.",
            max_tokens=100
        )
        return response.choices[0].text.strip()
    except Exception as e:
        return "Error querying OpenAI"  # Returning a simple error message if the query fails

# Record the overall start time and timestamp
overall_start_time = time.time()
overall_start_timestamp = datetime.datetime.now()
print(f"Overall process started at {overall_start_timestamp}")

try:
    # Load service descriptions from an Excel file
    input_excel_filename = r"your excel file"
    df = pd.read_excel(input_excel_filename)

    # Convert the 'AI Category Title' column to string to prevent dtype issues
    df['AI Category Title'] = df['AI Category Title'].astype(str)

    # Define batch size and number of batches dynamically
    batch_size = 650
    num_batches = (len(df) + batch_size - 1) // batch_size

    # Output file
    output_csv_filename = "Service description with no match for AI category generation_Phase2_10SEP_1st.csv"

    # Process each batch
    for batch_num in range(num_batches):
        batch_start_time = time.time()
        batch_start_timestamp = datetime.datetime.now()

        start_index = batch_num * batch_size
        end_index = min(start_index + batch_size, len(df))
        df_batch = df.iloc[start_index:end_index]

        # Iterate and update DataFrame
        for index, row in df_batch.iterrows():
            service_description = row['Service Description in English']
            unspsc_info = query_unspsc(service_description, OPENAI_API_KEY)
            df.at[index, 'AI Category Title'] = unspsc_info if unspsc_info else "No UNSPSC Found"

        # Save updated DataFrame to the same CSV file
        with open(output_csv_filename, 'a' if os.path.exists(output_csv_filename) else 'w', newline='',
                  encoding='utf-8') as f:
            df_batch.to_csv(f, header=not f.tell(), index=False)

        batch_end_time = time.time()
        batch_end_timestamp = datetime.datetime.now()
        time_taken = batch_end_time - batch_start_time
        records_processed = end_index - start_index

        # Logging batch processing details
        print(f"Batch {batch_num + 1} processed.")
        print(f"Started at: {batch_start_timestamp}")
        print(f"Ended at: {batch_end_timestamp}")
        print(f"Time taken for batch: {time_taken:.2f} seconds")
        print(f"Records processed in this batch: {records_processed}")

except Exception as e:
    print("Failed to process Excel file:", e)

# Calculate overall processing time
overall_elapsed_time = time.time() - overall_start_time
print(f"Overall process completed in {overall_elapsed_time:.2f} seconds")
