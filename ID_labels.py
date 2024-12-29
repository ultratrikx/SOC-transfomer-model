import csv
import uuid

def add_unique_id_to_csv(input_file: str, output_file: str, id_column_name: str = 'Unique_ID'):
    """
    Adds a new column with unique IDs to a CSV file.

    Args:
        input_file (str): Path to the input CSV file.
        output_file (str): Path to save the output CSV file.
        id_column_name (str): Name of the new column for unique IDs.
    """
    try:
        with open(input_file, 'r', newline='') as infile:
            reader = csv.reader(infile)
            header = next(reader)
            
            # Add the unique ID column to the header
            if id_column_name in header:
                raise ValueError(f"Column '{id_column_name}' already exists in the CSV file.")
            header.append(id_column_name)

            with open(output_file, 'w', newline='') as outfile:
                writer = csv.writer(outfile)
                writer.writerow(header)  # Write updated header

                for row in reader:
                    row.append(str(uuid.uuid4())[:8])  # Append shorter unique ID
                    writer.writerow(row)
        
        print(f"Successfully added '{id_column_name}' to {output_file}")

    except FileNotFoundError:
        print(f"Error: The file '{input_file}' does not exist.")
    except Exception as e:
        print(f"An error occurred: {e}")


# Example Usage
if __name__ == '__main__':
    input_csv = './data/csv/peatlands.csv'  # Replace with your input CSV file path
    output_csv = './data/csv/peatlands_id.csv'  # Replace with your desired output CSV file path
    add_unique_id_to_csv(input_csv, output_csv)
