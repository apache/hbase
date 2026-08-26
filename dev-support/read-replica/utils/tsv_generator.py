import argparse
import os
import random

NUM_COLUMNS = 10


def generate_data(row_key):
    columns = [str(random.randint(1, 100)) for _ in range(NUM_COLUMNS)]
    return f"{row_key}\t" + "\t".join(columns) + "\n"


def main(output_dir, num_rows, initial_row_value):
    tsv_file = os.path.join(output_dir, "output.tsv")

    rows_written = 0
    with open(tsv_file, "w") as f:
        for i in range(initial_row_value, num_rows+initial_row_value):
            row_key = f"row{i}"
            f.write(generate_data(row_key))
            rows_written += 1

    print(f"TSV file generated at {tsv_file} with {rows_written} rows and {NUM_COLUMNS} columns.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate a TSV file with random data for HBase bulk loading.")
    parser.add_argument("output_directory", help="Directory to write the output TSV file")
    parser.add_argument("-n", "--num-rows", type=int, default=500, help="Number of rows to generate (default: 500)")
    parser.add_argument("-i", "--initial-row-value", type=int, default=0, help="Starting row number (default: 0)")
    args = parser.parse_args()

    main(args.output_directory, args.num_rows, args.initial_row_value)
