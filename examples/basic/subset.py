import duckdb
import os

def split_geoparquet_by_column_duckdb(input_file, column_name, output_dir=None):
    """
    Split a GeoParquet file into multiple files based on unique values in a specified column using DuckDB.
    
    Parameters:
    -----------
    input_file : str
        Path to the input GeoParquet file
    column_name : str
        Name of the column to split by
    output_dir : str, optional
        Directory to save output files (defaults to current directory)
    
    Returns:
    --------
    list
        List of created output files
    """
    # Set default output directory if none provided
    if output_dir is None:
        output_dir = os.getcwd()
    
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Initialize DuckDB connection
    con = duckdb.connect(database=':memory:')
    
    # Load spatial extension for GeoParquet support
    con.execute("INSTALL spatial; LOAD spatial;")
    
    # Register the GeoParquet file
    print(f"Reading {input_file}...")
    con.execute(f"CREATE VIEW source_data AS SELECT * FROM read_parquet('{input_file}')")
    
    # Get unique values in the specified column
    unique_values = con.execute(f"SELECT DISTINCT \"{column_name}\" FROM source_data").fetchall()
    unique_values = [val[0] for val in unique_values]
    print(f"Found {len(unique_values)} unique values in column '{column_name}'")
    
    # List to store output paths
    output_files = []
    
    # Split the data by unique values and save to separate files
    for value in unique_values:
        # Create a valid filename (replace any invalid characters)
        safe_value = str(value).replace('/', '_').replace('\\', '_').replace(':', '_')
        
        # Output file path
        output_file = os.path.join(output_dir, f"{safe_value}.parquet")
        
        # Use parameterized query to handle special characters and different data types
        query = f"""
        COPY (
            SELECT * FROM source_data 
            WHERE "{column_name}" = ?
        ) TO '{output_file}' (FORMAT 'PARQUET')
        """
        
        # Execute the query with parameter binding
        con.execute(query, [value])
        
        # Get row count for reporting
        row_count = con.execute(f"SELECT COUNT(*) FROM source_data WHERE \"{column_name}\" = ?", [value]).fetchone()[0]
        
        # Store the output file
        output_files.append(output_file)
        
        print(f"Saved {row_count} rows with {column_name}='{value}' to {output_file}")
    
    # Close the connection
    con.close()
    
    return output_files

# Example usage
if __name__ == "__main__":
    # Replace with your actual file path
    input_file = "/Users/raymondyee/Data/iSample/2025_02_20_10_30_49/isamples_export_2025_02_20_10_30_49_geo.parquet"
    
    # Split by source_collection column
    output_files = split_geoparquet_by_column_duckdb(
        input_file=input_file,
        column_name="source_collection",
        output_dir="/Users/raymondyee/Data/iSample/"
    )
    
    print(f"\nSplit complete. Created {len(output_files)} files.")