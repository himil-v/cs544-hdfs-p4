import grpc
from concurrent import futures
import time
import os
import requests # Added for WebHDFS REST API calls

import lender_pb2
import lender_pb2_grpc

from sqlalchemy import create_engine, text
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import mysql.connector # Needed for the mysql+mysqlconnector driver to be registered
import pyarrow.fs # Explicitly import fs for FileSystem and FileType

# HDFS WebHDFS port (different from HDFS client port 9870)
_WEBHDFS_PORT = int(os.getenv("WEBHDFS_PORT", 9870))

_ONE_DAY_IN_SECONDS = 60 * 60 * 24

# Constants for database connection retries
_MAX_DB_CONNECT_RETRIES = 10
_DB_CONNECT_RETRY_DELAY_SECONDS = 6

# Configuration for MySQL (defaults based on user's prompt and Dockerfile.mysql)
_MYSQL_HOST = os.getenv("MYSQL_HOST", "mysql") # Default to 'mysql' service name for Docker Compose
_MYSQL_PORT = int(os.getenv("MYSQL_PORT", 3306))
_MYSQL_USER = os.getenv("MYSQL_USER", "root")
_MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "abc")
_MYSQL_DB = os.getenv("MYSQL_DB", "CS544") # Based on context from prompt
# _MYSQL_TABLE is removed as table name will be discovered dynamically

# Configuration for HDFS (defaults based on Dockerfile.namenode, using service name 'namenode')
_HDFS_NAMENODE_HOST = os.getenv("HDFS_NAMENODE_HOST", "boss") # Default to 'namenode' service name for Docker Compose
_HDFS_NAMENODE_PORT = int(os.getenv("HDFS_NAMENODE_PORT", 9000))

# Configure JVM memory for PyArrow's HDFS client
_JAVA_MAX_MEM_GB = int(os.getenv("JAVA_MAX_MEM_GB", 4)) # Default to 4GB
os.environ["_JAVA_OPTIONS"] = f"-Xmx{_JAVA_MAX_MEM_GB}g"
print(f"Set _JAVA_OPTIONS: {os.environ['_JAVA_OPTIONS']}")

class LenderServicer(lender_pb2_grpc.LenderServicer):
    def __init__(self):
        # Initialize HDFS client once per servicer instance
        print(f"Initializing HDFS client at {_HDFS_NAMENODE_HOST}:{_HDFS_NAMENODE_PORT}")
        self.fs = pa.fs.HadoopFileSystem(_HDFS_NAMENODE_HOST, _HDFS_NAMENODE_PORT, default_block_size=1024*1024, replication=2)
        self.fs2 = pa.fs.HadoopFileSystem(_HDFS_NAMENODE_HOST, _HDFS_NAMENODE_PORT, replication=1)

    def DbToHdfs(self, request, context):
        print("Received DbToHdfs request.")
        try:
            # Connect to SQL server
            db_connection_str = (
                f"mysql+mysqlconnector://{_MYSQL_USER}:{_MYSQL_PASSWORD}"
                f"@{_MYSQL_HOST}:{_MYSQL_PORT}/{_MYSQL_DB}"
            )
            
            engine = None
            for i in range(_MAX_DB_CONNECT_RETRIES):
                try:
                    print(f"Attempting to connect to MySQL (attempt {i+1}/{_MAX_DB_CONNECT_RETRIES}): {db_connection_str}")
                    engine = create_engine(db_connection_str)
                    # Test connection immediately
                    with engine.connect() as conn:
                        conn.execute(text("SELECT 1"))
                    print("Successfully connected to MySQL.")
                    break # Connection successful, exit retry loop
                except Exception as e:
                    print(f"Failed to connect to MySQL: {e}")
                    if i < _MAX_DB_CONNECT_RETRIES - 1:
                        print(f"Retrying in {_DB_CONNECT_RETRY_DELAY_SECONDS} seconds...")
                        time.sleep(_DB_CONNECT_RETRY_DELAY_SECONDS)
                    else:
                        raise # All retries failed, re-raise the last exception
            
            if engine is None:
                raise Exception("Failed to establish a database connection after multiple retries.")

            with engine.connect() as conn:
                
                # Query data with filtering
                query = text(f"""
                SELECT
                    loans.*,
                    loan_types.loan_type_name
                FROM
                (SELECT
                    *
                FROM
                    loans
                WHERE
                    loan_amount BETWEEN 30000 AND 800000) AS loans
                INNER JOIN
                (SELECT
                    *
                FROM
                    loan_types) AS loan_types
                ON
                    loans.loan_type_id = loan_types.id;
                """)
                df = pd.read_sql(query, conn)
            
            if df.empty:
                print("No data fetched from MySQL after filtering. Nothing to upload to HDFS.")
                return lender_pb2.StatusString(status="Success: No data matching criteria to upload.")

            # Upload to HDFS
            print(f"Using HDFS client from servicer: {_HDFS_NAMENODE_HOST}:{_HDFS_NAMENODE_PORT}")
            
            table = pa.Table.from_pandas(df)
            
            hdfs_path = "/hdma-wi-2021.parquet"
            print(f"Writing Parquet file to HDFS: {hdfs_path} with replication=2, blocksize=1MB")
            
            with self.fs.open_output_stream(hdfs_path) as writer: # Use self.fs
                 pq.write_table(table, writer)

            print("Successfully uploaded data to HDFS.")
            return lender_pb2.StatusString(status="Successfully uploaded data to HDFS.")
        except Exception as e:
            error_message = f"Failed to upload data to HDFS: {str(e)}"
            print(f"Error: {error_message}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(error_message)
            return lender_pb2.StatusString(status=f"Failed: {error_message}")

    def BlockLocations(self, request, context):
        print(f"Received BlockLocations request for path: {request.path}")
        block_counts = {}
        error_message = ""
        try:
            # Construct WebHDFS URL for GETFILEBLOCKLOCATIONS operation
            webhdfs_url = (
                f"http://{_HDFS_NAMENODE_HOST}:{_WEBHDFS_PORT}/webhdfs/v1"
                f"{request.path}?op=GETFILEBLOCKLOCATIONS"
            )
            print(f"Fetching block locations from WebHDFS: {webhdfs_url}")

            response = requests.get(webhdfs_url)
            response.raise_for_status() # Raise an exception for HTTP errors

            data = response.json()
            
            if "BlockLocations" not in data:
                raise Exception("Unexpected response format from WebHDFS: Missing 'BlockLocations' key.")
            
            block_locations_data = data["BlockLocations"]
            print(block_locations_data)

            for block_entry in block_locations_data["BlockLocation"]:
                for hostname in block_entry["hosts"]:
                    block_counts[hostname] = block_counts.get(hostname, 0) + 1
            
            print(f"Block locations fetched: {block_counts}")
            return lender_pb2.BlockLocationsResp(block_entries=block_counts)

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                error_message = f"File not found on HDFS: {request.path}. Error: {e}"
            else:
                error_message = f"HTTP error fetching block locations: {e}. Response: {e.response.text}"
            print(f"Error: {error_message}")
            context.set_code(grpc.StatusCode.NOT_FOUND if e.response.status_code == 404 else grpc.StatusCode.INTERNAL)
            context.set_details(error_message)
            return lender_pb2.BlockLocationsResp(block_entries={}, error=error_message)
        except Exception as e:
            error_message = f"Failed to get block locations for {request.path}: {str(e)}"
            print(f"Error: {error_message}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(error_message)
            return lender_pb2.BlockLocationsResp(block_entries={}, error=error_message)

    def _read_and_filter_main_file(self, main_hdfs_path, county_code):
        """Reads from the main Parquet file and filters by county code."""
        print(f"Reading from main dataset: {main_hdfs_path} with filter county_code = {county_code}")
        # Use self.fs2 for reading with 1x replication assumption, matching existing logic
        table = pq.read_table(main_hdfs_path, filesystem=self.fs2,
                              filters=[('county_code', '=', county_code)])
        return table

    def _write_partition_file(self, partition_dir, partition_hdfs_path, table):
        """Creates the partition directory if needed and writes the given PyArrow table to the specified partition path."""
        print(f"Creating directory: {partition_dir}")
        self.fs2.create_dir(partition_dir, recursive=True) # Ensure directory exists
        
        print(f"Writing filtered table to {partition_hdfs_path}")
        # Use self.fs2 for writing partition files with 1x replication assumption, matching existing logic
        with self.fs2.open_output_stream(partition_hdfs_path) as writer:
            pq.write_table(table, writer)

    def CalcAvgLoan(self, request, context):
        print(f"Received CalcAvgLoan request for county code: {request.county_code}")
        county_code = request.county_code
        main_hdfs_path = "/hdma-wi-2021.parquet"
        partition_dir = "/partitions"
        partition_hdfs_path = f"{partition_dir}/{county_code}.parquet"

        source = "reuse" # Default source
        table = None # Initialize table to None

        try:
            try:    
                # Attempt to read the county-specific file
                print(f"Checking for existing partition file: {partition_hdfs_path}")
                table = pq.read_table(partition_hdfs_path, filesystem=self.fs2)
                print(f"Found existing partition file for county {county_code}, reusing it.")
                source = "reuse" 
            except FileNotFoundError:
                # File does not exist, create it from the main dataset
                print(f"Partition file not found for county {county_code}. Creating new one.")
                source = "create"
                table = self._read_and_filter_main_file(main_hdfs_path, county_code)
                
                if table.num_rows == 0:
                    msg = f"No loans found for county code {county_code} in {main_hdfs_path}. Not creating partition file."
                    print(msg)
                    return lender_pb2.CalcAvgLoanResp(avg_loan=0, source=source, error=msg)
                
                self._write_partition_file(partition_dir, partition_hdfs_path, table)
            except OSError as os_error:
                # File exists but is corrupted or lost due to DataNode failure, recreate it
                print(f"OSError when reading partition file {partition_hdfs_path}: {os_error}. Recreating from main dataset.")
                source = "recreate"
                table = self._read_and_filter_main_file(main_hdfs_path, county_code)

                if table.num_rows == 0:
                    msg = f"No loans found for county code {county_code} in {main_hdfs_path}. Not recreating partition file."
                    print(msg)
                    return lender_pb2.CalcAvgLoanResp(avg_loan=0, source=source, error=msg)
                
                # Recreate the corrupted/lost partition file
                self._write_partition_file(partition_dir, partition_hdfs_path, table)

            # Compute average loan amount after table is obtained
            if table is None or table.num_rows == 0:
                msg = f"No data found for county code {county_code} to compute average loan."
                print(msg)
                return lender_pb2.CalcAvgLoanResp(avg_loan=0, source=source, error=msg)

            if "loan_amount" not in table.column_names:
                msg = f"'loan_amount' column missing in dataset for county {county_code}."
                print(msg)
                return lender_pb2.CalcAvgLoanResp(avg_loan=0, source=source, error=msg)

            loan_amounts = table["loan_amount"].to_pandas()
            if loan_amounts.empty:
                msg = f"No loan_amount data for county {county_code}."
                print(msg)
                return lender_pb2.CalcAvgLoanResp(avg_loan=0, source=source, error=msg)

            avg_loan = int(loan_amounts.mean())  # round down to nearest integer
            print(f"Calculated average loan for county {county_code}: {avg_loan}, source={source}")

            return lender_pb2.CalcAvgLoanResp(avg_loan=avg_loan, source=source)

        except Exception as e:
            error_message = f"Failed to calculate average loan for county code {county_code}: {str(e)}"
            print(f"Error: {error_message}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(error_message)
            return lender_pb2.CalcAvgLoanResp(avg_loan=0, source=source, error=error_message)

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    lender_pb2_grpc.add_LenderServicer_to_server(LenderServicer(), server)
    server.add_insecure_port("[::]:5000") # Server listens on all interfaces on port 5000
    server.start()
    print("Server started on port 5000")
    try:
        while True:
            time.sleep(_ONE_DAY_IN_SECONDS)
    except KeyboardInterrupt:
        print("Server stopping.")
        server.stop(0)

if __name__ == "__main__":
    serve()
