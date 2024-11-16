import snowflake.connector
import os
from datacleaning import clean_data
from loguru import logger
import warnings

warnings.filterwarnings(
    action="ignore", category=UserWarning, module="snowflake.connector"
)

# Configure logger with colors
logger.remove()  # Remove default handler
logger.add(
    sink=lambda msg: print(msg),
    format="<level>{message}</level>",
    colorize=True,
    level="INFO",
)
logger.add("pipelinelog.log", rotation="5 MB")


def execute_with_status(
    cursor: snowflake.connector.cursor.SnowflakeCursor, sql: str, description: str
) -> bool:
    """Execute SQL command and log status."""
    try:
        cursor.execute(sql).fetchall()
        logger.success(f"✓ {description}")
        return True
    except Exception as e:
        logger.error(f"✗ {description} - Failed: {str(e)}")
        return False


def load_data_to_snowflake(connection_name: str = "myconnection") -> None:
    """Main function to load data to Snowflake with error handling."""
    try:
        # Clean data and get file path
        clean_data()
        file_path = os.path.join(os.getcwd(), "filteredterroristdata.csv")
        logger.info("Starting Snowflake data load process...")

        # Connect to Snowflake
        with snowflake.connector.connect(connection_name=connection_name) as conn:
            with conn.cursor() as cur:
                # Dictionary of SQL commands and their descriptions
                setup_commands = [
                    ("USE ROLE DLT_LOADER_ROLE", "Setting role"),
                    ("USE WAREHOUSE TERRORISMHOTSPOTS", "Setting warehouse"),
                    ("USE DATABASE TERRORISMHOTSPOTS", "Setting database"),
                    ("USE SCHEMA TERRORISMHOTSPOTS", "Setting schema"),
                    (
                        """
                    CREATE OR REPLACE STAGE my_local_stage 
                    FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 0)
                    """,
                        "Creating stage",
                    ),
                    (f"PUT file://{file_path} @my_local_stage", "Uploading file"),
                    (
                        """
                    CREATE OR REPLACE FILE FORMAT csv
                    TYPE = CSV
                    FIELD_DELIMITER = ','
                    SKIP_HEADER = 1
                    NULL_IF = ('NULL', 'null') 
                    EMPTY_FIELD_AS_NULL = true
                    COMPRESSION = gzip
                    FIELD_OPTIONALLY_ENCLOSED_BY = '0x22'
                    """,
                        "Creating file format",
                    ),
                    (
                        """
                    CREATE OR REPLACE TABLE RAWINGEST (
                        eventid             number,
                        date               date,
                        country_txt        text,
                        region_txt         text,
                        provstate          text,
                        city               text,
                        latitude           float,
                        longitude          float,
                        success            number,
                        suicide            number,
                        attacktype1_txt    text,
                        targtype1_txt      text,
                        gname              text,
                        weapsubtype1_txt   text,
                        nkill              number,
                        nkillter           number,
                        nwound             number  
                    )
                    """,
                        "Creating table",
                    ),
                    (
                        """
                    COPY INTO RAWINGEST
                    FROM @my_local_stage/filteredterroristdata.csv
                    FILE_FORMAT = (FORMAT_NAME = 'csv')
                    """,
                        "Copying data",
                    ),
                ]

                # Execute all commands
                for sql, description in setup_commands:
                    if not execute_with_status(cur, sql, description):
                        logger.error("Process aborted due to error")
                        return

                logger.success("✨ All operations completed successfully!")

    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")


if __name__ == "__main__":
    load_data_to_snowflake()
