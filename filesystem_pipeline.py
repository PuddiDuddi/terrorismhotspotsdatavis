import snowflake.connector
import os
from datacleaning import clean_data

clean_data()
file_path = os.path.join(os.getcwd(), "filteredterroristdata.csv")
with snowflake.connector.connect(
    connection_name="myconnection",
) as conn:
    with conn.cursor() as cur:
        print(cur.execute("USE ROLE DLT_LOADER_ROLE").fetchall())
        print(cur.execute("USE WAREHOUSE TERRORISMHOTSPOTS").fetchall())
        print(cur.execute("USE DATABASE TERRORISMHOTSPOTS").fetchall())
        print(cur.execute("USE SCHEMA TERRORISMHOTSPOTS").fetchall())
        print(
            cur.execute("""
                CREATE OR REPLACE STAGE my_local_stage 
                FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 0);
                """).fetchall()
        )
        print(cur.execute(f"PUT file://{file_path} @my_local_stage").fetchall())
        print(
            cur.execute("""
                CREATE OR REPLACE FILE FORMAT csv
                TYPE = CSV
                FIELD_DELIMITER = ','
                SKIP_HEADER = 1
                NULL_IF = ('NULL', 'null') 
                EMPTY_FIELD_AS_NULL = true
                COMPRESSION = gzip
                FIELD_OPTIONALLY_ENCLOSED_BY = '0x22'
                """).fetchall()
        )
        print(
            cur.execute("""
                CREATE OR REPLACE TABLE RAWINGEST (
                eventid               number,
                date                 date,
                country_txt          text,
                region_txt           text,
                provstate            text,
                city               text,
                latitude            float,
                longitude           float,
                success               number,
                suicide               number,
                attacktype1_txt      text,
                targtype1_txt        text,
                gname                text,
                weapsubtype1_txt     text,
                nkill               number,
                nkillter              number,
                nwound              number  
                )
                """).fetchall()
        )
        print(
            cur.execute("""
                COPY INTO RAWINGEST
                FROM @my_local_stage/filteredterroristdata.csv
                FILE_FORMAT = (FORMAT_NAME = 'csv');
                """).fetchall()
        )
