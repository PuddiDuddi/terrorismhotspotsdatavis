import dlt


from dlt.sources.filesystem import readers, read_csv



# used for extracting data from source, in this case a local csv file 
files = readers(bucket_url="filteredterroristdata.csv").read_csv()


pipeline = dlt.pipeline(
        pipeline_name="snowflakecsv",
        destination="snowflake",
        dataset_name="terrorismhotspots"  # schema
)



# print the data yielded from resource
print(files)

# run the pipeline with your parameters
load_info = pipeline.run(files.with_name('filteredterroristdata.csv'), table_name="terrorismhotspots")

    # pretty print the information on data that was loaded
print(load_info)