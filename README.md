# rust-hive
--------------------------------

## Main
Executes the main ingestion process using multithreading.

This function performs the following steps:
1. Creates an in-memory DuckDB table.
2. Initiates population data updates for years 1993 to 2023 using multiple threads.
3. Waits for all update threads to complete.
4. Writes the collected data into Hive partitions.

## Returns

* `Result` which is:
    * `Ok(())` if the ingestion process completes successfully.
    * `Err(IngestionError)` if any step in the process fails, where `IngestionError`
    is a custom error type that encapsulates various potential error scenarios.