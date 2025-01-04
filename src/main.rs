mod databases;
mod parsers;
use databases::duckdb_functions::{
    create_duck_db_table, generate_insert_sql_given_row_struct, write_into_hive_partition,
};
use duckdb::{Connection, Error as DuckDBError, Result};

use reqwest::Error as RequestwestError;
use rust_hive::parsers::population::PopulationRow;
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use thiserror::Error;

// Custom error handling
#[derive(Error, Debug)]
enum IngestionError {
    #[error("Error connecting to DuckDB: {0}")]
    DuckDB(#[from] DuckDBError),
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Requestwest error: {0}")]
    Requestwest(#[from] RequestwestError),
    #[error("Parse error: {0}")]
    Parse(String),
}

/// Converts a Gregorian year to a Thai year.
///
/// This function takes an integer representing a Gregorian year and returns the corresponding Thai year.
/// The conversion is based on the Thai calendar system, which starts from the year 2500.
///
/// # Parameters
///
/// * `year` - An integer representing the Gregorian year.
///
/// # Returns
///
/// An integer representing the corresponding Thai year in short form.
fn convert_to_thai_year(year: i32) -> i32 {
    year + 543 - 2500
}

/// Retrieves statistical data for a given year from a specific URL.
///
/// This function converts the input Gregorian year to a Thai year, constructs a URL,
/// and attempts to fetch data from that URL. It handles the HTTP response and returns
/// the retrieved data as a string.
///
/// # Parameters
///
/// * `year`: An `i32` representing the Gregorian year for which to fetch data.
///
/// # Returns
///
/// A `Result` which is:
/// * `Ok` containing a `String` of the retrieved data, trimmed of leading/trailing whitespace and newlines.
/// * `Err` containing a `String` describing the error if the HTTP request fails or returns a non-2xx status code.
fn get_data_stat_by_year(year: i32) -> Result<String, String> {
    let thai_year = convert_to_thai_year(year);
    let url = format!(
        "https://stat.bora.dopa.go.th/new_stat/file/{}/stat_c{}.txt",
        thai_year, thai_year
    );

    let mut result = String::new();
    if let Ok(response) = reqwest::blocking::get(url) {
        if response.status().as_u16() / 100 != 2 {
            return Err(format!(
                "Fail request with HTTP code: {:?}",
                response.status().as_u16()
            ));
        }
        result = response.text().ok().unwrap();
    }

    Ok(result.trim_matches(|c| c == ' ' || c == '\n').to_string())
}

/// Extracts fields from a given row of data using the '|' delimiter.
///
/// This function splits the input row string by the '|' character and returns a vector of strings,
/// where each string represents a field extracted from the input row.
///
/// # Parameters
///
/// * `row` - A string slice representing the input row of data.
///
/// # Returns
///
/// A vector of strings, where each string is a field extracted from the input row.
fn extract_row(row: &str) -> Vec<String> {
    row.split('|')
        .into_iter()
        .map(|value| value.to_string())
        .collect::<Vec<String>>()
}

/// Updates a row in the database with population data.
///
/// This function processes a line of population data, parses it into a `PopulationRow` struct,
/// generates an SQL insert statement, and executes it against the provided database connection.
/// Note: Duckdb has internal mechanism which supports ACID
///
/// # Parameters
///
/// * `conn` - A reference to a DuckDB `Connection` object for database operations.
/// * `line` - A string slice containing the raw population data to be processed.
/// * `year` - An integer representing the year of the population data.
///
/// # Returns
///
/// A `Result` which is:
/// * `Ok` with a `String` "Updated population" if the operation was successful.
/// * `Err` with a boxed dynamic `Error` if any step in the process fails.
fn update_row(conn: &Connection, line: &str, year: i32) -> Result<String, IngestionError> {
    // Extract fields from the line and convert them into a PopulationRow struct
    let extracted = extract_row(line.trim_matches(|c| ['|', ' ', '\n', '\r'].contains(&c)));
    let population_row = match PopulationRow::parse(extracted) {
        Ok(row) => row,
        Err(e) => return Err(IngestionError::Parse(e)),
    };

    // Generate an SQL insert statement and execute it against the database connection
    let insert_sql = generate_insert_sql_given_row_struct(year, &population_row);
    conn.execute(&insert_sql, [])?;

    // Return success message
    Ok("Updated population".to_string())
}

/// Spawns a new thread to update population data for a given year.
///
/// This function creates a new thread that fetches population data for the specified year,
/// processes the data, and updates the corresponding database table. It uses multithreading
/// to improve performance by processing multiple years concurrently.
///
/// # Parameters
///
/// * `conn`: A reference to an `Arc<Mutex<Connection>>` containing the database connection.
/// * `year`: An `i32` representing the year for which to update population data.
///
/// # Returns
///
/// A `JoinHandle<()>` representing the handle to the spawned thread. The thread will update
/// the population data for the specified year and exit once completed.
fn update_population(conn: &Arc<Mutex<Connection>>, year: i32) -> JoinHandle<()> {
    let conn_clone = Arc::clone(&conn);
    let handle = thread::spawn(move || {
        if let Ok(data) = get_data_stat_by_year(year) {
            let data_lines: Vec<_> = data.split('\n').collect();
            let mut thread_handles = vec![];

            for line in data_lines {
                let conn_inner = Arc::clone(&conn_clone);
                let line = line.to_string();
                let handle = thread::spawn(move || {
                    let conn = conn_inner.lock().unwrap();
                    update_row(&conn, &line, year).ok();
                });
                thread_handles.push(handle);
            }

            for handle in thread_handles {
                handle.join().unwrap();
            }
        }
    });
    handle
}
/// Executes the main ingestion process using multithreading.
///
/// This function performs the following steps:
/// 1. Creates an in-memory DuckDB table.
/// 2. Initiates population data updates for years 1993 to 2023 using multiple threads.
/// 3. Waits for all update threads to complete.
/// 4. Writes the collected data into Hive partitions.
///
/// # Returns
///
/// A `Result` which is:
/// * `Ok(())` if the ingestion process completes successfully.
/// * `Err(IngestionError)` if any step in the process fails, where `IngestionError`
///   is a custom error type that encapsulates various potential error scenarios.
fn main() -> Result<(), IngestionError> {
    println!("Run ingestion - Multithreading");
    // Create a Duckdb table
    let conn = Connection::open_in_memory()?;
    create_duck_db_table(&conn)?;
    let conn = Arc::new(Mutex::new(conn));

    // Initial year
    let start_year = 1993;
    let end_year = 2025;

    let mut handles = vec![];
    for year in start_year..=end_year {
        let conn_clone = Arc::clone(&conn);
        let handle = update_population(&conn_clone, year);
        handles.push(handle);
    }
    for handle in handles {
        handle.join().unwrap();
    }

    let conn = Arc::try_unwrap(conn)
        .expect("Failed to unwrap Arc")
        .into_inner()
        .unwrap();
    write_into_hive_partition(&conn)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use duckdb::Connection;

    #[test]
    fn test_convert_to_thai_year() {
        assert_eq!(convert_to_thai_year(2000), 43);
        assert_eq!(convert_to_thai_year(2023), 66);
        assert_eq!(convert_to_thai_year(1993), 36);
    }

    #[test]
    fn test_extract_row() {
        let row = "value1|value2|value3";
        let extracted = extract_row(row);
        assert_eq!(extracted, vec!["value1", "value2", "value3"]);
    }

    #[test]
    fn test_update_row_success() {
        let conn = Connection::open_in_memory().expect("Failed to create connection");
        // Assuming `create_duck_db_table` creates the required table structure
        create_duck_db_table(&conn).expect("Failed to create table");
        let year = 2023;
        let line = "|2024|001|Description|RC01|Region Description|CCA01|CCAATT Desc|CCAMM01|CCAATTMM Desc|1234|5678|6912|345|";

        // Mock PopulationRow parse and SQL generator for the test
        let row_vec = vec![
            "2023",
            "002",
            "Description",
            "RC01",
            "Region Description",
            "CCA01",
            "CCAATT Desc",
            "CCAMM01",
            "CCAATTMM Desc",
            "1234",
            "5678",
            "6912",
            "345",
        ]
        .into_iter()
        .map(|value| value.to_string())
        .collect::<Vec<String>>();
        let parse_result = PopulationRow::parse(row_vec);
        assert!(parse_result.is_ok());

        let sql = generate_insert_sql_given_row_struct(year, &parse_result.unwrap());
        assert!(conn.execute(&sql, []).is_ok());

        let result = update_row(&conn, line, year);

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "Updated population");
    }
}
