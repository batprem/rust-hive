mod databases;
mod parsers;

use std::sync::Arc;
use tokio::sync::Mutex;
use databases::duckdb_functions::{
    create_duck_db_table, generate_insert_sql_given_row_struct, write_into_hive_partition,
};
use duckdb::{Connection, Error as DuckDBError, Result};

use reqwest::Error as RequestwestError;
use reqwest::Client;
use rust_hive::parsers::population::PopulationRow;
use thiserror::Error;
use tokio::task::JoinError;
use futures::future::join_all;


// Custom error handling
#[derive(Error, Debug)]
enum IngestionError {
    #[error("Error connecting to DuckDB: {0}")]
    DuckDB(#[from] DuckDBError),
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Requestwest error: {0}")]
    Requestwest(#[from] RequestwestError),
    #[error("Join error: {0}")]
    Join(#[from] JoinError),
    #[error("Parse error: {0}")]
    Parse(String),
}

/// Converts a Gregorian year to a Thai year.
///3
/// This function takes an integer representing a Gregorian year and returns the corresponding Thai year.
/// The conversion is based on the Thai calendar system, which starts from the year 2500.
///
/// # Parameters
///
/// * `year` - An integer representing the Gregorian year.
///
/// # Returns
///
/// An integer representing the corresponding Thai year.
fn convert_to_thai_year(year: i32) -> i32 {
    year + 543 - 2500
}

async fn get_data_stat_by_year(year: i32) -> Result<String, String> {
    let client = Client::new();
    let thai_year = convert_to_thai_year(year);
    let url = format!(
        "https://stat.bora.dopa.go.th/new_stat/file/{}/stat_c{}.txt",
        thai_year, thai_year
    );
    let mut result = String::new();
    if let Ok(response) = client.get(&url).send().await {
        if response.status().as_u16()/ 100 != 2 {
            return Err(format!(
                "Fail request with HTTP code: {:?}",
                response.status().as_u16()
            ));
        }
        result = response.text().await.ok().unwrap();
    }

    Ok(result.trim_matches(|c| c == ' ' || c == '\n').to_string())
}


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
async fn a_update_row(conn:  Arc<Mutex<&Connection>>, line: &str, year: i32) -> Result<String, IngestionError> {
    println!("{}", line);
    // Extract fields from the line and convert them into a PopulationRow struct
    let extracted = extract_row(line.trim_matches(|c| ['|', ' ', '\n', '\r'].contains(&c)));
    let population_row = match PopulationRow::parse(extracted) {
        Ok(row) => row,
        Err(e) => return Err(IngestionError::Parse(e)),
    };

    // Generate an SQL insert statement
    let insert_sql = generate_insert_sql_given_row_struct(year, &population_row);

    // Execute the SQL asynchronously
    let conn = conn.lock().await;
    conn.execute(&insert_sql, []).map_err(|e| IngestionError::DuckDB(e))?;


    // Return success message
    Ok("Updated population".to_string())
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<(), IngestionError> {
    println!("Run ingestion");
    // Create a Duckdb table
    let conn = Connection::open_in_memory()?;
    create_duck_db_table(&conn)?;

    // Initial year
    let mut year = 1993;
    // Ingest data and update the database in batches of 1000 rows per transactions
    // conn.execute("BEGIN TRANSACTION", [])?;
    while let Ok(data) = get_data_stat_by_year(year).await {
        let futures = data
            .split("\n")
            .into_iter()
            .map(|line| {
                let conn = Arc::new(Mutex::new(&conn));  // Make connection Arc for use in async block
                async move {
                    a_update_row(conn, line, year).await
                }
            }).collect::<Vec<_>>();

        let _results = join_all(futures).await;
        year += 1;
    }
    write_into_hive_partition(&conn)?;
    
    Ok(())
}
