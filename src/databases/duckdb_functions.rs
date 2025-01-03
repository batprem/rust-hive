#![allow(dead_code)]
#![allow(clippy::too_many_arguments)]

use duckdb::{Connection, Result};
use rust_hive::parsers::population::PopulationRow;
use std::fs;
use std::io::Error;
use std::path::Path;

/// Creates or replaces a table named 'thai_population' in the DuckDB database.
///
/// This function executes a SQL statement to create a table with columns
/// representing various demographic data for Thai population statistics.
///
/// # Arguments
///
/// * `conn` - A reference to a DuckDB Connection object used to execute the SQL statement.
///
/// # Returns
///
/// * `Result<()>` - Returns Ok(()) if the table is successfully created, or an error if the operation fails.
///
pub fn create_duck_db_table(conn: &Connection) -> Result<()> {
    conn.execute(
        "CREATE OR REPLACE TABLE thai_population (
            data_year INTEGER,
            yymm TEXT,
            cc_code INTEGER,
            cc_desc TEXT,
            rcode_code TEXT,
            rcode_desc TEXT,
            ccaatt_code TEXT,
            ccaatt_desc TEXT,
            ccaattmm_code TEXT,
            ccaattmm_desc TEXT,
            male INTEGER,
            female INTEGER,
            total INTEGER,
            house INTEGER,
            PRIMARY KEY (data_year, cc_code)
        );",
        [],
    )?;
    Ok(())
}

/// Generate Insertion SQL statement.
///
/// This function generates a SQL statement to insert a population row
///
///
/// # Arguments
///
/// data_year: i32,
/// yymm: &str,
/// cc_code: i32,
/// cc_desc: &str,
/// rcode_code: &str,
/// rcode_desc: &str,
/// ccaatt_code: &str,
/// ccaatt_desc: &str,
/// ccaattmm_code: &str,
/// ccaattmm_desc: &str,
/// male: i32,
/// female: i32,
/// total: i32,
/// house: i32,
///
/// # Returns
///
/// * `String`.
///
pub fn generate_insert_sql(
    data_year: i32,
    yymm: &str,
    cc_code: i32,
    cc_desc: &str,
    rcode_code: &str,
    rcode_desc: &str,
    ccaatt_code: &str,
    ccaatt_desc: &str,
    ccaattmm_code: &str,
    ccaattmm_desc: &str,
    male: i32,
    female: i32,
    total: i32,
    house: i32,
) -> String {
    format!(
        "INSERT INTO thai_population VALUES ('{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}')",
        data_year, yymm, cc_code, cc_desc, rcode_code, rcode_desc, ccaatt_code, ccaatt_desc, ccaattmm_code, ccaattmm_desc, male, female, total, house
    )
}


/// The function generates an SQL insert statement using data from a PopulationRow struct for a given
/// data year.
/// 
/// Arguments:
/// 
/// * `data_year`: The `data_year` parameter is an integer representing the year for which the data is
/// being inserted into the database.
/// * `row`: The `generate_insert_sql_given_row_struct` function takes in the `data_year` as an `i32`
/// and a reference to a `PopulationRow` struct named `row`. The `PopulationRow` struct likely contains
/// the following fields:
/// 
/// Returns:
/// 
/// A SQL insert statement is being returned as a String, generated based on the provided data_year and
/// PopulationRow struct fields.
pub fn generate_insert_sql_given_row_struct(data_year: i32, row: &PopulationRow) -> String {
    generate_insert_sql(
        data_year,
        &row.yymm,
        row.cc_code,
        &row.cc_desc,
        &row.rcode_code,
        &row.rcode_desc,
        &row.ccaatt_code,
        &row.ccaatt_desc,
        &row.ccaattmm_code,
        &row.ccaattmm_desc,
        row.male,
        row.female,
        row.total,
        row.house,
    )
}

/// The function `prepare_directory` creates a directory named "datasets" if it does not already exist.
/// 
/// Returns:
/// 
/// The `prepare_directory` function returns a `Result<(), Error>`.
fn prepare_directory() -> Result<(), Error> {
    let dir = Path::new("./datasets");
    if !dir.exists() {
        fs::create_dir_all(dir)?;
    }
    Ok(())
}

/// The function `write_into_hive_partition` writes data into a Hive partition in Rust.
/// 
/// Arguments:
/// 
/// * `conn`: The `conn` parameter in the `write_into_hive_partition` function is of type `&Connection`,
/// which likely represents a connection to a database or data storage system. This connection is used
/// to execute a SQL query to copy data into a Hive partition.
/// 
/// Returns:
/// 
/// The `write_into_hive_partition` function is returning a `Result` with a unit type `()` as the
/// success value.
pub fn write_into_hive_partition(conn: &Connection) -> Result<()> {
    let _ = prepare_directory();
    conn.execute(
        "
        COPY thai_population TO './datasets/thai_population' (
            FORMAT PARQUET,
            PARTITION_BY (data_year),
            OVERWRITE_OR_IGNORE,
            COMPRESSION GZIP,
            FILE_EXTENSION 'parquet.gz'
        );
        ",
        [],
    )?;
    Ok(())
}

/// The function `query_population_all` retrieves and prints population data from a database table in
/// Rust.
/// 
/// Arguments:
/// 
/// * `conn`: The `conn` parameter in the `query_population_all` function is a reference to a database
/// connection. This connection is used to interact with the database and execute SQL queries.
/// 
/// Returns:
/// 
/// The `query_population_all` function is querying the `thai_population` table from the database using
/// the provided connection. It retrieves data for each row in the table, extracts specific columns'
/// values, and prints them out in a formatted way. The function returns a `Result` indicating success
/// or an error.
pub fn query_population_all(conn: &Connection) -> Result<()> {
    let mut stmt = conn.prepare("SELECT * FROM thai_population;")?;
    let mut rows = stmt.query([])?;

    while let Some(row) = rows.next()? {
        let data_year: i32 = row.get(0)?; // Access columns by index
        let yymm: String = row.get(1)?;
        let cc_code: i32 = row.get(2)?;
        let cc_desc: String = row.get(3)?;
        let male: i32 = row.get(10)?;
        let female: i32 = row.get(11)?;
        let total: i32 = row.get(12)?;
        let house: i32 = row.get(13)?;

        println!(
            "Year: {}, YYMM: {}, Code: {}, Desc: {}, Male: {}, Female: {}, Total: {}, House: {}",
            data_year, yymm, cc_code, cc_desc, male, female, total, house
        );
    }

    Ok(())
}
