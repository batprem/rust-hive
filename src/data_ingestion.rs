mod databases;
use duckdb::{Connection, Result};
use databases::duckdb_functions::{
    create_duck_db_table,
    generate_insert_sql,
    write_into_hive_partition,
    query_population_all
};
use std::error::Error;




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


fn get_data_stat_by_year(year: i32) -> Result<String, Box<dyn Error>> {
    let thai_year = convert_to_thai_year(year);
    let url = format!(
        "https://stat.bora.dopa.go.th/new_stat/file/{}/stat_c{}.txt",
        thai_year, thai_year
    );
    let response = reqwest::blocking::get(url)?;
    if response.status().as_u16() != 200 {
        return Err(format!("Not found: HTTP {}", response.status().as_u16()).into());
    }
    let result = response.text()?;

    Ok(result.trim_matches(|c| c == ' ' || c == '\n').to_string())
}

fn clean_text(text: &str) -> String {
    text.trim_matches('\u{feff}').to_string()
}

fn extract_row(row: &str) -> Vec<&str> {
    row.split('|').collect()
}

fn string_to_int(value: &str) -> Result<i32, std::num::ParseIntError> {
    value.replace(",", "").parse::<i32>()
}

fn main() -> Result<(), Box<dyn Error>> {
    println!("Running data ingestion");
    // Create a Duckdb table
    let conn = Connection::open_in_memory()?;
    create_duck_db_table(&conn)?;

    // Initial year
    let mut year = 1993;

    while let Ok(data) = get_data_stat_by_year(year) {
        for line in data.split("\n") {
            // TODO: Make it to a function
            let extracted = extract_row(
                line.trim_matches(
                    |c| ['|', ' ', '\n', '\r'].contains(&c)
                )
            );
            if let [
                yymm,
                cc_code,
                cc_desc,
                rcode_code,
                rcode_desc,
                ccaatt_code,
                ccaatt_desc,
                ccaattmm_code,
                ccaattmm_desc,
                male_str,
                female_str,
                total_str,
                house_str
            ] = extracted[..] {
                let male = string_to_int(male_str)?;
                let female = string_to_int(female_str)?;
                let total = string_to_int(total_str)?;
                let house = string_to_int(house_str)?;
                let insert_sql = generate_insert_sql(
                    year,
                    yymm,
                    cc_code.parse::<i32>()?,
                    &clean_text(cc_desc),
                    rcode_code,
                    &clean_text(rcode_desc),
                    ccaatt_code,
                    &clean_text(ccaatt_desc),
                    ccaattmm_code,
                    &clean_text(ccaattmm_desc),
                    male,
                    female,
                    total,
                    house,
                );
                conn.execute(&insert_sql, [])?;
            }
            else {
                println!("Row does not have the correct number of fields");
            }
        }
        year += 1;
    }
    query_population_all(&conn)?;
    write_into_hive_partition(&conn)?;
    Ok(())
}
