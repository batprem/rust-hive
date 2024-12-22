#![allow(dead_code)]
pub mod population {
    pub struct PopulationRow {
        pub yymm: String,
        pub cc_code: String,
        pub cc_desc: String,
        pub rcode_code: String,
        pub rcode_desc: String,
        pub ccaatt_code: String,
        pub ccaatt_desc: String,
        pub ccaattmm_code: String,
        pub ccaattmm_desc: String,
        pub male: i32,
        pub female: i32,
        pub total: i32,
        pub house: i32,
    }

    impl PopulationRow {
        pub fn clean_text(text: &str) -> String {
            text.trim_matches(|c|  ['\u{feff}', '|'].contains(&c)).to_string()
        }

        pub fn extract_row(row: &str) -> Vec<&str> {
            row.split('|').collect()
        }

        pub fn string_to_int(value: &str) -> Result<i32, std::num::ParseIntError> {
            value.replace(",", "").parse::<i32>()
        }

        pub fn parse(row: &str) -> Result<Self, String> {
            let cleaned_row = Self::clean_text(row);
            let fields: Vec<&str> = Self::extract_row(&cleaned_row);

            if fields.len() != 13 {
                return Err("Row does not have the correct number of fields".to_string());
            }

            Ok(PopulationRow {
                yymm: fields[0].to_string(),
                cc_code: fields[1].to_string(),
                cc_desc: fields[2].to_string(),
                rcode_code: fields[3].to_string(),
                rcode_desc: fields[4].to_string(),
                ccaatt_code: fields[5].to_string(),
                ccaatt_desc: fields[6].to_string(),
                ccaattmm_code: fields[7].to_string(),
                ccaattmm_desc: fields[8].to_string(),
                male: Self::string_to_int(fields[9]).map_err(|e| e.to_string())?,
                female: Self::string_to_int(fields[10]).map_err(|e| e.to_string())?,
                total: Self::string_to_int(fields[11]).map_err(|e| e.to_string())?,
                house: Self::string_to_int(fields[12]).map_err(|e| e.to_string())?,
            })
        }
    }
}


fn main() {
    let row = "|2023|001|Description|RC01|Region Description|CCA01|CCAATT Desc|CCAMM01|CCAATTMM Desc|1234|5678|6912|345|";

    match population::PopulationRow::parse(row) {
        Ok(population_row) => {
            println!(
                "Parsed row: YYMM = {}, CC Code = {}, Male = {}",
                population_row.yymm, population_row.cc_code, population_row.male
            );
        }
        Err(err) => {
            println!("Error parsing row: {}", err);
        }
    }
}
