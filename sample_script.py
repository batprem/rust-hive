import requests
from result import Ok, Err, Result
import duckdb
from pathlib import Path


def create_duck_db_table():
    duckdb.sql("""CREATE OR REPLACE TABLE

    thai_population
    (
        data_year INTEGER,
        yymm VARCHAR,
        cc_code INTEGER,
        cc_desc VARCHAR,
        rcode_code VARCHAR,
        rcode_desc VARCHAR,
        ccaatt_code VARCHAR,
        ccaatt_desc VARCHAR,
        ccaattmm_code VARCHAR,
        ccaattmm_desc VARCHAR,
        male INTEGER,
        female INTEGER,
        total INTEGER,
        house INTEGER,
        PRIMARY KEY (data_year, cc_code)
    );""")


def generate_insert_sql(
    data_year: int,
    yymm: str,
    cc_code: int,
    cc_desc: str,
    rcode_code: str,
    rcode_desc: str,
    ccaatt_code: str,
    ccaatt_desc: str,
    ccaattmm_code: str,
    ccaattmm_desc: str,
    male: int,
    female: int,
    total: int,
    house: int,
) -> str:
    return f"""INSERT INTO thai_population
VALUES (
    '{data_year}',
    '{yymm}',
    '{cc_code}',
    '{cc_desc}',
    '{rcode_code}',
    '{rcode_desc}',
    '{ccaatt_code}',
    '{ccaatt_desc}',
    '{ccaattmm_code}',
    '{ccaattmm_desc}',
    '{male}',
    '{female}',
    '{total}',
    '{house}'
)"""


def write_into_hive_partition():
    Path('.datasets').mkdir(parents=True, exist_ok=True)
    duckdb.sql("""COPY thai_population
TO
    './datasets/thai_population'
    (
        FORMAT PARQUET,
        PARTITION_BY (data_year),
        OVERWRITE_OR_IGNORE,
        COMPRESSION GZIP,
        FILE_EXTENSION 'parquet.gz'
    )
""")


def convert_to_thai_year(year: int) -> int:
    """Convert year into shortened Thai year

    Args:
        year (int) - Year such as 2024
    Returns:
        int - (Thai year)
    """
    return (year + 543) - 2500


def get_data_stat_by_year(year: int) -> Result[str, str]:
    thai_year = convert_to_thai_year(year)
    url = f'https://stat.bora.dopa.go.th/new_stat/file/{thai_year}/stat_c{thai_year}.txt' # noqa
    response = requests.get(url)
    if not response.ok:
        return Err(response.text)
    else:
        return Ok(response.text)


def clean_text(text: str) -> str:
    return text.strip('\ufeff')


def extract_row(row: str) -> list[str]:
    return row.split('|')


def string_to_int(value: str) -> int:
    return int(value.replace(',', ''))


year = 1993


# Create table
create_duck_db_table()
while True:
    # Match pattern
    match get_data_stat_by_year(year):
        case Ok(value):
            ...
        case Err(e):
            break
    # Create a data structure
    table = clean_text(value)

    # Insert into the table
    for row in table.splitlines():
        (
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
            house_str,
        ) = extract_row(row.strip('|'))
        sql_syntax = generate_insert_sql(
            year,
            yymm,
            string_to_int(cc_code),
            cc_desc,
            rcode_code,
            rcode_desc,
            ccaatt_code,
            ccaatt_desc,
            ccaattmm_code,
            ccaattmm_desc,
            string_to_int(male_str),
            string_to_int(female_str),
            string_to_int(total_str),
            string_to_int(house_str),
        )
        duckdb.sql(sql_syntax)
    year += 1

write_into_hive_partition()
