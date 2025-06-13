import pandas as pd
import io
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from sqlalchemy import create_engine, VARCHAR, DATETIME
from airflow.models import Variable


def _parse_sequencing_sheet() -> pd.DataFrame:
    """
    Parse the sequencing sheets and return a DataFrame. This sheets are hardcoded and need to be taken carefully.
    """

    MAIN_URL = "https://docs.google.com/spreadsheets/d/"
    EXP_FORM = "format=csv"
    ILL_2024 = f"1yRRjqoNfsbaEp14Jx1md7oudqAWw2bbLwXK3Uc2mlQI/export?gid=1944745795&{EXP_FORM}"
    ILL_2025 = f"1L-mrdJSYNgu4Jq3tRNliQgeZX9MhDnlOyd6YwR9Nx8s/export?gid=0&{EXP_FORM}"
    MGI_2024 = f"1xvFL2WB7b2B4iYwvgw43BIQL7O8DZpqhDtwXbXr5KBY/export?gid=1944745795&{EXP_FORM}"
    MGI_2025 = f"1R8I7MakGrJJwiyHLDbU7Nii18qWhFIHFeOxwAQ9bUok/export?gid=0&{EXP_FORM}"
    # moved from ONT to MGI on different sheet
    MGI_2025_FROM_ONT = f"1R8I7MakGrJJwiyHLDbU7Nii18qWhFIHFeOxwAQ9bUok/export?gid=317001740&{EXP_FORM}"
    ONT = f"1pMmO4wHAGwyBMQoUT5H8ann6BFfW6sb8OLq5x50zpuY/export?gid=1243138284&{EXP_FORM}"

    # ILLUMINA
    ill_2025 = pd.read_csv(f"{MAIN_URL}{ILL_2025}")
    ill_2025.iloc[:, 9] = ill_2025.iloc[:, 9].astype(float)  # volume_remain
    ill_2025.iloc[:, 10] = pd.to_datetime(
        ill_2025.iloc[:, 10], format='%Y-%m-%d', errors='coerce')  # delivery_date
    ill_2025.iloc[:, 15] = pd.to_datetime(
        ill_2025.iloc[:, 15], format='%Y/%m/%d', errors='coerce')  # extraction_date
    ill_2025.iloc[:, 70] = pd.to_datetime(
        ill_2025.iloc[:, 70], format='%Y/%m/%d', errors='coerce')  # sequencing_start_date
    ill_2025.iloc[:, 71] = pd.to_datetime(
        ill_2025.iloc[:, 71], format='%Y/%m/%d', errors='coerce')  # sequencing_finish_date
    # code_repo, volume_remain, delivery_date, extraction_date, extraction_status, libprep_status, sequencing_start_date, sequencing_finish_date, sequencing_status
    ill_2025 = ill_2025.iloc[:, [5, 9, 10, 15, 24, 50, 70, 71, 73]]
    ill_2025.columns = ["code_repository",  "volume_remain", "delivery_date", "extraction_date",
                        "extraction_status", "libprep_status", "sequencing_start_date", "sequencing_finish_date", "sequencing_status"]
    ill_2025["platform"] = "ILLUMINA"
    ill_2025 = ill_2025[ill_2025['code_repository'].notna()]

    ill_2024 = pd.read_csv(f"{MAIN_URL}{ILL_2024}")
    ill_2024.iloc[:, 10] = pd.to_datetime(
        ill_2024.iloc[:, 10], format='%Y-%m-%d', errors='coerce')  # delivery_date
    ill_2024.iloc[:, 16] = pd.to_datetime(
        ill_2024.iloc[:, 16], format='%Y-%m-%d', errors='coerce')  # extraction_date
    # code_repo, volume_remain, delivery_date, extraction_date, extraction_status, libprep_status, sequencing_start_date, sequencing_finish_date, sequencing_status
    ill_2024 = ill_2024.iloc[:, [4, 10, 16, 26, 52, 75]]
    ill_2024.columns = ["code_repository", "delivery_date", "extraction_date",
                        "extraction_status", "libprep_status", "sequencing_status"]
    ill_2024["volume_remain"] = 0.0
    ill_2024["sequencing_start_date"] = pd.NaT
    ill_2024["sequencing_finish_date"] = pd.NaT
    ill_2024 = ill_2024[["code_repository",  "volume_remain", "delivery_date", "extraction_date",
                         "extraction_status", "libprep_status", "sequencing_start_date", "sequencing_finish_date", "sequencing_status"]]
    ill_2024["platform"] = "ILLUMINA"
    ill_2024 = ill_2024[ill_2024['code_repository'].notna()]

    # ONT
    ont = pd.read_csv(f"{MAIN_URL}{ONT}")
    ont.iloc[:, 7] = ont.iloc[:, 7].astype(float)  # volume_remain
    ont.iloc[:, 6] = pd.to_datetime(
        ont.iloc[:, 6], format='%y%m%d', errors='coerce')  # delivery_date
    ont.iloc[:, 13] = pd.to_datetime(
        ont.iloc[:, 13], format='%y%m%d', errors='coerce')  # extraction_date
    ont.iloc[:, 71] = pd.to_datetime(
        ont.iloc[:, 71], format='%y%m%d', errors='coerce')  # sequencing_start_date
    ont.iloc[:, 72] = pd.to_datetime(
        ont.iloc[:, 72], format='%y%m%d', errors='coerce')  # sequencing_finish_date
    # code_repo, volume_remain, delivery_date, extraction_date, extraction_status, libprep_status, sequencing_start_date, sequencing_finish_date, sequencing_status
    ont = ont.iloc[:, [2, 7, 6, 13, 27, 66, 71, 72, 73]]
    ont.columns = ["code_repository",  "volume_remain", "delivery_date", "extraction_date", "extraction_status",
                   "libprep_status", "sequencing_start_date", "sequencing_finish_date", "sequencing_status"]

    ont["platform"] = "ONT"
    ont = ont[ont['code_repository'].notna()]

    # MGI
    mgi_2024 = pd.read_csv(f"{MAIN_URL}{MGI_2024}")
    mgi_2024.iloc[:, 7] = pd.to_numeric(
        mgi_2024.iloc[:, 7], errors='coerce')  # volume_remain
    mgi_2024.iloc[:, 11] = pd.to_datetime(
        mgi_2024.iloc[:, 11], format='%y%m%d;%H:%M', errors='coerce')  # extraction_date
    mgi_2024.iloc[:, 68] = pd.to_datetime(
        mgi_2024.iloc[:, 68], errors='coerce')  # sequencing_start_date
    mgi_2024.iloc[:, 69] = pd.to_datetime(
        mgi_2024.iloc[:, 69], errors='coerce')  # sequencing_finish_date
    # code_repo, volume_remain, delivery_date, extraction_date, extraction_status, libprep_status, sequencing_start_date, sequencing_finish_date, sequencing_status
    mgi_2024 = mgi_2024.iloc[:, [2, 7, 11, 26, 46, 68, 69, 78]]
    mgi_2024.columns = ["code_repository",  "volume_remain", "extraction_date", "extraction_status",
                        "libprep_status", "sequencing_start_date", "sequencing_finish_date", "sequencing_status"]
    mgi_2024["delivery_date"] = pd.NaT

    mgi_2024 = mgi_2024[["code_repository",  "volume_remain", "delivery_date", "extraction_date",
                         "extraction_status", "libprep_status", "sequencing_start_date", "sequencing_finish_date", "sequencing_status"]]
    mgi_2024["platform"] = "MGI"
    mgi_2024 = mgi_2024[mgi_2024['code_repository'].notna()]

    mgi_2025 = pd.read_csv(f"{MAIN_URL}{MGI_2025}")
    mgi_2025.iloc[:, 7] = pd.to_numeric(
        mgi_2025.iloc[:, 8], errors='coerce')  # volume_remain
    mgi_2025.iloc[:, 10] = pd.to_datetime(
        mgi_2025.iloc[:, 10], format='%y%m%d; %H:%M', errors='coerce')  # extraction_date
    mgi_2025.iloc[:, 58] = pd.to_datetime(
        mgi_2025.iloc[:, 58], errors='coerce')  # sequencing_start_date
    mgi_2025.iloc[:, 59] = pd.to_datetime(
        mgi_2025.iloc[:, 59], errors='coerce')  # sequencing_finish_date
    # code_repo, volume_remain, delivery_date, extraction_date, extraction_status, libprep_status, sequencing_start_date, sequencing_finish_date, sequencing_status
    mgi_2025 = mgi_2025.iloc[:, [4, 8, 10, 23, 40, 58, 59, 61]]
    mgi_2025.columns = ["code_repository",  "volume_remain", "extraction_date", "extraction_status",
                        "libprep_status", "sequencing_start_date", "sequencing_finish_date", "sequencing_status"]
    mgi_2025["delivery_date"] = pd.NaT

    mgi_2025 = mgi_2025[["code_repository",  "volume_remain", "delivery_date", "extraction_date",
                         "extraction_status", "libprep_status", "sequencing_start_date", "sequencing_finish_date", "sequencing_status"]]
    mgi_2025["platform"] = "MGI"
    mgi_2025 = mgi_2025[mgi_2025['code_repository'].notna()]

    mgi_2025_from_ont = pd.read_csv(f"{MAIN_URL}{MGI_2025_FROM_ONT}")
    mgi_2025_from_ont.iloc[:, 7] = pd.to_numeric(
        mgi_2025_from_ont.iloc[:, 8], errors='coerce')  # volume_remain
    mgi_2025_from_ont.iloc[:, 10] = pd.to_datetime(
        mgi_2025_from_ont.iloc[:, 10], format='%y%m%d; %H:%M', errors='coerce')  # extraction_date
    mgi_2025_from_ont.iloc[:, 58] = pd.to_datetime(
        mgi_2025_from_ont.iloc[:, 59], errors='coerce')  # sequencing_start_date
    mgi_2025_from_ont.iloc[:, 59] = pd.to_datetime(
        mgi_2025_from_ont.iloc[:, 60], errors='coerce')  # sequencing_finish_date
    # code_repo, volume_remain, delivery_date, extraction_date, extraction_status, libprep_status, sequencing_start_date, sequencing_finish_date, sequencing_status
    mgi_2025_from_ont = mgi_2025_from_ont.iloc[:, [
        4, 8, 10, 23, 40, 59, 60, 62]]
    mgi_2025_from_ont.columns = ["code_repository",  "volume_remain", "extraction_date", "extraction_status",
                                 "libprep_status", "sequencing_start_date", "sequencing_finish_date", "sequencing_status"]
    mgi_2025_from_ont["delivery_date"] = pd.NaT

    mgi_2025_from_ont = mgi_2025_from_ont[["code_repository",  "volume_remain", "delivery_date", "extraction_date",
                                           "extraction_status", "libprep_status", "sequencing_start_date", "sequencing_finish_date", "sequencing_status"]]
    mgi_2025_from_ont["platform"] = "MGI"
    mgi_2025_from_ont = mgi_2025_from_ont[mgi_2025_from_ont['code_repository'].notna(
    )]

    res = pd.concat([ill_2024, ill_2025, ont, mgi_2024,
                    mgi_2025, mgi_2025_from_ont], ignore_index=True)
    res['code_repository'] = res['code_repository'].str.replace(
        r'\.', '_', regex=True)
    res.set_index(["code_repository", "platform", "extraction_status",
                  "libprep_status", "sequencing_finish_date", "sequencing_status"], inplace=True)

    return res


def dump_sequencing_sheets_to_s3_and_db(aws_conn_id: str, bucket_name: str,
                                        object_path: str, db_secret_url: str, **kwargs) -> None:
    df = _parse_sequencing_sheet()

    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)

    file_name = f"{object_path}/{kwargs['curr_ds']}.csv"

    print(f" == Save result to s3 == ")
    s3 = S3Hook(aws_conn_id=aws_conn_id)
    s3.load_string(
        string_data=csv_buffer.getvalue(),
        key=file_name,
        bucket_name=bucket_name,
        replace=True
    )
    engine = create_engine(db_secret_url)

    print(f" == Save result to dwh == ")
    with engine.begin() as conn:
        df.to_sql(
            name=f"sheets_sequencing",
            if_exists="replace",
            schema=f"{Variable.get('dwh_schema')}",
            con=conn,
            dtype={'code_repository': VARCHAR(16), 'platform': VARCHAR(8), 'extraction_status': VARCHAR(
                32), 'libprep_status': VARCHAR(32), 'sequencing_finish_date': DATETIME, 'sequencing_status': VARCHAR(8)}
        )
