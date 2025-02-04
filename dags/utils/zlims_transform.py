import pandas as pd

def transform_samples_data(df: pd.DataFrame, ts: str) -> pd.DataFrame:
    # Remove duplicates
    df = df.drop_duplicates()

   # Rename columns
    cols = {
        'Sample ID(*)': 'id_repository',
        'Flowcell ID': 'id_library',
        'Library ID': 'id_pool',
        'DNB ID(*)': 'id_dnb',
        'Flowcell ID': 'id_flowcell',
        'Index(*)': 'id_index',
        'Create Time': 'date_create',
    }

    df.rename(columns=cols, inplace=True)
    
    # Convert index to numeric, handling errors
    df['id_index'] = pd.to_numeric(df['id_index'], errors='coerce').astype('Int64')
    
    # Select only the columns we want
    df = df[list(cols.values())]

    # Filter out test/sample data
    patterns = r'(?i)Test|test|tes|^BC|SAMPLE|^DNB'
    df = df[~df['id_repository'].str.contains(patterns, na=False)]

    # Clean id_repository field
    df['id_repository'] = df['id_repository'].str.split(r'[-_]').str[0]
    
    print(f"Final rows after transformation: {len(df)}")


    if "created_at" not in df.columns:
        df["created_at"] = ts
    if "updated_at" not in df.columns:
        df["updated_at"] = ts

    # Need to fillna so that the mysql connector can insert the data.
    df = df.astype(str)
    df.fillna(value="", inplace=True)
    return df