import pandas as pd
from Crypto.Cipher import AES
from Crypto.Util.Padding import unpad
import binascii
import os
import datetime
import pandas as pd
from typing import Dict, Any, Callable
import json
import requests
from sqlalchemy import create_engine
from sqlalchemy.types import VARCHAR, DATETIME
import logging

logging.basicConfig(
format="[{levelname}][{asctime}]  {message}",
style="{",
datefmt="%Y-%m-%d %H:%M",
level=logging.INFO)

# AES_ENCRYPTION_SECRET_KEY = os.environ["AES_ENCRYPTION_SECRET_KEY"].encode("utf-8")
# IV = os.environ["AES_ENCRYPTION_IV"].encode("utf-8")
# RDS_SECRET = os.getenv("RDS_SECRET").replace("mysqldb", "pymysql")  # type: ignore
# TABLE_NAME = "decrypted_phenovar_participants"

def main(AES_ENCRYPTION_SECRET_KEY:str, IV:str, RDS_SECRET:str, TABLE_NAME:str, PHENOVAR_USERNAME:str, PHENOVAR_PASSWORD:str) -> None:

    host = "https://api-bgsi-2-registry.kemkes.go.id"
    def jwt_login(login_url:str, data: Dict, headers:dict, get_token_func: Callable):
        resp = requests.post(url=login_url, data=json.dumps(data), headers=headers) 
        try:
            if resp.status_code == 200:
                resp_header = resp.headers
                resp_data = resp.json()
                return get_token_func(resp_data, resp_header)
            else:
                raise Exception(f"Failed to login, status code: {resp.status_code}")
        except json.JSONDecodeError as e:
            logging.ERROR(f"Decoding into JSON failed: {e}") # type: ignore
            raise ValueError(f"Failed response: {resp}")
        except Exception as e:
            logging.ERROR(f"An error occurred: {e}")  # type: ignore
            raise Exception(f"An error occurred during the request: {type(e)} | {e.__str__}")


    def get_token(resp: Dict[str, Any], response_header: Dict[str, Any]) -> Dict[str, Any]:
        """
        Get token data from RegINA response.
        """
        return {
            "headers": {
                "Authorization": f"Bearer {resp['data']['access_token']}",
                "Cookie": f"{response_header['Set-Cookie']}"
            }
        }

    login_url =  f"{host}/api/v1/institution/login"
    headers = {"Content-Type": "application/json"}

    body = {
        "email": PHENOVAR_USERNAME,
        "password": PHENOVAR_PASSWORD
    }

    print(body)

    phenovar_token = jwt_login(login_url=login_url, data=body, headers=headers, get_token_func=get_token)


    get_participants_url = f"{host}/api/v1/participants?perpage=50000"
    def get_all_active_participants(url:str, headers:str):
        resp = requests.get(url=url, headers = headers)  # type: ignore
        return resp.json().get("data")

    data = get_all_active_participants(get_participants_url, phenovar_token["headers"])

    data = pd.DataFrame(data)
    logging.info(f"Data shape: {data.shape}")
    logging.info(data.columns)
    def aes_decrypt(encrypted_hex, key, iv):
        try:
            # Convert hex string to bytes
            encrypted_bytes = binascii.unhexlify(encrypted_hex)
            
            # Initialize AES cipher
            cipher = AES.new(key, AES.MODE_CBC, iv)
            
            # Decrypt and unpad
            decrypted = unpad(cipher.decrypt(encrypted_bytes), AES.block_size)
            return decrypted.decode("utf-8")
        except Exception as e:
            return f"Error: {str(e)}"

    def transform_data(df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        # Remove duplicates
        df = df.drop_duplicates()
        df["nik"] = df["nik"].apply(lambda x: aes_decrypt(x, AES_ENCRYPTION_SECRET_KEY, IV))
        df["full_name"] = df["full_name"].apply(lambda x: aes_decrypt(x, AES_ENCRYPTION_SECRET_KEY, IV))
        df["dob"] = df["dob"].apply(lambda x: aes_decrypt(x, AES_ENCRYPTION_SECRET_KEY, IV))

        df.rename(columns={"dob": "birth_date", "id": "id_subject", "gender": "sex"}, inplace=True)


        df["creation_date"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        df["updation_date"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    #     [5 rows x 23 columns]
    # Index(['id_sujbect', 'nik', 'ihs_number', 'source', 'full_name', 'sex', 'dob',
    #        'province_code', 'province', 'district_code', 'district',
    #        'subdistrict_code', 'subdistrict', 'village_code', 'village', 'address',
    #        'use_nik_ibu', 'satset_error', 'created_by', 'updated_by', 'created_at',
    #        'updated_at', 'updation_date'],

        df = df[["id_subject", "full_name", "nik", "use_nik_ibu", "birth_date", "sex", "source", "province", "province_code", "district", "district_code", "creation_date", "updation_date", "created_at", "updated_at"]]

        return df

    data = transform_data(data)
    data.reset_index(drop=True, inplace=True)
    data.set_index(["id_subject", "province_code", "province", "created_at", "updated_at"], inplace=True)

    engine = create_engine(RDS_SECRET)

    with engine.begin() as conn:
        data.to_sql(
            name=f"{TABLE_NAME}",
            schema="dwh_restricted",
            if_exists="replace",
            con=conn,
            dtype={'id_subject': VARCHAR(40), 'nik': VARCHAR(
                16), 'created_at': DATETIME, "province_code":VARCHAR(4), "province":VARCHAR(64), 'updated_at': DATETIME}
        )