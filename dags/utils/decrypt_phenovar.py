import pandas as pd
from Crypto.Cipher import AES
from Crypto.Util.Padding import unpad
import binascii
import os

AES_ENCRYPTION_SECRET_KEY = os.environ["AES_ENCRYPTION_SECRET_KEY"].encode("utf-8")
IV = os.environ["AES_ENCRYPTION_IV"].encode("utf-8")
PHENOVAR_PARTICIPANTS="phenovar_participants.csv"


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
    df["nik"] = df["encrypt_nik"].apply(lambda x: aes_decrypt(x, AES_ENCRYPTION_SECRET_KEY, IV))
    df["full_name"] = df["encrypt_full_name"].apply(lambda x: aes_decrypt(x, AES_ENCRYPTION_SECRET_KEY, IV))
    df["birth_date"] = df["encrypt_birth_date"].apply(lambda x: aes_decrypt(x, AES_ENCRYPTION_SECRET_KEY, IV))

    df = df[["id_subject", "full_name", "encrypt_full_name", "nik", "encrypt_nik", "birth_date", "encrypt_birth_date", "sex", "source", "province", "district", "creation_date", "updation_date"]]

    return df

print("Read data from source")
df = pd.read_csv(f"{PHENOVAR_PARTICIPANTS}")
print("Decrypt data")
df = transform_data(df)

print("Save decrypted data")
df.to_csv(f"decrypted_{PHENOVAR_PARTICIPANTS}", index=False)