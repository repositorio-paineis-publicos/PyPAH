import duckdb
import os

_con = None

def get_con():
    global _con
    if _con is None:
        _con = duckdb.connect()
        _con.execute("INSTALL httpfs; LOAD httpfs;")
        _con.execute(f"""
            SET s3_region='auto';
            SET s3_access_key_id='{os.environ["R2_ACCESS_KEY_ID"]}';
            SET s3_secret_access_key='{os.environ["R2_SECRET_ACCESS_KEY"]}';
            SET s3_endpoint='{os.environ["R2_ENDPOINT"]}';
            SET s3_url_style='path';
        """)
    return _con