# THIS MODULE SAVES API DATA INTO THE SQL DATABASE TABLE "stock_info"
# IT USES CHUNKSIZE TO IMPROVE PERFORMANCE AND PREVENT MEMORY ISSUES
import pandas as pd
from main import get_data_daily
from db import engine


def to_sql():
    try:
        df = get_data_daily()

        # VALIDATE DATAFRAME
        if df is None or not isinstance(df, pd.DataFrame):
            return "ERROR: GET_DATA_DAILY() DID NOT RETURN A DATAFRAME"

        if df.empty:
            return "ERROR: DATAFRAME IS EMPTY. NOTHING TO INSERT."

        # CONVERT DATE TO DATETIME TYPE
        if "fecha" in df.columns:
            df["Fecha"] = pd.to_datetime(df["fecha"], errors="coerce")

        # DROP INVALID DATES
        df = df.dropna(subset=["fecha"])

        # REMOVE DUPLICATES (FECHA + SYMBOL)
        df = df.drop_duplicates(subset=["fecha", "symbol"])

        # INSERT INTO SQL DATABASE
        df.to_sql("stock_info", engine, if_exists="append", index=False, chunksize=100)

        return "TABLE UPDATED CORRECTLY"

    except Exception as e:
        return f"SOMETHING WENT WRONG! {e}"
    
