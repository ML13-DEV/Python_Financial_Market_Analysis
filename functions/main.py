import yaml
import requests
import pandas as pd
import os
from db import engine


# LOAD CONFIGURATION FROM A YAML FILE SAFELY
# RETURNS A PYTHON DICTIONARY WITH API SETTINGS
def cargar_configuracion_yaml(ruta_archivo):
    try:
        with open(ruta_archivo, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)  # SAFE_LOAD IS RECOMMENDED
        return config

    except FileNotFoundError:
        # ERROR WHEN CONFIG FILE IS NOT FOUND
        print(f"ERROR: THE CONFIGURATION FILE '{ruta_archivo}' WAS NOT FOUND.")
        return None

    except yaml.YAMLError as exc:
        # ERROR WHEN YAML SYNTAX IS INVALID
        print(f"YAML SYNTAX ERROR: {exc}")
        return None


# REQUEST DAILY STOCK DATA FROM ALPHAVANTAGE API
# RETURNS A DATAFRAME WITH ALL SYMBOLS' DAILY DATA
def get_data_daily():
    configuracion = cargar_configuracion_yaml('config.yaml')

    # VALIDATE CONFIGURATION
    if not configuracion:
        df = pd.DataFrame()
        return df

    try:
        key = os.getenv("API_KEY_ALPHAVANTAGE")   # API KEY READ FROM ENV VARIABLE
        symbols = [stock for stock in configuracion['stock_symbols']]
        funcion = configuracion['funciones']
        size = configuracion['outputsize'][1]

        datos = []

        # LOOP OVER EACH STOCK SYMBOL
        for symbol in symbols:
            try:
                url = (
                    f"https://www.alphavantage.co/query?"
                    f"function={funcion}&symbol={symbol}&outputsize={size}&apikey={key}"
                )

                r = requests.get(url)
                info = r.json()

                # CHECK IF EXPECTED DAILY DATA EXISTS IN RESPONSE
                if "Time Series (Daily)" in info:
                    info_daily = info["Time Series (Daily)"]

                    # PARSE EACH DATE ENTRY
                    for dia, values in info_daily.items():
                        fecha = pd.to_datetime(dia)
                        open = float(values.get('1. open'))
                        high = float(values.get('2. high'))
                        low = float(values.get('3. low'))

                        # HANDLE CLOSE PRICE (ADJUSTED OR REGULAR)
                        close_price = values.get('5. adjusted close', values.get('4. close'))

                        if close_price is not None:
                            close_price = float(close_price)
                        else:
                            # ERROR HANDLING FOR MISSING CLOSE PRICES
                            print(
                                f"WARNING: MISSING CLOSE PRICE FOR {symbol} ON {dia}. DATA: {values}"
                            )
                            close_price = None

                        # VOLUME FIELD DEPENDS ON WHETHER ADJUSTED CLOSE EXISTS
                        if '5. adjusted close' in values:
                            volume_key = '6. volume'
                        else:
                            volume_key = '5. volume'

                        volume = int(values.get(volume_key, 0))

                        diccionario = {
                            "Fecha": fecha,
                            "Symbol": symbol,
                            "Open": open,
                            "High": high,
                            "Low": low,
                            "Close_Price": close_price,
                            "Volume": volume
                        }

                        datos.append(diccionario)

                else:
                    # HANDLE POSSIBLE API ERROR MESSAGES
                    if "Error Message" in info:
                        print(f"API ERROR FOR {symbol}: {info['Error Message']}")
                    elif "Note" in info:
                        print(f"API NOTE FOR {symbol}: {info['Note']}")
                    else:
                        print(f"UNEXPECTED API RESPONSE FOR {symbol}: {info}")

            except (requests.exceptions.RequestException, KeyError, ValueError, TypeError) as e:
                print(
                    f"ERROR '{e}'. FAILED WHILE FETCHING DATA FOR {symbol}. CONTINUING..."
                )

        df = pd.DataFrame(datos)
        return df

    except Exception as e:
        raise e

    