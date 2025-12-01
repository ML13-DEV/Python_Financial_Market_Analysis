#IMPORTANDO MÓDULOS A USAR
import pandas as pd
from sqlalchemy import text, bindparam
import datetime
import numpy as np
from db import engine

    
#FUNCTION TO OBTAIN ALL THE HISTORICAL DATA OF A SPECIFIC COMPANY
def GetHistoricalData(company):
    try:
        company_check = company.upper()
        query = text("select * from `stock_info` where Symbol = :company_check;")
        df= pd.read_sql(query, engine, params={"company_check":company_check})
        return df
        
    except Exception as e:
        df = pd.DataFrame()
        return df
    
#WE OBTAIN ALL THE INFORMATION ABOUT A COMPANY BUT WITHIN A SPECIFIED PERIOD
def GetDataPeriodically(start, end, company):
    try:
        company_check = company.upper()
        start_date = datetime.datetime(start[0], start[1], start[2])
        end_date = datetime.datetime(end[0], end[1], end[2])
        query = text("select * from `stock_info` where Symbol = :company_check and Fecha between :start_date and :end_date order by Fecha asc;")
        df = pd.read_sql(query, engine, params={"company_check":company_check, "start_date":start_date, "end_date":end_date})
        return df
    except Exception as e:
        df = pd.DataFrame()
        return df
    
#WE OBTAIN ALL THE INFORMATION FROM VARIOUS COMPANIES OVER A CERTAIN PERIOD OF TIME
def GetVariousCompanies(companies, start, end):
    try:
        start_date = datetime.datetime(start[0], start[1], start[2])
        end_date = datetime.datetime(end[0], end[1], end[2])
                    
        query = text("""select * from `stock_info` where Symbol in :companies and 
                    Fecha between :start_date and :end_date order by Fecha;""")
        df = pd.read_sql(query, engine, params={"companies":companies, "start_date":start_date, "end_date":end_date})
        return df
    except Exception as e:
        df = pd.DataFrame()
        return df
    
#WE OBTAIN THE MONTHLY VOLUME OF CERTAIN COMPANIES OVER A PERIOD OF TIME (MONTHS)   
def GetCompaniesVolumeSpan(companies, start, end):
    try:
        start_date = datetime.datetime(start[0], start[1], start[2])
        end_date = datetime.datetime(end[0], end[1], end[2])
                    
        query = text("""select Symbol as 'Empresa', month(Fecha) as 'Mes', sum(Volume) as 'Volumen mensual' from `stock_info`
                    where (Fecha between :start_date and :end_date) and (Symbol in :companies)
                    group by Symbol, month(Fecha)
                    order by month(Fecha), Symbol;""").bindparams(bindparam("companies", expanding=True))
        df = pd.read_sql(query, engine, params={"companies":companies, "start_date":start_date.date(), "end_date":end_date.date()})
        return df
    except Exception as e:
        df = pd.DataFrame()
        return df

#WE OBTAIN THE DAILY OPENING AND CLOSING VALUES OF A COMPANY IN A GIVEN PERIOD OF TIME.
def GetOpenCloseSpan(company, start, end):
    try:
        company_check = company.upper()
        start_date = datetime.date(start[0], start[1], start[2])
        end_date = datetime.date(end[0], end[1], end[2])
                    
        query = text("""select Fecha as 'Fecha' ,Symbol as 'Empresa', Open as 'Apertura' , Close_Price as 'Cierre' 
                    from `stock_info` where (Symbol = :company_check) and (Fecha between :start_date and :end_date)
                    group by Fecha, Symbol 
                    order by Fecha;""")
        df = pd.read_sql(query, engine, params={"company_check":company_check, "start_date":start_date, "end_date":end_date})
        return df
    except Exception as e:
        df = pd.DataFrame()
        print(e)
        return df
    
#WE OBTAIN THE MONTHLY VOLUME OF A COMPANY OVER A PERIOD OF TIME (MONTHS)
def GetCompanyVolumeSpan(company, start, end):
    try:
        company_check = company.upper()
        start_date = datetime.datetime(start[0], start[1], start[2])
        end_date = datetime.datetime(end[0], end[1], end[2])
        query = text("""select Symbol as 'Empresa', month(Fecha) as 'Mes', Volume as 'Volumen mensual' from `stock_info`
                where (Fecha between :start_date and :end_date) and (Symbol = :company_check)
                order by month(Fecha);""")
        df = pd.read_sql(query, engine, params={"company_check":company_check, "start_date":start_date, "end_date":end_date})
        return df
    except Exception as e:
        df = pd.DataFrame()
        return df
    
#WE OBTAIN THE ADJUSTED RETURN OF A COMPANY OVER A GIVEN PERIOD OF TIME
def GetAdjustedReturn(company, start, end):
    try:
        df = GetOpenCloseSpan(company, start, end)
        df['Retorno diario'] = round(np.log(df['Cierre'] / df['Cierre'].shift(1)), 2)
        df['Retorno anual'] = round(df['Retorno diario'].mean() * 252,2)
        df['Volatilidad anualizada'] = round(df['Retorno diario'].std() * np.sqrt(252),2)
        df['Ratio sharpe'] = round((df['Retorno anual'] - 0.0425) / df['Volatilidad anualizada'],2)
        df['Pico máximo'] = 0.00
        df['Drawdown actual'] = 0.00
        df['MDD'] = 0.00
        pico = 0.00
        minimo = df.iat[0,3]
        for i in range(len(df)):
            #Calculo el maximo 
            if df.iat[i, 3] > pico:
               pico = df.iat[i, 3]
            #Asigno el maximo
            df.iat[i, 8] = pico
            
            #Calculo el drawdown actual
            if pico > df.iat[i, 3]:
                #Asigno el drawdown
                df.iat[i,9] = round(((df.iat[i,3] / pico) - 1)*100, 2)
            
            
            
            #Calculo el minimo
            if minimo > df.iat[i, 3]:
                minimo = df.iat[i, 3]
            #Asigno el minimo
            df.iat[i,10] = minimo 

        return df
    except Exception as e:
        df = pd.DataFrame()
        return df

#WE OBTAIN THE DURATION OF THE MDD IN A CERTAIN PERIOD OF TIME.
def GetMDD_Duration(company, start, end):
    df = GetAdjustedReturn(company, start, end)
    fecha_minimo = df.loc[df['Drawdown actual'] == min(df['Drawdown actual'])]
    fila_minimo = df.loc[df['Drawdown actual'] == df['Drawdown actual'].min()]
    fecha_minimo = fila_minimo['Fecha'].iat[0]
    valor_inicial_pico_mdd = fila_minimo['Pico máximo'].iat[0]
    valor_valle = fila_minimo["Cierre"].iat[0]
    valor_mdd = fila_minimo['Drawdown actual'].iat[0]

    fila_recupero = df.loc[(df['Fecha'] >= fecha_minimo) & (df['Cierre'] >= valor_inicial_pico_mdd)]
    fecha_recuperacion = fila_recupero['Fecha'].iat[0]
    valor_alcanzado = fila_recupero['Cierre'].iat[0]


    fila_ultimo_pico = df.loc[(df['Pico máximo'] == valor_inicial_pico_mdd) & (df['Fecha'] <= fecha_minimo)]
    fecha_ultimo_pico = fila_ultimo_pico['Fecha'].iat[0]

    dias_peak_valle = fecha_minimo - fecha_ultimo_pico
    dias_valle_recover = fecha_recuperacion - fecha_minimo
    total_days = fecha_recuperacion - fecha_ultimo_pico

    diccionario = {
        "Fecha último pico" : fecha_ultimo_pico,
        "Valor incial" : valor_inicial_pico_mdd,
        "Fecha del valle" : fecha_minimo,
        "Valor del valle:": valor_valle,
        "% De pérdida" : valor_mdd,
        "Fecha de recupero": fecha_recuperacion,
        "Valor alcanzado": valor_alcanzado,
        "Dias totales en baja": dias_peak_valle,
        "Dias totales en recuperación": dias_valle_recover,
        "Días totales transcurridos": total_days
    }
    lista = [diccionario]
    df_mmd = pd.DataFrame(lista)
    return df_mmd

#WE OBTAIN THE MOVING, DAILY AND ANNUALLY VOLATILITY OVER A PERIOD OF TIME
def VolatilidadMovil(company, start, end):
    #Volatilidad Histórica Móvil: Para cada acción, calcula la volatilidad
    #  (desviación estándar de los retornos logarítmicos)
    #  en ventanas móviles de diferentes tamaños (ej. 20 días, 60 días, 252 días).
    start_date = datetime.datetime(start[0], start[1], start[2])
    end_date = datetime.datetime(end[0], end[1], end[2])
    dias = end_date - start_date
    if (dias.days) < 20:
        return "Período de tiempo muy corto, debe ser mayor o igual a 20"
    elif (dias.days) >= 20:
        #Creo el diccionario para guardar los valores y devolverlos
        diccionario = {}
        diccionario["Compañía"] = company
        #Llamo a la función GetOpenCloseSpan para que me cree el df con los datos necesarios para 
        #realizar los calculos
        df = GetOpenCloseSpan(company, start, end)

        #Creo la columna 'Retorno diario' calculando el logaritmo del valor de cierre sobre el 
        #valor de cierre del dia anterior y lo redondeo a 2 decimales
        df['Retorno diario'] = round(np.log(df['Cierre'] / df['Cierre'].shift(1)), 2)
        #Calculo la volatilidad diaria de los primeros 20 dias usando rolling y .std para la
        #desviacion estandar.
        volatilidad_20 = df['Retorno diario'].rolling(window=20).std()
        volatilidad_20 = round(volatilidad_20.iat[20],5)
        #Creo la columna 'Volatilidad diaria (20D)' la cual tendra el valor de la
        #volatilidad diaria a en 20 dias calculada en la accion anterior
        #volatilidad_20

        #Calculo la volatilidad anualizada de esos 20 dias usando la columna anteriormente
        #creada y la multiplico por la raiz de los dias totales que se hace trading en un año
        #252 aprox.
        volatilidad_20_anual = round(volatilidad_20 * np.sqrt(252),5)

        #Creo la columna 'Volatilidad anualizada (20D) la cual tendra el valor
        #obtenido en el calculo realizado en la variabla de arriba.
        #df['Volatilidad anualizada (20D)'] = volatilidad_20_anual

        #Asigno los pares llave-valor al diccionario que voy a devolver
        #creo la llave para la volatilidad diaria a 20 dias y le doy su resultado
        #lo mismo hago para la volatilidad anualizada
        diccionario["Volatilidad diaria (20D)"] = volatilidad_20
        diccionario["Volatilidad anualizada (20D)"] = volatilidad_20_anual

        if dias.days >= 40  and len(df['Retorno diario']) >= 40:
            volatilidad_40 = df['Retorno diario'].rolling(window=40).std()
            volatilidad_40 = round(volatilidad_40.iat[40], 5)
            diccionario["Volatilidad diaria (40D)"] = volatilidad_40
            diccionario['Volatilidad anualizada (40D)'] = round(volatilidad_40 * np.sqrt(252),5)
            
            if dias.days >= 60  and len(df['Retorno diario']) >= 60:
                volatilidad_60 = df['Retorno diario'].rolling(window=60).std()
                volatilidad_60 = round(volatilidad_60.iat[60], 5)
                diccionario["Volatilidad diaria (60D)"] = volatilidad_60
                diccionario['Volatilidad anualizada (60D)'] = round(volatilidad_60 * np.sqrt(252),5)
                
                if dias.days >= 80  and len(df['Retorno diario']) >= 80:
                    volatilidad_80 = df['Retorno diario'].rolling(window=80).std()
                    volatilidad_80 = round(volatilidad_80.iat[80], 5)
                    diccionario["Volatilidad diaria (80D)"] = volatilidad_80
                    diccionario['Volatilidad anualizada (80D)'] = round(volatilidad_80 * np.sqrt(252),5)

                    if dias.days >= 100 and len(df['Retorno diario']) >= 100:
                        volatilidad_100 = df['Retorno diario'].rolling(window=100).std()
                        volatilidad_100 = round(volatilidad_100.iat[100],5)
                        diccionario['Volatilidad diaria (100D)'] = volatilidad_100
                        diccionario['Volatilidad anualizada (100D)'] = round(volatilidad_100 * np.sqrt(252),5)

    return diccionario

#WE CALCULATE THE ADJUSTED RETURN FOR SEVERAL COMPANIES
def GetVariousAdjRet(companies, start, end):
    #CREAMOS UNA LISTA DONDE VAMOS A IR GUARDANDO LOS DATAFRAMES
    dataframes = []

    #RECORRO LA LISTA PROPORCIONADA DE COMPAÑÍAS Y A CADA UNA LE APLICO LA FUNCIÓN
    #DE OBTENER EL RETORNO AJUSTADO EN EL PERÍODO DE TIEMPO PROPORCIONADO
    for company in companies:
        data = GetAdjustedReturn(company, start, end)

        #COMPRUEBO QUE LA FUNCIÓN SE HAYA EJECUTADO CORRECTAMENTE Y RESETEO EL INDEX Y AGREGO
        #EL DATAFRAME A LA LISTA INICIAL
        if not data.empty:
            data_reset = data.reset_index() 
            dataframes.append(data_reset)
    
    #SI LA LISTA SIGUE VACÍA, DEVUELVO UN DATAFRAME VACÍO
    if not dataframes:
        return pd.DataFrame()
    
    #FINALMENTE, SI SE OBTIENEN LOS DATOS, CONCATENO LOS DATAFRAMES DE LA LISTA
    # Y LE PASO ignore_index=True PARA QUE NO TENGA EN CUENTA EL ÍNDICE A LA HORA DE CONCATENAR
    # Y NO LOS DUPLIQUE
    df = pd.concat(dataframes, ignore_index=True)

    #DEVUELVO EL DATAFRAME FINAL
    return df

#HERE WE GET THE CORRELATION BETWEEN 2 OR MORE COMPANIES IN A SPECIFIC PERIOD OF TIME
def GetCompaniesCorrInSpan(empresas, inicio, fin):
    data = GetVariousAdjRet(empresas, inicio, fin)

    df = data.pivot_table(values='Retorno diario',index='Fecha', columns='Empresa')
    df_2 = df.corr()

    return df_2

#THIS FUNCTION GIVES US STATISTICS ABOUT THE DAILY RETURN OF 2 OR MORE COMPANIES
def DailyReturnStats(companies, start, end):
    df = GetVariousAdjRet(companies,start,end)
    nuevos_dfs = []
    #Para cada empresa en tu DataFrame df_global_retornos_ajustados, calcula el promedio, 
    # la desviación estándar, el valor mínimo y el valor máximo de los "Retornos diarios".
    
    for company in companies:
        temp_df = df[df["Empresa"] == company]
        diccionario = {
            "Empresa": company,
            "Promedio RD": round(temp_df['Retorno diario'].mean(), 5),
            "Desviación estándar RD": round(temp_df['Retorno diario'].std(), 5),
            "Valor mínimo RD": round(temp_df['Retorno diario'].min(), 5),
            "Valor máximo RD": round(temp_df['Retorno diario'].max(), 5)
        }
        nuevos_dfs.append(diccionario)
        
    df_final = pd.DataFrame(nuevos_dfs)
    return df_final
    
#THIS FUNCTION CALCULATES THE 1% AND 99% QUANTILES OF THE DAILY RETURNS FOR EACH COMPANY 
# IN A GIVEN LIST AND TIME PERIOD.
def GetQuantiilesCompanies(companies,start,end):
    df = GetVariousAdjRet(companies,start,end)
    cuantiles = df.groupby("Empresa")["Retorno diario"].quantile([0.01,0.99])
    nuevo_df = cuantiles.unstack(level=-1)
    nuevo_df.columns = ["Cuantil 0.01", "Cuantil 0.99"]
    nuevo_df = nuevo_df.reset_index()
    return nuevo_df


