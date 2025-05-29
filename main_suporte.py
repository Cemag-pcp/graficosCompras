import gspread
import pandas as pd
import warnings
import matplotlib.pyplot as plt
import plotly.graph_objs as go
import streamlit as st
import plotly.figure_factory as ff
import time
import datetime
from datetime import datetime, timedelta
import numpy as np
from google.oauth2 import service_account
import os
import chromadb
from chromadb.utils import embedding_functions
from openai import OpenAI
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# Connect to Google Sheets
service_account_info = st.secrets["GOOGLE_SERVICE_ACCOUNT"]

scope = ['https://www.googleapis.com/auth/spreadsheets',
         "https://www.googleapis.com/auth/drive"]

warnings.filterwarnings("ignore")

@st.cache_data()
def load_sheets():

    # filename = 'service_account.json'

    credentials = service_account.Credentials.from_service_account_info(service_account_info, scopes=scope)
    sa = gspread.authorize(credentials)

    ## Conectando com google sheets e acessando testesGraficos

    sheet = 'Análise Previsão de Consumo (CMM / NTP ) DEE'
    worksheet = 'testesGraficos'

    # sa = gspread.service_account(filename)
    sh = sa.open(sheet)
    wks = sh.worksheet(worksheet)

    cabecalho = wks.row_values(2)
    
    dfDatas = wks.get()
    dfDatas = pd.DataFrame(dfDatas)
    dfDatas = dfDatas.iloc[:,0:19].set_axis(cabecalho, axis=1, copy=False)
    dfDatas = dfDatas.iloc[2:]
        
    ## Conectando com google sheets e acessando Análise Previsão de Consumo (CMM / NTP ) DEE

    sheet = 'Análise Previsão de Consumo (CMM / NTP ) DEE'
    worksheet = 'Simulação Pend. Vendas'

    # sa = gspread.service_account(filename)
    sh1 = sa.open(sheet)
    wks1 = sh1.worksheet(worksheet)
    dfSimulacao = wks1.get()
    dfSimulacao = pd.DataFrame(dfSimulacao)

    cabecalho = wks1.row_values(2)

    #tratando planilha Análise Previsão de Consumo (CMM / NTP ) DEE
    dfSimulacao = dfSimulacao.set_axis(cabecalho, axis=1)
    dfSimulacao = dfSimulacao.iloc[2:]

    ## Conectando com google sheets e acessando Análise Previsão de Consumo (CMM / NTP ) DEE

    sheet = 'Análise Previsão de Consumo (CMM / NTP ) DEE'
    worksheet = 'Dados Pedidos'

    # sa = gspread.service_account(filename)
    sh2 = sa.open(sheet)
    wks2 = sh2.worksheet(worksheet)
    dfPedidos = wks2.get()
    dfPedidos = pd.DataFrame(dfPedidos)

    dfPedidos.dropna(axis=1, inplace=True)

    cabecalho = wks2.row_values(1)
    cabecalho = cabecalho[:37]

    #tratando planilha Análise Previsão de Consumo (CMM / NTP ) DEE
    dfPedidos = dfPedidos.set_axis(cabecalho, axis=1)
    dfPedidos = dfPedidos.iloc[1:]

    ## Conectando com google sheets e acessando Análise Previsão de Consumo (CMM / NTP ) DEE

    # sheet = 'Análise Previsão de Consumo (CMM / NTP ) DEE'
    # worksheet = 'Dados Simulação'

    # sa = gspread.service_account(filename)
    # sh2 = sa.open(sheet)
    # wks2 = sh2.worksheet(worksheet)
    # planSimulacao = wks2.get()
    # planSimulacao = pd.DataFrame(planSimulacao)

    # planSimulacao.dropna(axis=1, inplace=True)

    # cabecalho = wks2.row_values(1)

    # #tratando planilha Análise Previsão de Consumo (CMM / NTP ) DEE
    # planSimulacao = planSimulacao.set_axis(cabecalho[0:23], axis=1)
    # planSimulacao = planSimulacao.iloc[1:]

    # planSimulacao['Código'] = planSimulacao['Código'] + " - " + planSimulacao['Descrição']

    grupo_df = pd.read_csv('agrupamento_chapas.csv', sep=';')

    # Filtrar linhas do DataFrame original pelos códigos na tabela de grupo
    duplicated_rows = dfSimulacao[dfSimulacao["Código"].isin(grupo_df["codigo"])].copy()

    # Mesclar as linhas duplicadas com o DataFrame de grupos
    duplicated_rows = duplicated_rows.merge(grupo_df, left_on="Código", right_on="codigo")

    # Ajustar colunas para refletir os valores do grupo
    duplicated_rows["Descrição"] = duplicated_rows["grupo"]
    duplicated_rows["Código"] = duplicated_rows["grupo"]

    # Concatenar as linhas duplicadas com o DataFrame original
    final_df = pd.concat([dfSimulacao, duplicated_rows.drop(columns=["grupo", "codigo"])])

    # Ordenar o DataFrame para manter a consistência
    final_df = final_df.reset_index(drop=True)

    final_df['Média 3M'] = final_df['Média 3M'].apply(lambda x: float(x.replace(".","").replace(",",".")) if x!='' else 0)
    final_df['Cons Mes\nAnterior'] = final_df['Cons Mes\nAnterior'].apply(lambda x: float(x.replace(".","").replace(",",".")) if x!='' else 0)
    final_df['Simulado \nPend Vendas'] = final_df['Simulado \nPend Vendas'].apply(lambda x: float(x.replace(".","").replace(",",".")) if x!='' else 0)
    final_df['Est.Almox Central'] = final_df['Est.Almox Central'].apply(lambda x: float(x.replace(".","").replace(",",".")) if x!='' else 0)
    final_df['Est. Produção'] = final_df['Est. Produção'].apply(lambda x: float(x.replace(".","").replace(",",".")) if x!='' else 0)
    final_df['Estoque Total'] = final_df['Estoque Total'].apply(lambda x: float(x.replace(".","").replace(",",".")) if x!='' else 0)
    final_df['Ped.Compras\n Pendente'] = final_df['Ped.Compras\n Pendente'].apply(lambda x: float(x.replace(".","").replace(",",".")) if x!='' else 0)
    final_df['Prev Con Mov Est(CMM)'] = final_df['Prev Con Mov Est(CMM)'].apply(lambda x: float(x.replace(".","").replace(",",".")) if x!='' else 0)
    final_df['SIMULAÇÃO / (F.Pend/Fat.MM)'] = final_df['SIMULAÇÃO / (F.Pend/Fat.MM)'].apply(lambda x: float(x.replace(".","").replace(",",".")) if x!='' else 0)
    final_df['DEE - Dias Em Est.'] = final_df['DEE - Dias Em Est.'].apply(lambda x: float(x.replace(".","").replace(",",".")) if x!='' else 0)
    final_df['Dias\nRessupr'] = final_df['Dias\nRessupr'].apply(lambda x: float(x.replace(".","").replace(",",".")) if x!='' else 0)
    final_df['Dias de seg.'] = final_df['Dias de seg.'].apply(lambda x: float(x.replace(".","").replace(",",".")) if x!='' else 0)
    final_df['Estoque Mínimo'] = final_df['Estoque Mínimo'].apply(lambda x: float(x.replace(".","").replace(",",".")) if x!='' else 0)

    final_df = final_df.groupby(["Descrição", "Código"]).sum(numeric_only=True).reset_index()
    
    dfSimulacao = final_df

    # --------------------------------------

    renomear_col = list(dfPedidos.columns)
    renomear_col[12] = 'Recurso_1'
    
    dfPedidos.columns = renomear_col 

    # Separar o código do Recurso
    dfPedidos["Código"] = dfPedidos["Recurso"].str.split(" - ").str[0]
    # dfPedidos[dfPedidos["Recurso"] == 'CHAPA LQ 6.00']
    
    # Filtrar linhas do DataFrame original pelos códigos na tabela de grupo
    duplicated_rows = dfPedidos[dfPedidos["Código"].isin(grupo_df["codigo"])].copy()
    # duplicated_rows['Código'] = duplicated_rows['Código'].apply(lambda x: f"{x} - {x}")

    # Mesclar as linhas duplicadas com o DataFrame de grupos
    duplicated_rows = duplicated_rows.merge(grupo_df, left_on="Código", right_on="codigo")

    # Ajustar colunas para refletir os valores do grupo
    duplicated_rows["Recurso"] = duplicated_rows["grupo"]
    duplicated_rows["Código"] = duplicated_rows["grupo"]
    duplicated_rows['Recurso'] = duplicated_rows['Recurso'].apply(lambda x: f"{x} - {x}")

    # Concatenar as linhas duplicadas com o DataFrame original
    final_df = pd.concat([dfPedidos, duplicated_rows.drop(columns=["grupo", "codigo"])])

    # Ordenar o DataFrame para manter a consistência
    final_df = final_df.reset_index(drop=True)
    final_df=final_df[final_df['Recurso']!='']
    
    # final_df[final_df["Recurso"] == 'CHAPA LQ 6.00']

    dfPedidos = final_df

    # dfSimulacao[dfSimulacao['Código'] == 'CHAPA LQ 6.00']
    # dfPedidos[dfPedidos['Código'] == '222404']

    return dfSimulacao, dfDatas, dfPedidos

df_simulacao, dfDatas, df_pedidos = load_sheets()

df_simulacao_teste = df_simulacao[df_simulacao['Código'] == '110322']

# buscar o consumo diário
df_simulacao_teste['consumo_diario'] = df_simulacao_teste.apply(
    lambda row: max(row['SIMULAÇÃO / (F.Pend/Fat.MM)'], row['Prev Con Mov Est(CMM)']) / 20,
    axis=1
)

# criar coluna onde mostre a data que o estoque chegará a zero
df_simulacao_teste['data_estoque_zero'] = df_simulacao_teste.apply(
    lambda row: datetime.now().date() + timedelta(days=row['Est.Almox Central'] / row['consumo_diario'])
    if row['consumo_diario'] > 0 else None,
    axis=1
)

df_simulacao_teste



