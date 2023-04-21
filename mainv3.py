import gspread
import pandas as pd
import warnings
#import matplotlib.pyplot as plt
import plotly.graph_objs as go
import streamlit as st
import plotly.figure_factory as ff
import time
import datetime
from datetime import datetime
import numpy as np

warnings.filterwarnings("ignore")

@st.cache_data()
def load_sheets():

    filename = 'service_account.json'

    ## Conectando com google sheets e acessando testesGraficos

    sheet = 'Análise Previsão de Consumo (CMM / NTP ) DEE'
    worksheet = 'testesGraficos'

    sa = gspread.service_account(filename)
    sh = sa.open(sheet)
    wks = sh.worksheet(worksheet)

    cabecalho = wks.row_values(2)
    
    dfDatas = wks.get()
    dfDatas = pd.DataFrame(dfDatas)
    dfDatas.set_axis(cabecalho, axis=1, inplace=True)
    dfDatas = dfDatas.iloc[2:]

    ## Conectando com google sheets e acessando Análise Previsão de Consumo (CMM / NTP ) DEE

    sheet = 'Análise Previsão de Consumo (CMM / NTP ) DEE'
    worksheet = 'Simulação Pend. Vendas'

    sa = gspread.service_account(filename)
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

    sa = gspread.service_account(filename)
    sh2 = sa.open(sheet)
    wks2 = sh2.worksheet(worksheet)
    dfPedidos = wks2.get()
    dfPedidos = pd.DataFrame(dfPedidos)

    dfPedidos.dropna(axis=1, inplace=True)

    cabecalho = wks2.row_values(1)

    #tratando planilha Análise Previsão de Consumo (CMM / NTP ) DEE
    dfPedidos = dfPedidos.set_axis(cabecalho, axis=1)
    dfPedidos = dfPedidos.iloc[1:]

    return dfSimulacao, dfDatas, dfPedidos

#tratando dataframes
dfSimulacao, dfDatas, dfPedidos = load_sheets()

dfDatasDiasUteis = dfDatas[dfDatas['natureza_tb1'] == 'saida'][['datas_tb1']]
dfSimulacao = dfSimulacao[dfSimulacao['Média 3M'] != ''].iloc[:dfSimulacao.shape[0]-1]

dfSimulacao['produto'] = dfSimulacao['Código'] + ' - ' + dfSimulacao['Descrição']

qtdProdutosUnico = len(dfSimulacao['produto'].unique())

tabelaGeralDataProduto = pd.merge(dfDatasDiasUteis.assign(key=1), dfSimulacao[['produto']].assign(key=1), on='key').drop('key', axis=1)
tabelaGeralDataProduto['datas_tb1'] = pd.to_datetime(tabelaGeralDataProduto['datas_tb1'], format='%d/%m/%Y')
tabelaGeralDataProduto = tabelaGeralDataProduto.sort_values(by='datas_tb1')
tabelaGeralDataProduto['natureza'] = 'saida'

dfProdutos = dfSimulacao[['produto', 'Média 3M', 'Estoque Total', 'DEE - Dias Em Est.', 'Prev Con Mov Est(CMM)']]

dfProdutos['Média 3M'] = dfProdutos['Média 3M'].apply(lambda x: float(x.replace(".", '').replace(',','.')))
dfProdutos['Estoque Total'] = dfProdutos['Estoque Total'].apply(lambda x: float(x.replace(".", '').replace(',','.')))
dfProdutos['DEE - Dias Em Est.'] = dfProdutos['DEE - Dias Em Est.'].apply(lambda x: float(x.replace(".", '').replace(',','.')))
dfProdutos['Prev Con Mov Est(CMM)'] = dfProdutos['Prev Con Mov Est(CMM)'].apply(lambda x: float(x.replace(".", '').replace(',','.')))
dfProdutos['consumoDiario'] = dfProdutos['Média 3M'] * 3 / 60
dfProdutos['estoqueMinimo'] = dfProdutos['consumoDiario'] * 5

tabelaProdutoGrupo = pd.read_csv("grupo.csv", sep=';')

dfProdutos = dfProdutos.merge(tabelaProdutoGrupo, on='produto')

dfPedidos = dfPedidos.rename(columns={'Recurso':'produto', 'Data Entrega':'datas_tb1'})
dfPedidos['natureza'] = 'entrada'
dfPedidos = dfPedidos[['produto', 'datas_tb1','natureza', 'Qde Ped']]
dfPedidos = dfPedidos.iloc[:,1:5]
dfPedidos['datas_tb1'] = pd.to_datetime(dfPedidos['datas_tb1'], format='%d/%m/%Y' )
dfPedidos = dfPedidos[['datas_tb1', 'produto', 'natureza', 'Qde Ped']]

hoje = datetime.now()
data_string = hoje.strftime('%Y-%m-%d')

tabelaGeralDataProduto = tabelaGeralDataProduto.append(dfPedidos).sort_values(by='datas_tb1')
tabelaGeralDataProduto = tabelaGeralDataProduto[tabelaGeralDataProduto['datas_tb1'] >= data_string].reset_index(drop=True)
tabelaGeralDataProduto['Qde Ped'] = tabelaGeralDataProduto['Qde Ped'].astype(str)
tabelaGeralDataProduto['Qde Ped'] = tabelaGeralDataProduto['Qde Ped'].apply(lambda x: float(x.replace(".","").replace(",",".")))
tabelaGeralDataProduto = tabelaGeralDataProduto.replace(np.nan,0)
tabelaGeralDataProduto = tabelaGeralDataProduto.rename(columns={'Qde Ped':'entradas'})

qtdProdutosUnico = len(tabelaGeralDataProduto['produto'].unique())

tabelaFinal = pd.DataFrame()

for i in range(qtdProdutosUnico):
    
    tabelaFiltrada = tabelaGeralDataProduto[tabelaGeralDataProduto['produto'] == tabelaGeralDataProduto['produto'][i]].reset_index(drop=True)
    tabelaFiltrada['saldoAtual'] = ''
    
    saldoAtual = dfProdutos[dfProdutos['produto'] == tabelaFiltrada['produto'][j]]['Estoque Total'].reset_index(drop=True)[0]       

    tabelaFiltrada['saldoAtual'][0] = saldoAtual

    for j in range(1,len(tabelaFiltrada)):

        if tabelaFiltrada['natureza'][j] == 'entrada':

            consumoDiario = dfProdutos[dfProdutos['produto'] == tabelaFiltrada['produto'][j]]['Estoque Total'].reset_index(drop=True)[0]
            entrada = tabelaGeralDataProduto['entradas'][j]
            saldoOntem = tabelaFiltrada['saldoAtual'][j-1]

            tabelaFiltrada['saldoAtual'][j] = float(entrada) + float(saldoOntem) - float(consumoDiario)

        else:
            saldoOntem = tabelaFiltrada['saldoAtual'][j-1]
            consumoDiario = dfProdutos[dfProdutos['produto'] == tabelaFiltrada['produto'][j]]['Estoque Total'].reset_index(drop=True)[0]

            tabelaFiltrada['saldoAtual'][j] = float(saldoOntem) - float(consumoDiario)

    tabelaFinal = tabelaFinal.append(tabelaFiltrada)








dfProdutos 
tabelaGeralDataProduto
