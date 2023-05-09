import gspread
import pandas as pd
import warnings
import matplotlib.pyplot as plt
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
    dfDatas = dfDatas.set_axis(cabecalho, axis=1, copy=False)
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

@st.cache_data()
def tratamento():

    dfSimulacao, dfDatas, dfPedidos = load_sheets()

    dfDatasDiasUteis = dfDatas[dfDatas['natureza_tb1'] == 'saida'][['datas_tb1']]
    dfSimulacao = dfSimulacao[dfSimulacao['Média 3M'] != ''].iloc[:dfSimulacao.shape[0]-1]

    dfSimulacao['produto'] = dfSimulacao['Código'] + ' - ' + dfSimulacao['Descrição']

    qtdProdutosUnico = len(dfSimulacao['produto'].unique())

    tabelaGeralDataProduto = pd.merge(dfDatasDiasUteis.assign(key=1), dfSimulacao[['produto']].assign(key=1), on='key').drop('key', axis=1)
    tabelaGeralDataProduto['datas_tb1'] = pd.to_datetime(tabelaGeralDataProduto['datas_tb1'], format='%d/%m/%Y')
    tabelaGeralDataProduto = tabelaGeralDataProduto.sort_values(by='datas_tb1')
    tabelaGeralDataProduto['natureza'] = 'saida'

    dezDiasUteis = tabelaGeralDataProduto['datas_tb1'].drop_duplicates().reset_index(drop=True)
    dezDiasUteis = dezDiasUteis.loc[0:9].tolist()

    dfProdutos = dfSimulacao[['produto', 'Média 3M', 'Estoque Total', 'DEE - Dias Em Est.', 'Prev Con Mov Est(CMM)']]

    dfProdutos['Média 3M'] = dfProdutos['Média 3M'].apply(lambda x: float(x.replace(".", '').replace(',','.')))
    dfProdutos['Estoque Total'] = dfProdutos['Estoque Total'].apply(lambda x: float(x.replace(".", '').replace(',','.')))
    dfProdutos['DEE - Dias Em Est.'] = dfProdutos['DEE - Dias Em Est.'].apply(lambda x: float(x.replace(".", '').replace(',','.')))
    dfProdutos['Prev Con Mov Est(CMM)'] = dfProdutos['Prev Con Mov Est(CMM)'].apply(lambda x: float(x.replace(".", '').replace(',','.')))

    dfProdutos['consumoDiario'] = dfProdutos['Média 3M'] * 3 / 60
    dfProdutos['estoqueMinimo'] = dfProdutos['consumoDiario'] * 10

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

    #tabelaGeralDataProduto = tabelaGeralDataProduto.append(dfPedidos).sort_values(by='datas_tb1')
    tabelaGeralDataProduto = pd.concat([tabelaGeralDataProduto, dfPedidos]).sort_values(by='datas_tb1')
    tabelaGeralDataProduto = tabelaGeralDataProduto[tabelaGeralDataProduto['datas_tb1'] >= data_string].reset_index(drop=True)
    tabelaGeralDataProduto['Qde Ped'] = tabelaGeralDataProduto['Qde Ped'].astype(str)
    tabelaGeralDataProduto['Qde Ped'] = tabelaGeralDataProduto['Qde Ped'].apply(lambda x: float(x.replace(".","").replace(",",".")))
    tabelaGeralDataProduto = tabelaGeralDataProduto.replace(np.nan,0)
    tabelaGeralDataProduto = tabelaGeralDataProduto.rename(columns={'Qde Ped':'entradas'})

    qtdProdutosUnico = len(tabelaGeralDataProduto['produto'].unique())
    
    tabelaFinal = pd.DataFrame()

    # média dos primeiros dez dias úteis

    # dfDezDias = tabelaFinal[tabelaFinal['datas_tb1'].isin(dezDiasUteis)]    
    # dfDezDias['consumoDiario'] *= 10
    
    # tabelaFinal.loc[tabelaFinal["datas_tb1"].isin(dezDiasUteis), "consumoDiario"] = dfDezDias["consumoDiario"]

    # dfDezDias = pd.read_csv("dezdias.csv", sep=';', encoding='iso-8859-1')
    dfDezDias = pd.read_csv("dezdiassimulado.csv", sep=';', encoding='iso-8859-1')
    colunas = ['Recurso','Quantidade#Saída']
    dfDezDias = dfDezDias.set_axis(colunas,axis=1,copy=False)

    dfDezDias = dfDezDias.replace('"',"", regex=True)
    dfDezDias = dfDezDias.replace('=',"", regex=True)

    dfDezDias['Quantidade#Saída'] = dfDezDias['Quantidade#Saída'].replace('\.','', regex=True)
    dfDezDias['Quantidade#Saída'] = dfDezDias['Quantidade#Saída'].replace(',','.', regex=True)
    # dfDezDias['Quantidade#Saída'] = dfDezDias['Quantidade#Saída'].astype(float) * -1

    dfDezDias = dfDezDias.rename(columns={'Recurso':'produto'})

    for i in range(qtdProdutosUnico):
        
        try:

            produtosUnicos = tabelaGeralDataProduto['produto'].unique()

            produto = produtosUnicos[i]
            tabelaFiltrada = tabelaGeralDataProduto[tabelaGeralDataProduto['produto'] == produto].reset_index(drop=True)
            tabelaFiltrada.reset_index(drop=True, inplace=True) 
            tabelaFiltrada['saldoAtual'] = ''
            
            try:
                saldoAtual = dfProdutos[dfProdutos['produto'] == produto].reset_index(drop=True)['Estoque Total'][0]       
            except:
                continue
                        
            tabelaFiltrada = tabelaFiltrada.sort_values(by=['natureza'], ascending=False)
            tabelaFiltrada = tabelaFiltrada.sort_values(by=['datas_tb1'], ascending=True)
            
            tabelaFiltrada = tabelaFiltrada.reset_index(drop=True)

            tabelaFiltrada['saldoAtual'][0] = saldoAtual

            for j in range(1, len(tabelaFiltrada)):
            
                if tabelaFiltrada['datas_tb1'][j] in dezDiasUteis:
                    
                    consumoSimuladoDezDias = float(dfDezDias[dfDezDias['produto'] == tabelaFiltrada['produto'][j]]['Quantidade#Saída'].reset_index(drop=True)[0]) / 10
                    
                    consumoDiario = dfProdutos[dfProdutos['produto'] == tabelaFiltrada['produto'][j]]['consumoDiario'].reset_index(drop=True)[0]

                    if consumoSimuladoDezDias > consumoDiario:

                        consumoDiario = consumoSimuladoDezDias
                    
                    else:

                        consumoDiario = consumoDiario
                else:

                    consumoDiario = dfProdutos[dfProdutos['produto'] == tabelaFiltrada['produto'][j]]['consumoDiario'].reset_index(drop=True)[0]
                
                # consumoDiario = dfProdutos[dfProdutos['produto'] == tabelaFiltrada['produto'][j]]['consumoDiario'].reset_index(drop=True)[0]

                entrada = tabelaFiltrada['entradas'][j]
                saldoOntem = tabelaFiltrada['saldoAtual'][j-1]

                if tabelaFiltrada['natureza'][j] == 'entrada':
                    
                    tabelaFiltrada['saldoAtual'][j] = float(saldoOntem) + float(entrada)

                else:    

                    tabelaFiltrada['saldoAtual'][j] = float(saldoOntem) - float(consumoDiario)

            tabelaFinal = pd.concat([tabelaFinal, tabelaFiltrada])

        except:
            continue
        
    tabelaFinal.reset_index(drop=True, inplace=True)

    dfProdutos = dfProdutos.merge(dfDezDias, on='produto')

    tabelaFinal = tabelaFinal.merge(dfProdutos, on='produto')
    
    tabelaFinal.rename(columns={'Quantidade#Saída':'mediaDezDias'}, inplace=True)
    dfProdutos.rename(columns={'Quantidade#Saída':'mediaDezDias'}, inplace=True)
    
    for i in range(len(tabelaFinal)):
        if tabelaFinal['datas_tb1'][i] in dezDiasUteis:
            tabelaFinal['consumoDiario'][i] = float(dfProdutos[dfProdutos['produto'] == tabelaFinal['produto'][i]]['mediaDezDias'].reset_index(drop=True)) / 10
        else:
            continue

    #tabelaFinal['estoqueMinimo'] = tabelaFinal['consumoDiario'] * 10
    
    corrigido = tabelaFinal.copy()
    #corrigido = tabelaFinal.merge(corrigido)
    corrigido = corrigido[corrigido['natureza'] == 'saida'][['datas_tb1','produto', 'grupo']]

    compraMaxima = dfProdutos[['produto','Média 3M','Estoque Total','estoqueMinimo', 'consumoDiario','mediaDezDias']]
 
    corrigido['valorCorrigido'] = 0

    tbCorrigida = pd.DataFrame()

    for i in range(qtdProdutosUnico):
        
        try:
            dados = corrigido[corrigido['produto'] == dfProdutos['produto'][i]].reset_index(drop=True)
        except:
            dados = pd.DataFrame()

        if len(dados) != 0:

            if dados['grupo'][0] != 'Chapas':

                maximo = compraMaxima[compraMaxima['produto'] == compraMaxima['produto'][i]].reset_index(drop=True)[['Média 3M']].values.tolist()[0][0]

                saldoInicial = compraMaxima[compraMaxima['produto'] == compraMaxima['produto'][i]].reset_index(drop=True)[['Estoque Total']].values.tolist()[0][0]

                estoqueMinimo = compraMaxima[compraMaxima['produto'] == compraMaxima['produto'][i]].reset_index(drop=True)[['estoqueMinimo']].values.tolist()[0][0]

                mediaDezDias = float(compraMaxima[compraMaxima['produto'] == compraMaxima['produto'][i]].reset_index(drop=True)[['mediaDezDias']].values.tolist()[0][0]) / 10

                consumoDiario = compraMaxima[compraMaxima['produto'] == compraMaxima['produto'][i]].reset_index(drop=True)[['consumoDiario']].values.tolist()[0][0]

                dados['valorCorrigido'][0] = saldoInicial 

                for j in range(1,len(dados)-1):
                    
                    if dados['valorCorrigido'][j-1] <= float(estoqueMinimo):
                        
                        data = dados['datas_tb1'][j-1]
                        produto = dados['produto'][j-1]
                        grupo = dados['grupo'][j-1]
                        valorCorrigido = maximo + dados['valorCorrigido'][j-1]
                        
                        df_inserir = pd.DataFrame({'datas_tb1':[data],
                                                    'produto':[produto],
                                                        'grupo':[grupo],
                                                        'valorCorrigido':[valorCorrigido]
                                                        }, index=[j-1 + 0.5])
                        
                        dados.index = dados.index.astype('float64')

                        dados = pd.concat([dados.loc[:j-1], df_inserir, dados.loc[j:]]).reset_index(drop=True)

                        j = j + 1

                    else:
                        
                        # if dados['datas_tb1'][j] in dezDiasUteis:
                        #     dados['valorCorrigido'][j] = dados['valorCorrigido'][j-1] - mediaDezDias
                        # else:
                        #      dados['valorCorrigido'][j] = dados['valorCorrigido'][j-1] - consumoDiario

                        if dados['datas_tb1'][j] in dezDiasUteis and mediaDezDias > consumoDiario:
                            
                            dados['valorCorrigido'][j] = dados['valorCorrigido'][j-1] - mediaDezDias

                        else:
                             
                             dados['valorCorrigido'][j] = dados['valorCorrigido'][j-1] - consumoDiario

                        dimensao = dados.shape[0]

                        j = j + 1
            
            else:
                
                maximo = 10000

                saldoInicial = compraMaxima[compraMaxima['produto'] == compraMaxima['produto'][i]].reset_index(drop=True)[['Estoque Total']].values.tolist()[0][0]

                estoqueMinimo = compraMaxima[compraMaxima['produto'] == compraMaxima['produto'][i]].reset_index(drop=True)[['estoqueMinimo']].values.tolist()[0][0]

                mediaDezDias = float(compraMaxima[compraMaxima['produto'] == compraMaxima['produto'][i]].reset_index(drop=True)[['mediaDezDias']].values.tolist()[0][0]) / 10

                consumoDiario = compraMaxima[compraMaxima['produto'] == compraMaxima['produto'][i]].reset_index(drop=True)[['consumoDiario']].values.tolist()[0][0]

                

                dados = dados.reset_index(drop=True) 

                dados['valorCorrigido'][0] = saldoInicial

                j = 1
                dimensao = dados.shape[0]-1

                while j <= dimensao-1:
                    
                    if dados['valorCorrigido'][j-1] <= float(estoqueMinimo):
                        
                        data = dados['datas_tb1'][j-1]
                        produto = dados['produto'][j-1]
                        grupo = dados['grupo'][j-1]
                        valorCorrigido = maximo + dados['valorCorrigido'][j-1]
                        
                        df_inserir = pd.DataFrame({'datas_tb1':[data],
                                                    'produto':[produto],
                                                        'grupo':[grupo],
                                                        'valorCorrigido':[valorCorrigido]
                                                        }, index=[j-1 + 0.5])
                        
                        dados.index = dados.index.astype('float64')

                        dados = pd.concat([dados.loc[:j-1], df_inserir, dados.loc[j:]]).reset_index(drop=True)
                        
                        dimensao = dados.shape[0]

                        j = j + 1

                    else:
                        
                        if dados['datas_tb1'][j] in dezDiasUteis and mediaDezDias > consumoDiario:
                            
                            dados['valorCorrigido'][j] = dados['valorCorrigido'][j-1] - mediaDezDias

                        else:
                             
                             dados['valorCorrigido'][j] = dados['valorCorrigido'][j-1] - consumoDiario
                        
                        dimensao = dados.shape[0]

                        j = j + 1

            #tbCorrigida = tbCorrigida.append(dados)
            tbCorrigida = pd.concat([tbCorrigida, dados])

        else:
            continue
    
    dfProdutos['mediaDezDias'] = dfProdutos['mediaDezDias'].astype(float) / 10

    return tbCorrigida, tabelaFinal, dfProdutos

# tbCorrigida[tbCorrigida['produto'] == '313210 - CATALISADOR PU 1/1 5058 COMP B']
# tabelaFinal[tabelaFinal['produto'] == '313210 - CATALISADOR PU 1/1 5058 COMP B']
# dfProdutos[dfProdutos['produto'] == '313210 - CATALISADOR PU 1/1 5058 COMP B']

dfGrupo = pd.read_csv('grupo.csv', sep=';') 

tbGrupo = dfGrupo[['grupo']]
tbProduto = dfGrupo[['produto']]

grupoUnico = tbGrupo['grupo'].unique()
produtoUnico = tbProduto['produto'].unique()

listaGrupos = ['Selecione']
listaProdutos = ['Selecione']

for i in range(len(grupoUnico)):
    listaGrupos.append(grupoUnico[i])

for i in range(len(produtoUnico)):
    listaProdutos.append(produtoUnico[i])

with st.sidebar:
    selectGrupo = st.selectbox("Selecione o grupo: ", listaGrupos)
    selectProduto = st.selectbox("Selecione o produto: ", listaProdutos)

if selectGrupo != 'Selecione':

    # produto1='326409 - VERNIZ SINTÉTICO - TINGIDO'

    tbCorrigida, tabelaFinal, dfProdutos = tratamento()

    # tbCorrigida[tbCorrigida['produto'] == produto1]
    # tabelaFinal[tabelaFinal['produto'] == produto1]

    tbCorrigida.dropna(inplace=True)
    tabelaFinal.dropna(inplace=True)
    dfProdutos.dropna(inplace=True)

    tbCorrigida = tbCorrigida[tbCorrigida['grupo'] == selectGrupo]
    tabelaFinal = tabelaFinal[tabelaFinal['grupo'] == selectGrupo]
    
    tabelaFinal['valor_0'] = 0
    
    produtosUnico = tbCorrigida['produto'].unique()

    for produto in range(len(produtosUnico)):
        
        df_grafico = tabelaFinal[tabelaFinal['produto'] == produtosUnico[produto]]
        df_grafico1 = tbCorrigida[tbCorrigida['produto'] == produtosUnico[produto]]

        titulo = 'Produto: ' + produtosUnico[produto]

        fig = go.Figure()

        fig.add_trace(go.Scatter(x=df_grafico['datas_tb1'], y=df_grafico['saldoAtual'], mode='lines', name='Consumo real'))
        fig.add_trace(go.Scatter(x=df_grafico['datas_tb1'], y=df_grafico['valor_0'], mode='lines', name='zero'))
        fig.add_trace(go.Scatter(x=df_grafico1['datas_tb1'], y=df_grafico1['valorCorrigido'], mode='lines', name='Consumo corrigido'))
        fig.add_trace(go.Scatter(x=df_grafico['datas_tb1'], y=df_grafico['estoqueMinimo'], mode='lines', name='Estoque mínimo'))

        fig.update_xaxes(
        tickmode='array',
        tickvals=df_grafico['datas_tb1'],
        ticktext=df_grafico['datas_tb1'].dt.strftime('%d/%m/%Y'),
        ) # Formatar as datas como 'dd/mm/aaaa')

        fig.update_layout(title={'text': titulo, 'x': 0.2}, xaxis_title='Data', xaxis_tickangle=90, yaxis_title='Valor', width=800, height=600, xaxis_tickformat = '%d %B (%a)<br>%Y')

        st.plotly_chart(fig)

        dfProdutos[dfProdutos['produto'] == produtosUnico[produto]]

if selectProduto != 'Selecione':

    tbCorrigida, tabelaFinal, dfProdutos = tratamento()
    
    tbCorrigida.dropna(inplace=True)
    tabelaFinal.dropna(inplace=True)
    dfProdutos.dropna(inplace=True)

    tbCorrigida = tbCorrigida[tbCorrigida['produto'] == selectProduto]
    tabelaFinal = tabelaFinal[tabelaFinal['produto'] == selectProduto]

    tabelaFinal['valor_0'] = 0

    produtosUnico = tbCorrigida['produto'].unique()

    for produto in range(len(produtosUnico)):
        
        df_grafico = tabelaFinal[tabelaFinal['produto'] == produtosUnico[produto]]
        df_grafico1 = tbCorrigida[tbCorrigida['produto'] == produtosUnico[produto]]

        titulo = 'Produto: ' + produtosUnico[produto]

        fig = go.Figure()

        fig.add_trace(go.Scatter(x=df_grafico['datas_tb1'], y=df_grafico['saldoAtual'], mode='lines', name='Consumo real'))
        fig.add_trace(go.Scatter(x=df_grafico['datas_tb1'], y=df_grafico['valor_0'], mode='lines', name='zero'))
        fig.add_trace(go.Scatter(x=df_grafico1['datas_tb1'], y=df_grafico1['valorCorrigido'], mode='lines', name='Consumo corrigido'))
        fig.add_trace(go.Scatter(x=df_grafico['datas_tb1'], y=df_grafico['estoqueMinimo'], mode='lines', name='Estoque mínimo'))

        fig.update_layout(title=titulo, xaxis_title='Data', yaxis_title='Valor')

        st.plotly_chart(fig)

        dfProdutos[dfProdutos['produto'] == produtosUnico[produto]]