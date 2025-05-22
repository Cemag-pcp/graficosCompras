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
from google.oauth2 import service_account

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
    final_df['Dias de seguranca'] = final_df['Dias de seguranca'].apply(lambda x: float(x.replace(".","").replace(",",".")) if x!='' else 0)

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

@st.cache_data()
def tratamento():

    hoje = datetime.now()
    data_string = hoje.strftime('%Y-%m-%d')

    dfSimulacao, dfDatas, dfPedidos = load_sheets()
    # dfSimulacao[dfSimulacao['Código'] == '222401']

    dfPedidos['Data Entrega'] = pd.to_datetime(dfPedidos['Data Entrega'], format='%d/%m/%Y')
    dfPedidos['Data Entrega'] = dfPedidos['Data Entrega'].apply(lambda x: hoje if x < hoje else x)
    dfPedidos['Data Entrega'] = dfPedidos['Data Entrega'].dt.strftime('%d/%m/%Y')

    dfDatasDiasUteis = dfDatas[dfDatas['natureza_tb1'] == 'saida'][['datas_tb1']]
    dfSimulacao = dfSimulacao[dfSimulacao['Média 3M'] != ''].reset_index(drop=True)

    dfSimulacao['produto'] = dfSimulacao['Código'] + ' - ' + dfSimulacao['Descrição']
    
    qtdProdutosUnico = len(dfSimulacao['produto'].unique())

    tabelaGeralDataProduto = pd.merge(dfDatasDiasUteis.assign(key=1), dfSimulacao[['Código']].assign(key=1), on='key').drop('key', axis=1)
    
    tabelaGeralDataProduto['datas_tb1'] = pd.to_datetime(tabelaGeralDataProduto['datas_tb1'], format='%d/%m/%Y')
    tabelaGeralDataProduto = tabelaGeralDataProduto.sort_values(by='datas_tb1')
    tabelaGeralDataProduto['natureza'] = 'saida'
    
    # tabelaGeralDataProduto[tabelaGeralDataProduto['produto'] == '222401 - CUBO RODA F. FUND. NOD. GGG-50" F6']

    dezDiasUteis = tabelaGeralDataProduto['datas_tb1'].drop_duplicates().reset_index(drop=True)
    dezDiasUteis = dezDiasUteis.loc[0:9].tolist()

    dfProdutos = dfSimulacao[['produto', 'Média 3M', 'Estoque Total', 'DEE - Dias Em Est.', 'Prev Con Mov Est(CMM)', 'Cons Mes\nAnterior', 'Simulado \nPend Vendas']]

    # dfProdutos[dfProdutos['produto'] == 'CHAPA LQ 6.00 - CHAPA LQ 6.00']

    # dfProdutos['Média 3M'] = dfProdutos['Média 3M'].apply(lambda x: float(x.replace(".", '').replace(',','.')))
    # dfProdutos['Estoque Total'] = dfProdutos['Estoque Total'].apply(lambda x: float(x.replace(".", '').replace(',','.')))
    # dfProdutos['DEE - Dias Em Est.'] = dfProdutos['DEE - Dias Em Est.'].apply(lambda x: float(x.replace(".", '').replace(',','.')))
    # dfProdutos['Prev Con Mov Est(CMM)'] = dfProdutos['Prev Con Mov Est(CMM)'].apply(lambda x: float(x.replace(".", '').replace(',','.')))

    #consumo diário: Média 3M * (3 / 60) 9
    #nova regra: div maior valor dos 3 ('Média 3M', 'Cons Mes\nAnterior', 'Simulado \nPend Vendas') por 20.
    
    #estoque mínimo: consumo diário * 10
    #nova regra: maior dos 3 ('Média 3M', 'Cons Mes\nAnterior', 'Simulado \nPend Vendas') valores div por 2.

    # maior valor entre as três colunas
    maior_valor = dfProdutos[['Média 3M', 'Cons Mes\nAnterior', 'Simulado \nPend Vendas']].max(axis=1)

    dfProdutos['consumoDiario'] = maior_valor / 20
    dfProdutos['estoqueMinimo'] = maior_valor / 2
    
    dfProdutos['descricao'] = dfProdutos['produto'].apply(lambda x: x.split('-', maxsplit=1)[1].rstrip())
    dfProdutos['codigo'] = dfProdutos['produto'].apply(lambda x: x.split('-', maxsplit=1)[0].rstrip())

    tabelaProdutoGrupo = pd.read_csv("grupo.csv", sep=',')
    tabelaProdutoGrupo['produto'] = tabelaProdutoGrupo['produto'].apply(lambda x: x.split('-')[0].rstrip())

    dfProdutos = dfProdutos.merge(tabelaProdutoGrupo, left_on='codigo', right_on='produto')
    # dfProdutos[dfProdutos['grupo'] == 'Cubo de roda']

    # dfProdutos['produto'] = dfProdutos['codigo'] + ' - ' + dfProdutos['descricao']

    dfPedidos = dfPedidos.rename(columns={'Recurso':'produto', 'Data Entrega':'datas_tb1'})
    dfPedidos['natureza'] = 'entrada'
    dfPedidos = dfPedidos[['produto', 'datas_tb1','natureza', 'Qde Ped']]
    dfPedidos = dfPedidos.iloc[:,0:4]
    dfPedidos['datas_tb1'] = pd.to_datetime(dfPedidos['datas_tb1'], format='%d/%m/%Y' )
    dfPedidos = dfPedidos[['datas_tb1', 'produto', 'natureza', 'Qde Ped']]
    dfPedidos["Qde Ped"] = dfPedidos["Qde Ped"].str.replace(".", "").str.replace(",", ".").astype(float)
    dfPedidos = dfPedidos.groupby(["produto", "datas_tb1","natureza"], as_index=False)["Qde Ped"].sum()
    dfPedidos['Código'] = dfPedidos['produto'].apply(lambda x: x.split('-', maxsplit=1)[0].rstrip())

    #tabelaGeralDataProduto = tabelaGeralDataProduto.append(dfPedidos).sort_values(by='datas_tb1')
    # tabelaGeralDataProduto[tabelaGeralDataProduto['produto'] == '222404 - CUBO RODA F. FUND. NOD. GGG-50 CBHM 6000']
    
    tabelaGeralDataProduto = pd.concat([tabelaGeralDataProduto, dfPedidos]).sort_values(by='datas_tb1')

    tabelaGeralDataProduto = tabelaGeralDataProduto[tabelaGeralDataProduto['datas_tb1'] >= data_string].reset_index(drop=True)
    # tabelaGeralDataProduto['Qde Ped'] = tabelaGeralDataProduto['Qde Ped'].astype(str)
    # tabelaGeralDataProduto['Qde Ped'] = tabelaGeralDataProduto['Qde Ped'].replace("","0")
    # tabelaGeralDataProduto['Qde Ped'] = tabelaGeralDataProduto['Qde Ped'].apply(lambda x: float(x.replace(".","").replace(",",".")))
    tabelaGeralDataProduto = tabelaGeralDataProduto.replace(np.nan,0)
    tabelaGeralDataProduto = tabelaGeralDataProduto.rename(columns={'Qde Ped':'entradas'})
    
    # tabelaGeralDataProduto[tabelaGeralDataProduto['produto'] == 'CHAPA LQ 6.00 - CHAPA LQ 6.00']

    qtdProdutosUnico = len(tabelaGeralDataProduto['produto'].unique())
    
    tabelaFinal = pd.DataFrame()

    # média dos primeiros dez dias úteis

    # dfDezDias = tabelaFinal[tabelaFinal['datas_tb1'].isin(dezDiasUteis)]    
    # dfDezDias['consumoDiario'] *= 10
    
    # tabelaFinal.loc[tabelaFinal["datas_tb1"].isin(dezDiasUteis), "consumoDiario"] = dfDezDias["consumoDiario"]

    # dfDezDias = pd.read_csv("dezdias.csv", sep=';', encoding='iso-8859-1')
    # dfDezDias = pd.read_csv("dezdiassimulado.csv", sep=',', encoding='iso-8859-1')
    # colunas = ['Recurso','Quantidade#Saída']
    # dfDezDias = dfDezDias.set_axis(colunas,axis=1,copy=False)

    # dfDezDias = dfDezDias.replace('"',"", regex=True)
    # dfDezDias = dfDezDias.replace('=',"", regex=True)

    # dfDezDias['Quantidade#Saída'] = dfDezDias['Quantidade#Saída'].replace('\.','', regex=True)
    # dfDezDias['Quantidade#Saída'] = dfDezDias['Quantidade#Saída'].replace(',','.', regex=True)
    # dfDezDias['Quantidade#Saída'] = dfDezDias['Quantidade#Saída'].astype(float) * -1

    # dfDezDias = dfDezDias.rename(columns={'Recurso':'produto'})

    # "CHAPA LQ 6.00 - CHAPA LQ 6.00"
    # 462

    # substring = "CHAPA LQ 6.00 - CHAPA LQ 6.00"
    # indices = [i for i, item in enumerate(tabelaGeralDataProduto['produto'].unique()) if substring in item]

    # print(f"Índices dos itens que contêm '{substring}': {indices}")
    
    produtosUnicos = tabelaGeralDataProduto['Código'].unique()
    
    for produto in produtosUnicos:
        # produto = '222404 - CUBO RODA F. FUND. NOD. GGG-50 CBHM 6000'
        try:
            tabelaFiltrada = tabelaGeralDataProduto[tabelaGeralDataProduto['Código'] == produto].copy()
            tabelaFiltrada.sort_values(by=['natureza'], ascending=False, inplace=True)
            tabelaFiltrada.sort_values(by=['datas_tb1'], ascending=True, inplace=True)
            tabelaFiltrada.reset_index(drop=True, inplace=True)
            tabelaFiltrada['saldoAtual'] = ''

            dados_produto = dfProdutos[dfProdutos['codigo'] == produto.split('-', maxsplit=1)[0].strip()]
            if dados_produto.empty:
                continue

            saldoAtual = dados_produto['Estoque Total'].values[0]
            consumo_padrao = dados_produto['consumoDiario'].values[0]
            tabelaFiltrada.loc[0, 'saldoAtual'] = saldoAtual

            for j in range(1, len(tabelaFiltrada)):
                data = tabelaFiltrada.loc[j, 'datas_tb1']
                natureza = tabelaFiltrada.loc[j, 'natureza']
                entrada = tabelaFiltrada.loc[j, 'entradas']
                saldoOntem = tabelaFiltrada.loc[j - 1, 'saldoAtual']

                # Consumo simulado
                # if data in dezDiasUteis:
                #     df_simulado = dfDezDias[dfDezDias['produto'] == produto]
                #     if not df_simulado.empty:
                #         consumoSimuladoDezDias = float(df_simulado['Quantidade#Saída'].values[0]) / 10
                #         consumoDiario = max(consumoSimuladoDezDias, consumo_padrao)
                #     else:
                #         consumoDiario = consumo_padrao
                # else:
                consumoDiario = consumo_padrao

                # Calcula saldo
                if natureza == 'entrada':
                    saldoAtualizado = float(saldoOntem) + float(entrada)
                else:
                    saldoAtualizado = float(saldoOntem) - float(consumoDiario)

                tabelaFiltrada.loc[j, 'saldoAtual'] = saldoAtualizado

            tabelaFinal = pd.concat([tabelaFinal, tabelaFiltrada], ignore_index=True)

        except Exception as e:
            print(f"Erro ao processar produto {produto}: {e}")
            continue

    tabelaFinal.reset_index(drop=True, inplace=True)
    # tabelaFinal[tabelaFinal['produto'] == 'CHAPA LQ 6.00 - CHAPA LQ 6.00']

    # dfDezDias['codigo_t'] = dfDezDias['produto'].apply(lambda x: x.split('-', maxsplit=1)[0].strip())
    # dfDezDias = dfDezDias[['codigo_t','Quantidade#Saída']]
    # dfProdutos = dfProdutos.merge(dfDezDias, left_on='codigo',right_on='codigo_t', how='left')

    dfProdutos.fillna(0, inplace=True)
    
    # tabelaFinal['codigo_z'] = tabelaFinal['produto'].apply(lambda x: x.split('-', maxsplit=1)[0].strip())
    # tabelaFinal = tabelaFinal[['codigo_z','datas_tb1','natureza','entradas','saldoAtual']]
    tabelaFinal = tabelaFinal.merge(dfProdutos, left_on='Código',right_on='codigo')
    tabelaFinal['produto'] = tabelaFinal['codigo'] + ' - ' + tabelaFinal['descricao']

    # tabelaFinal.rename(columns={'Quantidade#Saída':'mediaDezDias'}, inplace=True)
    # dfProdutos.rename(columns={'Quantidade#Saída':'mediaDezDias'}, inplace=True)
    
    # for i in range(len(tabelaFinal)):
    #     try:
    #         if tabelaFinal['datas_tb1'][i] in dezDiasUteis:
    #             tabelaFinal['consumoDiario'][i] = float(dfProdutos[dfProdutos['produto'] == tabelaFinal['produto'][i]]['mediaDezDias'].reset_index(drop=True)) / 10
    #         else:
    #             continue
    #     except:
    #         tabelaFinal['consumoDiario'][i] = 0

    corrigido = tabelaFinal.copy()

    corrigido = corrigido[corrigido['natureza'] == 'saida'][['datas_tb1','codigo','descricao', 'grupo']]

    compraMaxima = dfProdutos[['codigo','descricao','Média 3M','Estoque Total','estoqueMinimo', 'consumoDiario']]#,'mediaDezDias']]
 
    corrigido['valorCorrigido'] = 0

    tbCorrigida = pd.DataFrame()
    
    for i in range(qtdProdutosUnico):
        
        try:
            produto_atual = dfProdutos['codigo'][i]
        except:
            continue

        dados = corrigido[corrigido['codigo'] == produto_atual].copy().reset_index(drop=True)

        if dados.empty:
            continue

        info_produto = compraMaxima[compraMaxima['codigo'] == produto_atual]
        if info_produto.empty:
            continue

        grupo = dados['grupo'][0]
        is_chapa = grupo == 'Chapas'

        maximo = 10000 if is_chapa else info_produto['Média 3M'].values[0]
        saldoInicial = info_produto['Estoque Total'].values[0]
        estoqueMinimo = info_produto['estoqueMinimo'].values[0]
        # mediaDezDias = float(info_produto['mediaDezDias'].values[0]) / 10
        consumoDiario = info_produto['consumoDiario'].values[0]

        dados.loc[0, 'valorCorrigido'] = saldoInicial

        nova_lista = [dados.iloc[0].to_dict()]  # inicia com a primeira linha

        j = 1
        insercoes_consecutivas = 0
        LIMITE_INSERCOES = 2
        while j < len(dados):
            linha_anterior = nova_lista[-1]
            atual = dados.iloc[j].to_dict()
            valor_corrigido = linha_anterior['valorCorrigido']

            if valor_corrigido <= estoqueMinimo and estoqueMinimo > 0:
                if insercoes_consecutivas >= LIMITE_INSERCOES:
                    print(f"⚠️ Produto {produto_atual} preso no loop. Parando preventivamente.")
                    break  # segurança contra loop infinito

                nova_linha = {
                    'datas_tb1': linha_anterior['datas_tb1'],
                    'codigo': linha_anterior['codigo'],
                    'descricao': linha_anterior['descricao'],
                    'grupo': linha_anterior['grupo'],
                    'valorCorrigido': valor_corrigido + maximo
                }
                nova_lista.append(nova_linha)
                insercoes_consecutivas += 1
                continue
            else:
                insercoes_consecutivas = 0  # resetar quando não está reinserindo
                # if atual['datas_tb1'] in dezDiasUteis and mediaDezDias > consumoDiario:
                #     atual['valorCorrigido'] = valor_corrigido - mediaDezDias
                # else:
                atual['valorCorrigido'] = valor_corrigido - consumoDiario

                nova_lista.append(atual)
                j += 1

        dados_corrigido = pd.DataFrame(nova_lista)
        tbCorrigida = pd.concat([tbCorrigida, dados_corrigido], ignore_index=True)
    
    # dfProdutos['mediaDezDias'] = dfProdutos['mediaDezDias'].astype(float) / 10

    tabelaFinal = tabelaFinal[tabelaFinal['datas_tb1'] < max(tbCorrigida['datas_tb1'])]

    return tbCorrigida, tabelaFinal, dfProdutos

# tbCorrigida[tbCorrigida['produto'] == '313210 - CATALISADOR PU 1/1 5058 COMP B']
# tabelaFinal[tabelaFinal['produto'] == '313210 - CATALISADOR PU 1/1 5058 COMP B']
# dfProdutos[dfProdutos['produto'] == '313210 - CATALISADOR PU 1/1 5058 COMP B']

dfGrupo = pd.read_csv('grupo.csv', sep=',') 
tbGrupo = dfGrupo[['grupo']]
# tbProduto = dfGrupo[['produto']]

grupoUnico = tbGrupo['grupo'].unique()

# produtoUnico = tbProduto['produto'].unique()

tbCorrigida, tabelaFinal, dfProdutos = tratamento()

tbCorrigida.dropna(inplace=True)
tabelaFinal.dropna(inplace=True)
dfProdutos.dropna(inplace=True)

tbCorrigida['produto'] = tbCorrigida['codigo'] + ' - ' + tbCorrigida['descricao']
listaProdutos = tbCorrigida['produto'].unique().tolist()

listaProdutos.insert(0, 'Selecione')

listaGrupos = ['Selecione']
# listaProdutos = ['Selecione']

for i in range(len(grupoUnico)):
    listaGrupos.append(grupoUnico[i])

listaGrupos = [valor for valor in listaGrupos if valor and not isinstance(valor, float) and valor.strip()]

# for i in range(len(produtoUnico)):
#     listaProdutos.append(produtoUnico[i])

with st.sidebar:
    selectGrupo = st.selectbox("Selecione o grupo: ", listaGrupos)
    selectProduto  = st.selectbox("Selecione o produto: ", listaProdutos)

# selectGrupo = 'Cubo de roda'

if selectGrupo != 'Selecione':

    # produto1='240471 - CILINDRO TELESCÓPICO CBH 6T 10 OC NV'

    tbCorrigida, tabelaFinal, dfProdutos = tratamento()

    # dfProdutos[dfProdutos['produto'] == produto1]
    # tbCorrigida[tbCorrigida['produto'] == produto1]
    # tabelaFinal[tabelaFinal['produto'] == produto1]

    tbCorrigida.dropna(inplace=True)
    tabelaFinal.dropna(inplace=True)
    dfProdutos.dropna(inplace=True)

    tbCorrigida = tbCorrigida[tbCorrigida['grupo'] == selectGrupo]
    tabelaFinal = tabelaFinal[tabelaFinal['grupo'] == selectGrupo]

    tabelaFinal = tabelaFinal.sort_values(['datas_tb1', 'natureza'], ascending=[True, False]).reset_index(drop=True)
    
    tabelaFinal[(tabelaFinal['codigo'] == '222404') & (tabelaFinal['datas_tb1'] > '2025-06-11')]

    tabelaFinal['valor_0'] = 0
    
    tbCorrigida['produto'] = tbCorrigida['codigo'] + ' - ' + tbCorrigida['descricao']
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

        inicio = min(df_grafico['datas_tb1'])  # Defina a data de início com base nos rótulos originais
        fim = max(df_grafico['datas_tb1'])  # Defina a data de fim com base nos rótulos originais
        novos_rotulos = pd.date_range(start=inicio, end=fim, freq='5D')

        fig.update_layout(title={'text': titulo, 'x': 0.2}, xaxis_title='Data', xaxis_tickangle=45, yaxis_title='Valor', width=800, height=600, xaxis=dict(tickmode='array', tickvals=novos_rotulos, tickformat='%Y-%m-%d'))

        st.plotly_chart(fig)

        dfProdutos['produto'] = dfProdutos['codigo'] + ' - ' + dfProdutos['descricao']
        dfProdutos[dfProdutos['produto'] == produtosUnico[produto]]

if selectProduto != 'Selecione':

    tbCorrigida, tabelaFinal, dfProdutos = tratamento()

    tbCorrigida.dropna(inplace=True)
    tabelaFinal.dropna(inplace=True)
    dfProdutos.dropna(inplace=True)

    tbCorrigida['produto'] = tbCorrigida['codigo'] + ' - ' + tbCorrigida['descricao']
    tbCorrigida = tbCorrigida[tbCorrigida['produto'] == selectProduto]
    tabelaFinal = tabelaFinal[tabelaFinal['produto'] == selectProduto]

    tabelaFinal = tabelaFinal.sort_values(['datas_tb1', 'natureza'], ascending=[True, False]).reset_index(drop=True)

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

        inicio = min(df_grafico['datas_tb1'])  # Defina a data de início com base nos rótulos originais
        fim = max(df_grafico['datas_tb1'])  # Defina a data de fim com base nos rótulos originais
        novos_rotulos = pd.date_range(start=inicio, end=fim, freq='5D')

        fig.update_layout(title={'text': titulo, 'x': 0.2}, xaxis_title='Data', xaxis_tickangle=45, yaxis_title='Valor', width=800, height=600, xaxis=dict(tickmode='array', tickvals=novos_rotulos, tickformat='%Y-%m-%d'))

        st.plotly_chart(fig)

        dfProdutos['produto'] = dfProdutos['codigo'] + ' - ' + dfProdutos['descricao']
        dfProdutos[dfProdutos['produto'] == produtosUnico[produto]]