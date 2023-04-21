import gspread
import pandas as pd
import warnings
#import matplotlib.pyplot as plt
import plotly.graph_objs as go
import streamlit as st
import plotly.figure_factory as ff
import time

warnings.filterwarnings("ignore")

filename = 'service_account.json'

## Conectando com google sheets e acessando testesGraficos

sheet = 'Análise Previsão de Consumo (CMM / NTP ) DEE'
worksheet = 'testesGraficos'

sa = gspread.service_account(filename)
sh = sa.open(sheet)
wks = sh.worksheet(worksheet)

## Conectando com google sheets e acessando Análise Previsão de Consumo (CMM / NTP ) DEE

sheet = 'Análise Previsão de Consumo (CMM / NTP ) DEE'
worksheet = 'Simulação Pend. Vendas'

sa = gspread.service_account(filename)
sh1 = sa.open(sheet)
wks1 = sh1.worksheet(worksheet)
df = wks1.get()
df = pd.DataFrame(df)

cabecalho = wks1.row_values(2)

#tratando planilha Análise Previsão de Consumo (CMM / NTP ) DEE
df = df.set_axis(cabecalho, axis=1)
df = df.iloc[2:]
df['produto'] = df['Código'] + " - " + df['Descrição']

df = df[['produto', 'Média 3M', 'Cons Mes\nAnterior', 'DEE - Dias Em Est.']]
df = df[df['Média 3M']!=''].reset_index(drop=True)
df = df.iloc[:df.shape[0]-1]

#df.columns

#transformando coluna de média 3m em númerico
df['Média 3M'] = df['Média 3M'].apply(lambda x: float(x.replace('.', '').replace(',', '.')))

#carregando arquivo com nome de produto e grupos
grupo = pd.read_csv("grupo.csv", sep=';')

#juntando os dois dataframes "grupo" e "df", para adicionar a coluna de média 3m
grupo = grupo.merge(df, on='produto')

grupoUnico = grupo['grupo'].unique()

st.title("Gráficos")

lista_grupos = []
lista_grupos.append("Selecione")

for i in range(len(grupoUnico)):
    lista_grupos.append(grupoUnico[i])

selectGrupo  = st.selectbox("Escolha um grupo de material: ", lista_grupos)

@st.cache()
def load_data(grupo):
    
    df = pd.DataFrame()

    my_bar = st.progress(0)

    for produto in range(len(grupo)):
        
        time.sleep(2)

        my_bar.progress(produto + 1)

        wks.update('AA2', grupo['produto'][produto])
        
        if grupo['grupo'][produto] == 'Chapas':
            wks.update('S3', 10000)
        else:
            wks.update('S3', grupo['Média 3M'][produto])

        headers = wks.row_values(2)

        base = wks.get()
        base = pd.DataFrame(base)
        base = base.set_axis(headers, axis=1, inplace=False)[2:]
        
        ## Tratando planilha 

        base = base.iloc[:,5:15].dropna(axis=0)

        teste = base[['datas_tb2','saldo atual_tb2', 'estoque minimo_tb2', 'corrigido_tb2', 'data corrigida_tb2', 'entradas_tb2']]
        teste['datas_tb2'] = pd.to_datetime(teste['datas_tb2'], format='%d/%m/%Y')
        teste['data corrigida_tb2'] = pd.to_datetime(teste['data corrigida_tb2'], format='%d/%m/%Y')

        teste['saldo atual_tb2'] = teste['saldo atual_tb2'].apply(lambda x: float(x.replace('.', '').replace(',', '.')))
        teste['estoque minimo_tb2'] = teste['estoque minimo_tb2'].apply(lambda x: float(x.replace('.', '').replace(',', '.')))
        teste['corrigido_tb2'] = teste['corrigido_tb2'].apply(lambda x: float(x.replace('.', '').replace(',', '.')))

        teste['saldo atual_tb2'] = teste['saldo atual_tb2'].astype(float)
        teste['produto'] = grupo['produto'][produto]

        teste['grupo'] = ''
        teste['grupo'] = grupo['grupo'][produto]
        
        df = df.append(teste, ignore_index=True)
    
    df.to_csv("dados.csv")

    return df

if st.button("Atualizar"):
    load_data(grupo)

if selectGrupo != 'Selecione':
    
    df = pd.read_csv('dados.csv')
    #st.dataframe(df)
    
    df = df[df['grupo'] == selectGrupo].reset_index(drop=True)
    produtosUnico = df['produto'].unique()

    for produto in range(len(produtosUnico)):

        df_grafico = df[df['produto'] == produtosUnico[produto]]

        titulo = 'Produto: ' + produtosUnico[produto]

        fig = go.Figure()

        fig.add_trace(go.Scatter(x=df_grafico['datas_tb2'], y=df_grafico['saldo atual_tb2'], mode='lines', name='Consumo real'))
        fig.add_trace(go.Scatter(x=df_grafico['data corrigida_tb2'], y=df_grafico['corrigido_tb2'], mode='lines', name='Consumo corrigido'))
        fig.add_trace(go.Scatter(x=df_grafico['datas_tb2'], y=df_grafico['estoque minimo_tb2'], mode='lines', name='Estoque mínimo'))

        fig.update_layout(title=titulo, xaxis_title='Data', yaxis_title='Valor')
        
        st.plotly_chart(fig)

## Plotando gráficos