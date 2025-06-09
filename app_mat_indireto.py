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
import plotly.graph_objects as go

from sugestoes import sugestoes
from utils import adicionar_dias_uteis, formatar_data_brasileiro, processar_qde_ped, ajustar_estoque, flag_urgencia

# Connect to Google Sheets
service_account_info = st.secrets["GOOGLE_SERVICE_ACCOUNT"]

scope = ['https://www.googleapis.com/auth/spreadsheets',
         "https://www.googleapis.com/auth/drive"]

warnings.filterwarnings("ignore")

@st.cache_data()
def load_sheets():

    credentials = service_account.Credentials.from_service_account_info(service_account_info, scopes=scope)
    sa = gspread.authorize(credentials)
    sh = sa.open_by_key('1s6w-B4kqHiSOk6l9m8jA67-43b6l_ejWN-E2SWavoOs')
        
    ## Conectando com google sheets e acessando Análise Previsão de Consumo (Materiais Custo Indireto "Requisitados")

    worksheet = 'Análise Previsão de Consumo'

    # sa = gspread.service_account(filename)
    wks1 = sh.worksheet(worksheet)
    dfSimulacao = wks1.get()
    dfSimulacao = pd.DataFrame(dfSimulacao)

    cabecalho = wks1.row_values(2)

    #tratando planilha Análise Previsão de Consumo (Materiais Custo Indireto "Requisitados")
    dfSimulacao = dfSimulacao.iloc[:,:len(cabecalho)].set_axis(cabecalho, axis=1)
    dfSimulacao = dfSimulacao.iloc[2:]

    ## Conectando com google sheets e acessando Análise Previsão de Consumo (Materiais Custo Indireto "Requisitados")

    worksheet = 'Dados Pedidos'

    # sa = gspread.service_account(filename)
    wks2 = sh.worksheet(worksheet)
    dfPedidos = wks2.get()
    dfPedidos = pd.DataFrame(dfPedidos)

    # dfPedidos.dropna(axis=1)

    cabecalho = wks2.row_values(1)
    cabecalho = cabecalho[:37]

    #tratando planilha Análise Previsão de Consumo (CMM / NTP ) DEE
    dfPedidos = dfPedidos.iloc[:,:len(cabecalho)].set_axis(cabecalho, axis=1)
    dfPedidos = dfPedidos.iloc[1:]

    final_df = dfSimulacao.copy()

    # Ordenar o DataFrame para manter a consistência
    final_df = final_df.reset_index(drop=True)

    final_df['Média 3M'] = final_df['Média 3M'].apply(lambda x: float(x.replace(".","").replace(",",".")) if x!='' else 0)
    final_df['Cons Mes\nAnterior'] = final_df['Cons Mes\nAnterior'].apply(lambda x: float(x.replace(".","").replace(",",".")) if x!='' else 0)
    # final_df['Simulado \nPend Vendas'] = final_df['Simulado \nPend Vendas'].apply(lambda x: float(x.replace(".","").replace(",",".")) if x!='' else 0)
    final_df['Est.Almox Central'] = final_df['Est.Almox Central'].apply(lambda x: float(x.replace(".","").replace(",",".")) if x!='' else 0)
    # final_df['Est. Produção'] = final_df['Est. Produção'].apply(lambda x: float(x.replace(".","").replace(",",".")) if x!='' else 0)
    # final_df['Estoque Total'] = final_df['Estoque Total'].apply(lambda x: float(x.replace(".","").replace(",",".")) if x!='' else 0)
    # final_df['Est.Almox Central'] = final_df['Est.Almox Central'].apply(lambda x: float(x.replace(".","").replace(",",".")) if x!='' else 0)
    final_df['Ped.Compra\n Pendente'] = final_df['Ped.Compra\n Pendente'].apply(lambda x: float(x.replace(".","").replace(",",".")) if x!='' else 0)
    final_df['Prev Con Mov Est(CMM)'] = final_df['Prev Con Mov Est(CMM)'].apply(lambda x: float(x.replace(".","").replace(",",".")) if x!='' else 0)
    # final_df['SIMULAÇÃO / (F.Pend/Fat.MM)'] = final_df['SIMULAÇÃO / (F.Pend/Fat.MM)'].apply(lambda x: float(x.replace(".","").replace(",",".")) if x!='' else 0)
    final_df['DEE - Dias Em Est.'] = final_df['DEE - Dias Em Est.'].apply(lambda x: float(x.replace(".","").replace(",",".")) if x!='' else 0)
    final_df['Dias\nRessupr'] = final_df['Dias\nRessupr'].apply(lambda x: float(x.replace(".","").replace(",",".")) if x!='' else 0)
    final_df['Dias de seguranca'] = final_df['Dias de seguranca'].apply(lambda x: float(x.replace(".","").replace(",",".")) if x!='' else 10)
    final_df['Estoque Mínimo'] = (final_df['Prev Con Mov Est(CMM)'] / 20 ) * final_df['Dias de seguranca']

    dfSimulacao = final_df

    # --------------------------------------

    renomear_col = list(dfPedidos.columns)
    renomear_col[12] = 'Recurso_1'
    
    dfPedidos.columns = renomear_col 

    # Separar o código do Recurso
    dfPedidos["Código"] = dfPedidos["Recurso"].str.split(" - ").str[0]
    # dfPedidos[dfPedidos["Recurso"] == 'CHAPA LQ 2.00 x 1500']
    
    # Ordenar o DataFrame para manter a consistência
    final_df = final_df.reset_index(drop=True)
    final_df=final_df[final_df['Código']!='']
    
    dfPedidos = final_df

    # dfSimulacao[dfSimulacao['Código'] == 'CHAPA LQ 6.00']
    # dfPedidos[dfPedidos['Código'] == '222404']

    return dfSimulacao, dfPedidos

def tratamento_geral():

    df_simulacao, df_pedidos = load_sheets()

    df_pedidos['Data Entrega'] = pd.to_datetime(df_pedidos['Data Entrega'], format='%d/%m/%Y', errors='coerce')
    # tudo que for sabado joga pra sexta e tudo que for domingo joga pra segunda
    df_pedidos['Data Entrega'] = df_pedidos['Data Entrega'].apply(
        lambda x: x - timedelta(days=1) if x.weekday() == 5 else (x + timedelta(days=1) if x.weekday() == 6 else x)
    )

    grupos_df = pd.read_csv('grupos_atualizados_indireto.csv', sep=',')
    # grupos_df['Código'] = grupos_df['produto'].apply(lambda x: str(x).split(' - ', maxsplit=1)[0])
    grupos_df = grupos_df[['Código', 'grupo']]

    df_simulacao_teste = df_simulacao

    df_simulacao_teste = df_simulacao_teste.merge(grupos_df, on='Código', how='left').fillna({'grupo': 'Sem Grupo'})

    # apenas para chapas agrupadas fazer o calculo de DEE

    # buscar o consumo diário
    df_simulacao_teste['consumo_diario'] = df_simulacao_teste.apply(
        lambda row: row['Prev Con Mov Est(CMM)'] / 20,
        axis=1
    )

    # criar coluna onde mostre a data que o estoque chegará a zero
    df_simulacao_teste['data_estoque_zero'] = df_simulacao_teste.apply(
        lambda row: timedelta(days=row['Est.Almox Central'] / row['consumo_diario'])
        if row['consumo_diario'] > 0 else 0,
        axis=1
    )

    # Aplicação no seu DataFrame - VERSÃO CORRETA
    df_simulacao_teste['dias_ate_estoque_minimo'] = df_simulacao_teste.apply(
        lambda row: (row['Est.Almox Central'] - row['Estoque Mínimo']) / row['consumo_diario']
        if row['consumo_diario'] > 0 else None,
        axis=1
    )

    # Agora soma os dias pulando apenas fins de semana
    df_simulacao_teste['data_estoque_minimo'] = df_simulacao_teste['dias_ate_estoque_minimo'].apply(
        lambda dias: adicionar_dias_uteis(datetime.now().date(), dias)
    )

    # Aplicação no seu DataFrame - VERSÃO CORRETA
    df_simulacao_teste['dias_ate_estoque_zero'] = df_simulacao_teste.apply(
        lambda row: row['Est.Almox Central'] / row['consumo_diario']
        if row['consumo_diario'] > 0 else None,
        axis=1
    )

    # Agora soma os dias pulando apenas fins de semana
    df_simulacao_teste['data_estoque_zero'] = df_simulacao_teste['dias_ate_estoque_zero'].apply(
        lambda dias: adicionar_dias_uteis(datetime.now().date(), dias)
    )

    # Data para solicitar a compra ao fornecedor colunas: Dias\nRessupr e data_estoque_minimo, usar funcao de adicionar_dias_uteis
    df_simulacao_teste['data_compra'] = df_simulacao_teste.apply(
        lambda row: adicionar_dias_uteis(row['data_estoque_minimo'], (row['Dias\nRessupr']) * -1)
        if pd.notna(row['data_estoque_minimo']) and pd.notna(row['Dias\nRessupr']) else None,
        axis=1
    )

    df_simulacao_teste.drop(columns=['dias_ate_estoque_minimo', 'dias_ate_estoque_zero'], inplace=True)

    df_simulacao_teste['Est.Almox Central'] = df_simulacao_teste.apply(
        lambda row: ajustar_estoque(row, df_pedidos),
        axis=1
    )

    # recalcula a data para estoque minimo
    df_simulacao_teste['data_estoque_minimo'] = df_simulacao_teste.apply(
        lambda row: adicionar_dias_uteis(row['data_compra'], row['Dias\nRessupr'])
        if pd.notna(row['data_compra']) and pd.notna(row['Dias\nRessupr']) else None,
        axis=1
    )

    # Aplicação no seu DataFrame - VERSÃO CORRETA
    df_simulacao_teste['dias_ate_estoque_minimo'] = df_simulacao_teste.apply(
        lambda row: (row['Est.Almox Central'] - row['Estoque Mínimo']) / row['consumo_diario']
        if row['consumo_diario'] > 0 else None,
        axis=1
    )

    # Agora soma os dias pulando apenas fins de semana
    df_simulacao_teste['data_estoque_minimo'] = df_simulacao_teste['dias_ate_estoque_minimo'].apply(
        lambda dias: adicionar_dias_uteis(datetime.now().date(), dias)
    )

    df_simulacao_teste['dias_ate_estoque_zero'] = df_simulacao_teste.apply(
        lambda row: row['Est.Almox Central'] / row['consumo_diario']
        if row['consumo_diario'] > 0 else None,
        axis=1
    )

    # criar coluna onde mostre a data que o estoque chegará a zero
    df_simulacao_teste['data_estoque_zero'] = df_simulacao_teste.apply(
        lambda row: timedelta(days=row['Est.Almox Central'] / row['consumo_diario'])
        if row['consumo_diario'] > 0 else None,
        axis=1
    )

    # Agora soma os dias pulando apenas fins de semana
    df_simulacao_teste['data_estoque_zero'] = df_simulacao_teste['dias_ate_estoque_zero'].apply(
        lambda dias: adicionar_dias_uteis(datetime.now().date(), dias)
    )

    # Data para solicitar a compra ao fornecedor colunas: Dias\nRessupr e data_estoque_minimo, usar funcao de adicionar_dias_uteis
    df_simulacao_teste['data_compra'] = df_simulacao_teste.apply(
        lambda row: adicionar_dias_uteis(row['data_estoque_minimo'], (row['Dias\nRessupr'] + 1) * -1)
        if pd.notna(row['data_estoque_minimo']) and pd.notna(row['Dias\nRessupr']) else None,
        axis=1
    )

    # dias para data_compra
    df_simulacao_teste['dias_ate_data_compra'] = df_simulacao_teste.apply(
        lambda row: np.busday_count(datetime.now().date(), row['data_compra'])
        if pd.notna(row['data_compra']) else None,
        axis=1
    )

    df_simulacao_teste['flag_urgencia'] = df_simulacao_teste['dias_ate_data_compra'].apply(flag_urgencia)

    df_simulacao_teste['data_estoque_zero'] = df_simulacao_teste['data_estoque_zero'].apply(formatar_data_brasileiro)
    df_simulacao_teste['data_estoque_minimo'] = df_simulacao_teste['data_estoque_minimo'].apply(formatar_data_brasileiro)
    df_simulacao_teste['data_compra'] = df_simulacao_teste['data_compra'].apply(formatar_data_brasileiro)

    # coluna se existe ou não algum pedido de compra
    df_simulacao_teste['tem_pedido_de_compra'] = df_simulacao_teste['Ped.Compra\n Pendente'].apply(lambda x: 'SIM' if x else 'NÃO')

    #retirar primeira linha
    df_simulacao_teste = df_simulacao_teste.iloc[1:]

    return df_simulacao_teste, df_pedidos

def plotar_grafico_serrote(produto_escolhido,grupo_escolhido,df_simulacao, df_pedidos):

    if grupo_escolhido:

        produtos_do_grupo = df_simulacao[df_simulacao['grupo'] == grupo_escolhido]['Código'].unique()

        for produto in produtos_do_grupo:

            # Pega apenas a linha do produto escolhido
            linha_produto = df_simulacao[df_simulacao['Código'] == produto].iloc[0]

            estoque_atual = linha_produto['Est.Almox Central']
            estoque_minimo = linha_produto['Estoque Mínimo']
            consumo_diario = linha_produto['consumo_diario']

            # Gera datas de hoje até hoje + 60 dias (dias corridos)
            # datas = pd.date_range(start=datetime.now().date(), periods=61, freq='D')
            datas = pd.date_range(start=datetime.now().date(), periods=61, freq='B')

            # Inicializa a lista de estoques para cada dia
            estoque_diario = []
            estoque_atual_dia = estoque_atual
            datas_grafico = []
            estoque_diario = []

            for data in datas:
                # Consome primeiro
                estoque_atual_dia -= consumo_diario
                datas_grafico.append(data)
                estoque_diario.append(estoque_atual_dia)

                # Depois, se houver pedido de compra no dia, adiciona no MESMO DIA
                pedidos_do_dia = df_pedidos[
                    (df_pedidos['Código'] == produto) &
                    (df_pedidos['Data Entrega'] == data)
                ]

                if not pedidos_do_dia.empty:
                    estoque_atual_dia += pedidos_do_dia['Qde Ped Corrigido'].sum()
                    datas_grafico.append(data)  # o mesmo dia, mas agora “pós entrada”
                    estoque_diario.append(estoque_atual_dia)

            # Inicializa listas
            datas_ideal_grafico = []
            estoque_ideal_diario = []

            estoque_ideal = estoque_atual
            valor_ideal_compra = linha_produto['Dias\nRessupr'] * linha_produto['consumo_diario']

            for data in datas:
                # Consome
                estoque_ideal -= consumo_diario

                # Armazena saldo depois do consumo
                datas_ideal_grafico.append(data)
                estoque_ideal_diario.append(estoque_ideal)

                # Se chegou no estoque mínimo ou abaixo, "compra" no mesmo dia
                if estoque_ideal <= estoque_minimo:
                    estoque_ideal += valor_ideal_compra  # reabastece
                    # Armazena ponto da subida (mesmo dia, vertical)
                    datas_ideal_grafico.append(data)
                    estoque_ideal_diario.append(estoque_ideal)

            # Cria o gráfico
            fig = go.Figure()

            # Linha horizontal do estoque mínimo
            fig.add_trace(go.Scatter(
                x=datas,
                y=[estoque_minimo] * len(datas),
                mode='lines',
                name='Estoque Mínimo',
                line=dict(dash='solid', color='red')
            ))

            # Linha horizontal do zero
            fig.add_trace(go.Scatter(
                x=datas,
                y=[0] * len(datas),
                mode='lines',
                name='Estoque zero',
                line=dict(dash='solid', color='gray')
            ))

            # Linha consumo real
            fig.add_trace(go.Scatter(
                x=datas_grafico,
                y=estoque_diario,
                mode='lines',
                name='Consumo real',
                line=dict(dash='solid', color='blue')
            ))

            # Linha de consumo ideal (serrote automático)
            fig.add_trace(go.Scatter(
                x=datas_ideal_grafico,
                y=estoque_ideal_diario,
                mode='lines',
                name='Consumo Ideal',
                line=dict(dash='solid', color='green')
            ))

            # Ajustes visuais
            fig.update_layout(
                title=f"{produto} - {linha_produto['Descrição']}",
                xaxis_title='Data',
                yaxis_title='Estoque',
                width=900,
                height=600,
                xaxis_tickangle=45,
                xaxis=dict(type='category', tickformat='%Y-%m-%d')  # Isso força eixo X categórico
            )

            # Plota no Streamlit
            st.plotly_chart(fig)
            sugestoes(produto, df_pedidos, estoque_atual, estoque_minimo, linha_produto)

    else:

        try:
            # Pega apenas a linha do produto escolhido
            linha_produto = df_simulacao[df_simulacao['Código'] == produto_escolhido].iloc[0]
        except IndexError:
            st.error(f'No filtro de "Produto" escolha a opção "Selecione..."')
            return

        estoque_atual = linha_produto['Est.Almox Central']
        estoque_minimo = linha_produto['Estoque Mínimo']
        consumo_diario = linha_produto['consumo_diario']

        # Gera datas de hoje até hoje + 60 dias (dias corridos)
        # datas = pd.date_range(start=datetime.now().date(), periods=61, freq='D')
        datas = pd.date_range(start=datetime.now().date(), periods=61, freq='B')

        # Inicializa a lista de estoques para cada dia
        estoque_diario = []
        estoque_atual_dia = estoque_atual
        datas_grafico = []
        estoque_diario = []

        for data in datas:
            # Consome primeiro
            estoque_atual_dia -= consumo_diario
            datas_grafico.append(data)
            estoque_diario.append(estoque_atual_dia)

            # Depois, se houver pedido de compra no dia, adiciona no MESMO DIA
            pedidos_do_dia = df_pedidos[
                (df_pedidos['Código'] == produto_escolhido) &
                (df_pedidos['Data Entrega'] == data)
            ]

            if not pedidos_do_dia.empty:
                estoque_atual_dia += pedidos_do_dia['Qde Ped Corrigido'].sum()
                datas_grafico.append(data)  # o mesmo dia, mas agora “pós entrada”
                estoque_diario.append(estoque_atual_dia)

        # Inicializa listas
        datas_ideal_grafico = []
        estoque_ideal_diario = []

        estoque_ideal = estoque_atual
        valor_ideal_compra = linha_produto['Dias\nRessupr'] * linha_produto['consumo_diario']

        for data in datas:
            # Consome
            estoque_ideal -= consumo_diario

            # Armazena saldo depois do consumo
            datas_ideal_grafico.append(data)
            estoque_ideal_diario.append(estoque_ideal)

            # Se chegou no estoque mínimo ou abaixo, "compra" no mesmo dia
            if estoque_ideal <= estoque_minimo:
                estoque_ideal += valor_ideal_compra  # reabastece
                # Armazena ponto da subida (mesmo dia, vertical)
                datas_ideal_grafico.append(data)
                estoque_ideal_diario.append(estoque_ideal)

        # Cria o gráfico
        fig = go.Figure()

        # Linha horizontal do estoque mínimo
        fig.add_trace(go.Scatter(
            x=datas,
            y=[estoque_minimo] * len(datas),
            mode='lines',
            name='Estoque Mínimo',
            line=dict(dash='solid', color='red')
        ))

        # Linha horizontal do zero
        fig.add_trace(go.Scatter(
            x=datas,
            y=[0] * len(datas),
            mode='lines',
            name='Estoque zero',
            line=dict(dash='solid', color='gray')
        ))

        # Linha consumo real
        fig.add_trace(go.Scatter(
            x=datas_grafico,
            y=estoque_diario,
            mode='lines',
            name='Consumo real',
            line=dict(dash='solid', color='blue')
        ))

        # Linha de consumo ideal (serrote automático)
        fig.add_trace(go.Scatter(
            x=datas_ideal_grafico,
            y=estoque_ideal_diario,
            mode='lines',
            name='Consumo Ideal',
            line=dict(dash='solid', color='green')
        ))

        # Ajustes visuais
        fig.update_layout(
            title=f"{produto_escolhido} - {linha_produto['Descrição']}",
            xaxis_title='Data',
            yaxis_title='Estoque',
            width=900,
            height=600,
            xaxis_tickangle=45,
            xaxis=dict(type='category', tickformat='%Y-%m-%d')  # Isso força eixo X categórico
        )

        # Plota no Streamlit
        st.plotly_chart(fig)
        sugestoes(produto_escolhido, df_pedidos, estoque_atual, estoque_minimo, linha_produto)
                
#jogar essa tabela no streamlit, apenas em formato de tabela, sem gráficos

def main(df_simulacao_teste, df_pedidos, plotar_grafico_serrote):
    
    st.title('Análise de Consumo e Previsão de Estoque - Mat indireto')

    aba1, aba2 = st.tabs(["🔍 Visualização e Filtros", "📜 Regras"])
        
    df_simulacao_teste['codigo_descricao'] = df_simulacao_teste['Código'] + ' - ' + df_simulacao_teste['Descrição']

    with aba1:
        with st.sidebar:
            produto_escolhido = st.selectbox(
                "Selecione o produto:",
                ["Selecione..."] + list(df_simulacao_teste['codigo_descricao'].unique())
            )
            grupo_escolhido = st.selectbox(
                "Selecione o grupo:",
                ["Selecione..."] + list(df_simulacao_teste['grupo'].unique())
            )

            filtro = st.radio(
                "Filtros adicionais",
                (
                    "Mostrar tudo",
                    "Apenas itens urgentes",
                    "Apenas itens com prazo curto",
                    "Apenas itens que vencem hoje"
                )
            )

        df_filtrado = df_simulacao_teste.copy()

        if filtro == "Apenas itens urgentes":
            df_filtrado = df_filtrado[df_filtrado["dias_ate_data_compra"] < 0]
        elif filtro == "Apenas itens com prazo curto":
            df_filtrado = df_filtrado[(df_filtrado["dias_ate_data_compra"] >= 0) & (df_filtrado["dias_ate_data_compra"] <= 5)]
        elif filtro == "Apenas itens que vencem hoje":
            df_filtrado = df_filtrado[df_filtrado["dias_ate_data_compra"] == 0]

        if produto_escolhido != 'Selecione...':
            produto_escolhido = produto_escolhido.split(' - ', maxsplit=1)[0]  # Pega apenas o código do produto

            df_resultado = df_filtrado[df_filtrado['Código'] == produto_escolhido]
            df_plot = df_resultado.copy()
            df_resultado.set_index(['Código', 'Descrição'], inplace=True)
            st.dataframe(df_resultado)
            plotar_grafico_serrote(produto_escolhido, None, df_plot, df_pedidos)

        elif grupo_escolhido != 'Selecione...':
            df_resultado = df_filtrado[df_filtrado['grupo'] == grupo_escolhido]
            df_plot = df_resultado.copy()
            df_resultado.set_index(['Código', 'Descrição'], inplace=True)
            st.dataframe(df_resultado)
            plotar_grafico_serrote(None, grupo_escolhido, df_plot, df_pedidos)

        else:
            df_filtrado.set_index(['Código', 'Descrição'], inplace=True)
            st.dataframe(df_filtrado)
            st.warning(f"📈 Para ver os **gráficos** escolha um produto ou um grupo.")

    with aba2:
        st.subheader("Regras utilizadas")
        st.markdown("""
        - Apenas itens que estão na pendência de vendas.
        - Todos os cálculos são feitos levando em consideração apenas dados de pedidos de compra antes de atingir o estoque mínimo.
        - **Estoque mínimo**: Valor MÁXIMO entre: ( [Prev Con Mov Est(CMM)] ) / 20 ) * Dias segurança
        - **consumo_diario**: Valor MÁXIMO entre: ( [Prev Con Mov Est(CMM)] ) / 20 )
        - **dias_ate_estoque_zero**: Estoque / consumo_diario
        - **dias_ate_estoque_minimo**: [Estoque - Estoque mínimo] / consumo diário
        - **dias_ate_compra**: data_estoque_minimo - dias de ressuprimento
        """)
