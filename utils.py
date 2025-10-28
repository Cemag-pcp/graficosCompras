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


def adicionar_dias_uteis(data_inicial, num_dias):
    """
    Adiciona ou subtrai um número específico de dias úteis, pulando fins de semana.
    Trata valores nulos, inválidos e evita OverflowError.

    Args:
        data_inicial: datetime.date ou datetime
        num_dias: int (positivo para frente, negativo para trás)
    Returns:
        datetime.date
    """

    # 1. Tratar casos nulos e inválidos
    if pd.isna(num_dias) or num_dias == 0:
        return data_inicial

    try:
        num_dias = int(num_dias)
    except (ValueError, TypeError):
        return data_inicial

    # 2. Limite de segurança — evita estouro de data (±20.000 dias úteis ≈ 77 anos)
    if abs(num_dias) > 20000:
        return data_inicial

    # 3. Garantir tipo date
    if isinstance(data_inicial, datetime):
        data_inicial = data_inicial.date()

    # 4. Calcular rapidamente, sem loop, com base no número de semanas completas
    incremento = 1 if num_dias > 0 else -1
    dias_abs = abs(num_dias)

    # semanas completas e resto de dias úteis
    semanas, resto = divmod(dias_abs, 5)
    dias_corridos = semanas * 7

    # mover data base para frente ou para trás
    data_final = data_inicial + timedelta(days=dias_corridos * incremento)

    # agora aplicar os dias úteis restantes manualmente
    while resto > 0:
        data_final += timedelta(days=incremento)
        if data_final.weekday() < 5:  # 0=Seg ... 4=Sex
            resto -= 1

    # 5. Proteger contra overflow final
    try:
        return data_final
    except OverflowError:
        return None
    
def processar_qde_ped(valor):
    if pd.isna(valor):
        return None  # ou 0, dependendo da sua regra

    if isinstance(valor, str):
        # Separa por espaços
        partes = valor.split()
        # Converte cada parte para float
        numeros = []
        for p in partes:
            p_limpo = p.replace(".", "").replace(",", ".")
            try:
                numeros.append(float(p_limpo))
            except ValueError:
                continue  # pula valores que não são numéricos
        if numeros:
            return numeros[0]  # ou sum(numeros), se quiser a soma
        else:
            return None
    else:
        return valor  # se já for número

# se antes da data de para chegar no estoque mínimo tiver pedido de compra adicionar no estoque almox central
def ajustar_estoque(row, df_pedidos):
    data_limite = row['data_estoque_minimo']

    # Se a data é datetime.date, converte para datetime64[ns] só se estiver dentro do intervalo
    if isinstance(data_limite, datetime):
        data_limite = data_limite.date()
    
    if isinstance(data_limite, pd.Timestamp):
        data_limite = data_limite.to_pydatetime().date()
    
    # pandas só suporta datas até 2262-04-11
    if data_limite and data_limite < datetime(2262, 4, 11).date():
        data_limite = pd.to_datetime(data_limite)
    else:
        return row['Est.Almox Central']  # ou ajuste conforme sua lógica

    try:
        primeira_qde_ped = df_pedidos.loc[:, ~df_pedidos.columns.duplicated()]['Qde Ped']
    except KeyError:
        primeira_qde_ped = df_pedidos.loc[:, ~df_pedidos.columns.duplicated()]['Qdade Pedido']

    primeira_qde_ped = primeira_qde_ped.apply(
        lambda x: '0' if pd.isna(x) or str(x).strip() == '' else str(x).strip()
    )

    # Converter para float após limpar os valores
    primeira_qde_ped = primeira_qde_ped.apply(
        lambda x: float(str(x).replace('.', '').replace(',', '.'))
    )

    df_pedidos['Qde Ped Corrigido'] = primeira_qde_ped
    
    df_pedidos['Data Entrega'] = pd.to_datetime(df_pedidos['Data Entrega'], format='%d/%m/%Y', errors='coerce')

    # data_limite = pd.to_datetime(row['data_estoque_minimo'])  # converte para datetime64[ns]

    pedidos_filtrados = df_pedidos[
        (df_pedidos['Código'] == row['Código']) &
        (df_pedidos['Data Entrega'] <= data_limite)
    ]
    if not pedidos_filtrados.empty:
        return row['Est.Almox Central'] + pedidos_filtrados['Qde Ped Corrigido'].sum()
    else:
        return row['Est.Almox Central']

# criar coluna informando uma flag de "urgência" para itens com dias_ate_data_compra negativo "Urgência - Solicitar Compra" e para positivos "Dentro do prazo - x dias"
def flag_urgencia(dias):
    if pd.isna(dias):
        return None
    elif dias <= 0:
        return "🟥 - URGENTE!!"
    elif dias <= 5:
        return f"🟨 - Prazo curto - {dias} dias"
    else:
        return f"🟩 Prazo ok - {dias} dias"

# mudar formato de coluna de data para brasileiro
def formatar_data_brasileiro(data):
    if pd.isna(data):
        return None
    return data.strftime('%d/%m/%Y')


