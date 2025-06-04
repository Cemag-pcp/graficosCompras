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

from utils import adicionar_dias_uteis, formatar_data_brasileiro 

def sugestoes(produto_escolhido, df_pedidos, estoque_atual, estoque_minimo, linha_produto):
    """
    Gera sugestões inteligentes para compras baseadas no status do estoque
    e pedidos pendentes.
    """
    
    # Validação básica dos dados de entrada
    if linha_produto['Dias\nRessupr'] == 0:
        st.error("**🚫 Erro**: Não é possível gerar sugestões - produto sem dias de ressuprimento configurados.")
        st.info("💡 **Ação recomendada**: Configure os dias de ressuprimento para este produto no cadastro.")
        return

    # Conversão e preparação das datas
    try:
        data_estoque_zero = datetime.strptime(linha_produto['data_estoque_zero'], "%d/%m/%Y").date()
        data_estoque_minimo_dt = pd.to_datetime(
            datetime.strptime(linha_produto['data_estoque_minimo'], "%d/%m/%Y").date()
        )
    except (ValueError, KeyError) as e:
        st.error("**🚫 Erro**: Problema com as datas do produto. Verifique os dados.")
        return

    # Filtra pedidos pendentes após a data de estoque mínimo
    df_pedidos_filtrados = df_pedidos[
        (df_pedidos['Código'] == produto_escolhido) & 
        (df_pedidos['Data Entrega'] >= data_estoque_minimo_dt)
    ]
    
    # CENÁRIO 1: Já existe pedido pendente
    if not df_pedidos_filtrados.empty:
        _analisar_pedido_pendente(df_pedidos_filtrados, data_estoque_zero, linha_produto)
        return
    
    # CENÁRIO 2: Estoque crítico (abaixo do mínimo)
    if estoque_atual <= estoque_minimo:
        _sugerir_compra_critica(estoque_atual, estoque_minimo, linha_produto)
        return

    # CENÁRIO 3: Prazo de compra vencido/crítico
    if linha_produto['dias_ate_data_compra'] <= 0:
        _sugerir_compra_urgente(linha_produto, data_estoque_zero, estoque_atual, estoque_minimo)

    # CENÁRIO 4: Situação normal - sugestões preventivas
    elif linha_produto['dias_ate_data_compra'] <= 3:
        _sugerir_compra_preventiva(linha_produto, estoque_atual, estoque_minimo)
    
    # CENÁRIO 5: Tudo OK
    else:
        _mostrar_status_ok(linha_produto)

def _analisar_pedido_pendente(df_pedidos, data_estoque_zero, linha_produto):
    """Analisa pedidos pendentes e sugere ações."""
    pedido_mais_proximo = df_pedidos.iloc[0]
    data_entrega = pedido_mais_proximo['Data Entrega'].date()
    data_estoque_minimo = datetime.strptime(linha_produto['data_estoque_minimo'], "%d/%m/%Y").date()

    st.info("📦 **Status**: Existe pedido de compra pendente para este produto.")
    st.write(f"📅 **Data prevista de entrega**: {formatar_data_brasileiro(data_entrega)}")
    
    if data_entrega > data_estoque_minimo:
        st.warning(f"**ATENÇÃO**: O pedido chegará após atingir o estoque mínimo.")

    if data_entrega > data_estoque_zero:
        dias_sem_material = abs(np.busday_count(data_entrega, data_estoque_zero))
        data_limite_entrega = formatar_data_brasileiro(adicionar_dias_uteis(data_estoque_zero, -1))
        
        st.error(f"🚨 **CRÍTICO**: Pedido chegará {dias_sem_material} dias após o estoque zerar!")
        st.warning(f"⚡ **Ação urgente**: Antecipar entrega para no máximo **{data_limite_entrega}**")
    else:
        st.success("✅ **OK**: Pedido chegará antes do estoque zerar.")
        dias_margem = np.busday_count(data_entrega,data_estoque_zero)
        st.write(f"📊 **Margem de segurança**: {dias_margem} dias, então quer dizer que o pedido chegará {dias_margem} dias antes de atingir a data de estoque zero: {formatar_data_brasileiro(data_estoque_zero)}")

def _sugerir_compra_critica(estoque_atual, estoque_minimo, linha_produto):
    """
    Sugere a quantidade de compra para situação crítica de estoque.
    Informa o déficit e o reabastecimento mínimo necessário.
    """
    dias_ressupr = linha_produto['Dias\nRessupr']
    consumo_diario = linha_produto['consumo_diario']
    data_chegada_material = adicionar_dias_uteis(datetime.now().date(), dias_ressupr)
    dias_atraso = abs(linha_produto['dias_ate_data_compra'])

    # Calcula dias úteis até a chegada (não inclui data_chegada_material)
    dias_uteis_ate_chegada = np.busday_count(datetime.now().date(), (data_chegada_material + timedelta(days=1)))
    
    # Estoque que será consumido até a chegada
    consumo_ate_chegada = dias_uteis_ate_chegada * consumo_diario
    
    # Estoque projetado no dia da chegada
    estoque_no_dia_chegada = estoque_atual - consumo_ate_chegada
    
    # Gap projetado e necessidade de reabastecimento
    gap_estoque_projetado = estoque_minimo - estoque_no_dia_chegada

    # gap_estoque = estoque_minimo - (estoque_atual - consumo_diario) # diminui com consumo diário para buscar o estoque de hoje consumido
    qtd_ressuprimento = dias_ressupr * consumo_diario
    qtd_total_sugerida = gap_estoque_projetado + qtd_ressuprimento
    
    st.error(f"🚨 **URGENTE**: Prazo para compra venceu há {dias_atraso} dias!")
    st.error("🚨 **ESTOQUE CRÍTICO**: O estoque está abaixo do nível mínimo necessário!")
    st.warning("⚡ **Ação imediata**: Solicitar compra **hoje**!")
    st.info(f"📅 **Chegada prevista do material**: {formatar_data_brasileiro(data_chegada_material)}")

    st.warning(
        f"📦 **Ação Imediata**: Recomenda-se providenciar a compra de **{qtd_total_sugerida:.2f} unidades**.\n\n"
        f"Essa quantidade considera:\n"
        f"- O déficit de estoque no dia da chegada (**{gap_estoque_projetado:.2f} unidades**).\n"
        f"- A necessidade de reabastecimento para manter a cobertura mínima por **{dias_ressupr} dias** (**{qtd_ressuprimento:.2f} unidades**).\n\n"
    )
    
def _sugerir_compra_urgente(linha_produto, data_estoque_zero, estoque_atual, estoque_minimo):
    """
    Sugere compra urgente quando prazo já venceu e mostra o estoque projetado no dia de chegada.
    """
    dias_ressupr = linha_produto['Dias\nRessupr']
    dias_atraso = abs(linha_produto['dias_ate_data_compra'])
    data_chegada_material = adicionar_dias_uteis(datetime.now().date(), dias_ressupr)
    consumo_diario = linha_produto['consumo_diario']
    
    # Calcula dias úteis até a chegada (não inclui data_chegada_material)
    dias_uteis_ate_chegada = np.busday_count(datetime.now().date(), (data_chegada_material + timedelta(days=1)))
    
    # Estoque que será consumido até a chegada
    consumo_ate_chegada = dias_uteis_ate_chegada * consumo_diario
    
    # Estoque projetado no dia da chegada
    estoque_no_dia_chegada = estoque_atual - consumo_ate_chegada
    
    # Gap projetado e necessidade de reabastecimento
    gap_estoque_projetado = estoque_minimo - estoque_no_dia_chegada
    qtd_ressuprimento = gap_estoque_projetado + (dias_ressupr * consumo_diario)

    st.error(f"🚨 **URGENTE**: Prazo para compra venceu há {dias_atraso} dias!")
    st.warning("⚡ **Ação imediata**: Solicitar compra **hoje**!")
    st.info(f"📅 **Chegada prevista do material**: {formatar_data_brasileiro(data_chegada_material)}")
    
    st.info(
        f"📦 **Estoque projetado no dia da chegada**: **{estoque_no_dia_chegada:.2f} unidades**\n\n"
        f"👉 Esse valor considera o consumo diário de **{consumo_diario:.2f} unidades** ao longo de "
        f"**{dias_uteis_ate_chegada} dias úteis** até o material chegar.\n\n"
    )
    
    st.warning(
        f"📈 **Compra sugerida para evitar rupturas**: **{qtd_ressuprimento:.2f} unidades**\n\n"
        f"Essa quantidade cobre:\n"
        f"- O gap projetado no dia da chegada (**{gap_estoque_projetado:.2f} unidades**) (estoque mínimo - quantidade no dia da chegada)\n"
        f"- A necessidade de reabastecimento para garantir o estoque mínimo pelos "
        f"**{dias_ressupr} dias** de ressuprimento (**{dias_ressupr * consumo_diario:.2f} unidades**)."
    )

def _sugerir_compra_preventiva(linha_produto, estoque_atual, estoque_minimo):
    """
    Sugere compra preventiva quando está próximo do prazo de compra,
    incentivando o planejamento e evitando urgências.
    """
    dias_restantes = linha_produto['dias_ate_data_compra']
    dias_ressupr = linha_produto['Dias\nRessupr']
    consumo_diario = linha_produto['consumo_diario']
    
    st.warning(f"⏰ **Alerta Preventivo**: Restam apenas **{dias_restantes} dias** para solicitar a compra.")
    st.info("📋 **Recomendação**: Iniciar planejamento do pedido de compra nos próximos dias.")
    
    if estoque_atual < estoque_minimo:
        gap_estoque = estoque_minimo - estoque_atual
        qtd_sugerida = (dias_ressupr * consumo_diario) + gap_estoque
        
        st.warning(
            f"📦 **Estoque abaixo do mínimo!**\n\n"
            f"• Gap atual: **{gap_estoque:.2f} unidades**\n"
            f"• Ressuprimento para {dias_ressupr} dias: **{dias_ressupr * consumo_diario:.2f} unidades**\n"
            f"👉 **Total recomendado para compra**: **{qtd_sugerida:.2f} unidades**"
        )
    else:
        qtd_sugerida = dias_ressupr * consumo_diario
        st.info(
            f"✅ **Estoque dentro do mínimo**, mas recomenda-se compra de "
            f"**{qtd_sugerida:.2f} unidades** para garantir a cobertura de {dias_ressupr} dias."
        )

def _mostrar_status_ok(linha_produto):
    """Mostra status quando tudo está sob controle."""
    dias_restantes = linha_produto['dias_ate_data_compra']
    
    st.success("✅ **Status**: Situação controlada")
    st.info(f"📅 **Próxima compra em**: {dias_restantes} dias")
    
    # Informações adicionais úteis
    consumo_diario = linha_produto['consumo_diario']
    dias_ressupr = linha_produto['Dias\nRessupr']
    qtd_padrao = dias_ressupr * consumo_diario
    
    with st.expander("📊 Informações do produto"):
        st.write(f"• Consumo diário: {consumo_diario:.2f} unidades")
        st.write(f"• Dias de ressuprimento: {dias_ressupr}")
        st.write(f"• Quantidade padrão de compra: {qtd_padrao:.2f} unidades")
