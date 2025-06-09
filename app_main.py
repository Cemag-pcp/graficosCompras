import streamlit as st
from main_suporte import main as main_suporte
from app_mat_indireto import main as main_indireto

# Suponha que você importe os dados de algum módulo ou arquivo
from app_mat_indireto import tratamento_geral as tratamento_geral_indireto
from app_mat_indireto import plotar_grafico_serrote as plotar_grafico_serrote_indireto

from main_suporte import tratamento_geral as tratamento_geral_direto
from main_suporte import plotar_grafico_serrote as plotar_grafico_serrote_direto

if "page" not in st.session_state:
    st.session_state.page = "main_suporte"

st.sidebar.title("Menu")
if st.sidebar.button("🔧 Material Direto"):
    st.session_state.page = "main_suporte"
if st.sidebar.button("📊 Material Indireto"):
    st.session_state.page = "mat_indireto"

if st.session_state.page == "main_suporte":
    df_simulacao, df_pedidos = tratamento_geral_direto()
    main_suporte(df_simulacao, df_pedidos, plotar_grafico_serrote_direto)
elif st.session_state.page == "mat_indireto":
    df_simulacao, df_pedidos = tratamento_geral_indireto()
    main_indireto(df_simulacao, df_pedidos, plotar_grafico_serrote_indireto)
