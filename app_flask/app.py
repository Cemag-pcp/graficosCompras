from flask import Flask, render_template, request
import pandas as pd

app = Flask(__name__)

from flask import Flask, render_template, request, jsonify
import pandas as pd

from utils import dados_grafico_serrote,tratamento_geral_direto

app = Flask(__name__)

@app.route('/api/dados_direto/', methods=['GET'])
def dados_direto():
    df_simulacao, df_pedidos = tratamento_geral_direto()

    datas, datas_grafico, datas_ideal_grafico, estoque_ideal_diario, estoque_diario = dados_grafico_serrote(
        '213602', None, df_simulacao, df_pedidos
    )

    resultado = {
        'datas': [str(d) for d in datas],  # datas como string para garantir serialização
        'datas_grafico': [str(d) for d in datas_grafico],
        'datas_ideal_grafico': [str(d) for d in datas_ideal_grafico],
        'estoque_diario': estoque_diario,
        'estoque_ideal_diario': estoque_ideal_diario
    }

    return jsonify(resultado)

@app.route('/api/produtos/', methods=['GET'])
def produtos():
    df_simulacao, df_pedidos = tratamento_geral_direto()

@app.route('/api/grupos/', methods=['GET'])
def grupos():
    df_simulacao, df_pedidos = tratamento_geral_direto()


if __name__ == '__main__':
    app.run(debug=True)
