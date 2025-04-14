# src/visualization/plotter.py

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
import logging
from src.config import logger

# Define o diretório de saída para os plots
# Assume que este arquivo está em src/visualization/
PLOT_OUTPUT_DIR = Path(__file__).parent.parent.parent / 'plots'

def setup_plotting():
    """ Configura estilo básico para os plots e cria diretório de saída """
    sns.set_theme(style="whitegrid") # Define um tema visual agradável
    # Cria o diretório de plots se não existir
    PLOT_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    logger.info(f"Diretório de plots configurado em: {PLOT_OUTPUT_DIR}")

def plot_frequency_bar(freq_series: pd.Series, title: str, filename: str):
    """ Cria e salva um gráfico de barras para frequência de dezenas """
    if freq_series is None or freq_series.empty:
        logger.warning(f"Série de frequência para '{title}' está vazia. Plot não gerado.")
        return

    full_path = PLOT_OUTPUT_DIR / f"{filename}.png"
    logger.info(f"Gerando plot: {title} -> {full_path}")

    try:
        plt.figure(figsize=(12, 6)) # Define o tamanho da figura
        bars = sns.barplot(x=freq_series.index, y=freq_series.values, palette="viridis")
        plt.title(title, fontsize=16)
        plt.xlabel("Dezena", fontsize=12)
        plt.ylabel("Frequência", fontsize=12)
        plt.xticks(rotation=45) # Rotaciona os labels do eixo X se ficarem apertados
        plt.tight_layout() # Ajusta o layout para evitar sobreposição

        # Adiciona os valores no topo das barras (opcional)
        for bar in bars.patches:
             bars.annotate(format(bar.get_height(), '.0f'),
                           (bar.get_x() + bar.get_width() / 2., bar.get_height()),
                           ha = 'center', va = 'center',
                           size=9, xytext = (0, 5), # Deslocamento vertical
                           textcoords = 'offset points')

        plt.savefig(full_path)
        plt.close() # Fecha a figura para liberar memória
        logger.debug(f"Plot '{filename}.png' salvo com sucesso.")
    except Exception as e:
        logger.error(f"Erro ao gerar ou salvar o plot '{filename}.png': {e}")

def plot_distribution_bar(dist_series: pd.Series, title: str, filename: str):
    """ Cria e salva um gráfico de barras para distribuições de propriedades """
    if dist_series is None or dist_series.empty:
        logger.warning(f"Série de distribuição para '{title}' está vazia. Plot não gerado.")
        return

    full_path = PLOT_OUTPUT_DIR / f"{filename}.png"
    logger.info(f"Gerando plot: {title} -> {full_path}")

    try:
        plt.figure(figsize=(10, 6))
        # Usar o índice da Series (as categorias da distribuição) no eixo X
        bars = sns.barplot(x=dist_series.index, y=dist_series.values, palette="magma")
        plt.title(title, fontsize=16)
        plt.xlabel("Distribuição", fontsize=12)
        plt.ylabel("Número de Ocorrências", fontsize=12)
        plt.xticks(rotation=45, ha='right') # Rotaciona e alinha à direita
        plt.tight_layout()

        # Adiciona valores no topo das barras
        for bar in bars.patches:
             bars.annotate(format(bar.get_height(), '.0f'),
                           (bar.get_x() + bar.get_width() / 2., bar.get_height()),
                           ha = 'center', va = 'center',
                           size=9, xytext = (0, 5),
                           textcoords = 'offset points')

        plt.savefig(full_path)
        plt.close()
        logger.debug(f"Plot '{filename}.png' salvo com sucesso.")
    except Exception as e:
        logger.error(f"Erro ao gerar ou salvar o plot '{filename}.png': {e}")

# Inicializa a configuração ao importar o módulo
setup_plotting()