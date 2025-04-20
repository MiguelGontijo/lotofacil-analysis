# src/visualization/plotter.py
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
from pathlib import Path
import numpy as np
from typing import Optional, Union, List, Dict, Any

# Configurações (podem vir do config.py eventualmente)
PLOTS_DIR = Path("plots") # Define o diretório base para salvar os plots
DEFAULT_FIGSIZE = (14, 7) # Ajustado para gráficos de linha
DEFAULT_DPI = 100
ROLLING_WINDOW_DEFAULT = 10 # Janela para média móvel (pode ser ajustada)

# Garante que o diretório de plots exista
PLOTS_DIR.mkdir(parents=True, exist_ok=True)

# --- Funções de Plotagem Existentes ---
# (plot_frequency_distribution, plot_delay_distribution, ...)
# ... Mantenha todas as suas funções existentes aqui ...

def plot_frequency_distribution(freq_series: pd.Series, title: str = "Distribuição de Frequência", filename: str = "frequency_distribution.png"):
    """ Gera um gráfico de barras da frequência de cada dezena. """
    if freq_series is None or freq_series.empty: return
    plt.figure(figsize=DEFAULT_FIGSIZE)
    sns.barplot(x=freq_series.index.astype(str), y=freq_series.values, palette="viridis") # Garante índice como string
    plt.title(title)
    plt.xlabel("Dezena")
    plt.ylabel("Frequência / Valor") # Generalizado
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(PLOTS_DIR / filename, dpi=DEFAULT_DPI)
    plt.close() # Fecha a figura para liberar memória

def plot_delay_distribution(delay_series: pd.Series, title: str = "Distribuição de Atraso Atual", filename: str = "delay_distribution.png"):
    """ Gera um gráfico de barras do atraso atual ou outra métrica por dezena. """
    if delay_series is None or delay_series.empty: return
    plt.figure(figsize=DEFAULT_FIGSIZE)
    # Ordena para melhor visualização (maior valor primeiro)
    delay_series_sorted = delay_series.sort_values(ascending=False)
    sns.barplot(x=delay_series_sorted.index.astype(str), y=delay_series_sorted.values, palette="magma") # Converte index para str se necessário
    plt.title(title)
    plt.xlabel("Dezena")
    plt.ylabel("Valor da Métrica") # Generalizado
    plt.xticks(rotation=90) # Mais espaço para números
    plt.tight_layout()
    plt.savefig(PLOTS_DIR / filename, dpi=DEFAULT_DPI)
    plt.close()

def plot_cycle_duration_histogram(cycles_df: pd.DataFrame, title: str = "Histograma da Duração dos Ciclos", filename: str = "cycle_duration_histogram.png"):
    """ Gera um histograma da coluna 'duracao' do DataFrame de ciclos. """
    if cycles_df is None or cycles_df.empty or 'duracao' not in cycles_df.columns: return
    plt.figure(figsize=DEFAULT_FIGSIZE)
    sns.histplot(cycles_df['duracao'], kde=True, bins=max(1, cycles_df['duracao'].nunique())) # Ajusta bins
    plt.title(title)
    plt.xlabel("Duração do Ciclo (Nº Concursos)")
    plt.ylabel("Quantidade de Ciclos")
    plt.tight_layout()
    plt.savefig(PLOTS_DIR / filename, dpi=DEFAULT_DPI)
    plt.close()

def plot_repetition_distribution(repetition_series: pd.Series, title: str = "Distribuição de Dezenas Repetidas", filename: str = "repetition_distribution.png"):
    """ Gera um gráfico de barras mostrando quantas vezes cada quantidade de repetições ocorreu. """
    if repetition_series is None or repetition_series.empty: return
    # Calcula a contagem de ocorrências para cada quantidade de repetições
    repetition_counts = repetition_series.value_counts().sort_index()
    plt.figure(figsize=DEFAULT_FIGSIZE)
    sns.barplot(x=repetition_counts.index, y=repetition_counts.values, palette="coolwarm")
    plt.title(title)
    plt.xlabel("Quantidade de Dezenas Repetidas do Sorteio Anterior")
    plt.ylabel("Número de Ocorrências")
    plt.tight_layout()
    plt.savefig(PLOTS_DIR / filename, dpi=DEFAULT_DPI)
    plt.close()

def plot_pair_frequency_heatmap(pair_freq_df: pd.DataFrame, top_n: int = 30, title: str = "Heatmap de Frequência dos Pares Mais Comuns", filename: str = "pair_frequency_heatmap.png"):
    """ Gera um heatmap mostrando a frequência dos N pares mais comuns. """
    if pair_freq_df is None or pair_freq_df.empty or not all(c in pair_freq_df.columns for c in ['dezena1', 'dezena2', 'frequencia']): return
    # Pega os top N pares
    top_pairs = pair_freq_df.nlargest(top_n, 'frequencia')
    if top_pairs.empty: return
    # Cria uma matriz pivotada para o heatmap
    try:
        pivot_table = top_pairs.pivot(index='dezena1', columns='dezena2', values='frequencia')
        # Preenche NaNs onde o par não existe ou não está no top N
        # Para visualização, pode ser útil preencher com 0, mas semanticamente NaN é mais correto.
        # Vamos preencher com 0 para o plot não ficar vazio.
        pivot_table.fillna(0, inplace=True)

        # Tenta reindexar para ter uma matriz mais completa (opcional, pode ficar esparso)
        all_involved_numbers = sorted(list(set(top_pairs['dezena1']) | set(top_pairs['dezena2'])))
        pivot_table = pivot_table.reindex(index=all_involved_numbers, columns=all_involved_numbers, fill_value=0)

        plt.figure(figsize=(15, 12)) # Maior para heatmap
        sns.heatmap(pivot_table, annot=False, cmap="hot", fmt=".0f") # Annot=False se for muito denso
        plt.title(title + f" (Top {top_n})")
        plt.xlabel("Dezena 2")
        plt.ylabel("Dezena 1")
        plt.tight_layout()
        plt.savefig(PLOTS_DIR / filename, dpi=DEFAULT_DPI)
        plt.close()
    except Exception as e:
        # Usar logger se disponível, senão print
        try:
            from src.config import logger
            logger.error(f"Erro ao gerar heatmap de pares: {e}")
        except ImportError:
            print(f"Erro ao gerar heatmap de pares: {e}")
        plt.close() # Garante fechar a figura em caso de erro

def plot_property_distribution_pie(property_counts: pd.Series, title: str = "Distribuição de Propriedade", filename: str = "property_distribution_pie.png"):
    """ Gera um gráfico de pizza para a contagem de propriedades (ex: Pares vs Ímpares). """
    if property_counts is None or property_counts.empty: return
    plt.figure(figsize=(8, 8)) # Pizza fica melhor quadrada
    plt.pie(property_counts, labels=property_counts.index, autopct='%1.1f%%', startangle=90, colors=sns.color_palette("pastel"))
    plt.title(title)
    plt.tight_layout()
    plt.savefig(PLOTS_DIR / filename, dpi=DEFAULT_DPI)
    plt.close()

def plot_group_trend_lines(trend_df: pd.DataFrame, title: str = "Tendência de Grupos ao Longo do Tempo", filename: str = "group_trend_lines.png"):
    """
    Gera gráficos de linha mostrando a tendência de métricas de grupos ao longo dos concursos.
    Espera um DataFrame com 'concurso' como coluna ou índice e outras colunas representando as métricas dos grupos.
    """
    if trend_df is None or trend_df.empty: return

    # Define 'concurso' como índice se ainda não for
    if 'concurso' in trend_df.columns:
        plot_df = trend_df.set_index('concurso')
    else:
        plot_df = trend_df # Assume que já é o índice

    if plot_df.empty: return

    plt.figure(figsize=(15, 8)) # Linhas ficam melhores mais largas
    for col in plot_df.columns:
        # Adiciona uma média móvel para suavizar (opcional)
        try:
            # Tenta calcular média móvel, ignora se der erro (ex: coluna não numérica)
            rolling_mean = plot_df[col].rolling(window=ROLLING_WINDOW_DEFAULT, min_periods=1).mean()
            plt.plot(plot_df.index, rolling_mean, label=f"{col} (Média Móvel {ROLLING_WINDOW_DEFAULT})")
        except (TypeError, ValueError):
             # Se não for numérico, plota o original (ou ignora)
             # plt.plot(plot_df.index, plot_df[col], label=col) # Plotar original se falhar a média
             pass # Ignora colunas não numéricas por enquanto


    plt.title(title)
    plt.xlabel("Número do Concurso / Índice") # Generaliza o eixo X
    plt.ylabel("Valor da Métrica")
    # Evita erro se não houver linhas para plotar
    handles, labels = plt.gca().get_legend_handles_labels()
    if handles:
        plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(PLOTS_DIR / filename, dpi=DEFAULT_DPI)
    plt.close()


# --- NOVA FUNÇÃO PARA VISUALIZAÇÃO DE CHUNKS ---
def plot_chunk_metric_evolution(
    chunk_metrics_df: pd.DataFrame,
    metrics_to_plot: List[str],
    chunk_type_name: str, # Ex: "Linear 10", "Fibonacci 8"
    title_suffix: str = "Evolução por Bloco",
    filename_prefix: str = "chunk_evolution"
):
    """
    Gera gráficos de linha mostrando a evolução de métricas ao longo dos blocos (chunks).

    Args:
        chunk_metrics_df (pd.DataFrame): DataFrame onde o índice representa o ID ou
                                         o início/fim do chunk, e as colunas são as
                                         métricas calculadas para cada chunk.
                                         Ex: freq_media_dezena_1, contagem_pares, etc.
        metrics_to_plot (List[str]): Lista de nomes das colunas (métricas) a serem plotadas.
        chunk_type_name (str): Nome descritivo do tipo de chunk (usado no título/filename).
        title_suffix (str): Sufixo para adicionar ao título do gráfico.
        filename_prefix (str): Prefixo para o nome do arquivo de saída.
    """
    if chunk_metrics_df is None or chunk_metrics_df.empty:
        # Usar logger se disponível
        try:
            from src.config import logger
            logger.warning(f"DataFrame de métricas de chunk para '{chunk_type_name}' vazio ou inválido.")
        except ImportError:
            print(f"Warning: DataFrame de métricas de chunk para '{chunk_type_name}' vazio ou inválido.")
        return

    # Verifica se as métricas existem no DataFrame
    valid_metrics = [m for m in metrics_to_plot if m in chunk_metrics_df.columns]
    if not valid_metrics:
        try:
            from src.config import logger
            logger.warning(f"Nenhuma das métricas especificadas {metrics_to_plot} encontrada no DataFrame para chunks '{chunk_type_name}'.")
        except ImportError:
            print(f"Warning: Nenhuma das métricas especificadas {metrics_to_plot} encontrada no DataFrame para chunks '{chunk_type_name}'.")
        return

    plot_df = chunk_metrics_df[valid_metrics]

    plt.figure(figsize=DEFAULT_FIGSIZE)

    for metric in valid_metrics:
        try:
            # Tenta plotar a métrica e sua média móvel
            plt.plot(plot_df.index, plot_df[metric], label=f"{metric}", alpha=0.6)
            rolling_mean = plot_df[metric].rolling(window=ROLLING_WINDOW_DEFAULT, min_periods=1).mean()
            plt.plot(plot_df.index, rolling_mean, label=f"{metric} (Média Móvel {ROLLING_WINDOW_DEFAULT})", linestyle='--')
        except (TypeError, ValueError):
            # Ignora colunas não numéricas ou com outros problemas para plotagem de linha
            try:
                from src.config import logger
                logger.warning(f"Não foi possível plotar a métrica '{metric}' para chunks '{chunk_type_name}' (provavelmente não numérica).")
            except ImportError:
                print(f"Warning: Não foi possível plotar a métrica '{metric}' para chunks '{chunk_type_name}'.")
            continue # Pula para a próxima métrica

    plt.title(f"{chunk_type_name} - {title_suffix}")
    plt.xlabel("Identificador do Bloco (Chunk ID / Início)") # O índice do DF
    plt.ylabel("Valor da Métrica")

    # Evita erro se não houver linhas para plotar
    handles, labels = plt.gca().get_legend_handles_labels()
    if handles:
         # Controla o número de itens na legenda se ficar muito grande
        max_legend_items = 15
        if len(handles) > max_legend_items:
             plt.legend(handles[:max_legend_items], labels[:max_legend_items], fontsize='small')
        else:
             plt.legend(fontsize='small')

    plt.grid(True)
    plt.tight_layout()

    # Cria um nome de arquivo seguro
    safe_chunk_name = "".join(c if c.isalnum() else "_" for c in chunk_type_name)
    filename = f"{filename_prefix}_{safe_chunk_name}.png"
    plt.savefig(PLOTS_DIR / filename, dpi=DEFAULT_DPI)
    plt.close()

    try:
        from src.config import logger
        logger.info(f"Gráfico de evolução de chunks '{chunk_type_name}' salvo em: {PLOTS_DIR / filename}")
    except ImportError:
        print(f"Gráfico de evolução de chunks '{chunk_type_name}' salvo em: {PLOTS_DIR / filename}")


# Adicionar mais funções de plotagem conforme necessário...
