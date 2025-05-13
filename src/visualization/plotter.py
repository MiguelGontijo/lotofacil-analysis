# src/visualization/plotter.py
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import seaborn as sns
import os
import logging
from typing import List, Optional, Dict, Any 
from pathlib import Path # <<< --- IMPORTAÇÃO DE PATH ADICIONADA/CONFIRMADA

from src.database_manager import DatabaseManager
from src.config import PLOT_DIR_CONFIG, ALL_NUMBERS

logger = logging.getLogger(__name__)

def ensure_output_dir(output_dir: str):
    """Garante que o diretório de saída exista."""
    Path(output_dir).mkdir(parents=True, exist_ok=True) # Agora 'Path' está definido


def plot_frequency(
    df_frequency: pd.DataFrame,
    metric_type: str = 'Absoluta',
    output_dir: str = str(PLOT_DIR_CONFIG)
):
    """
    Plota a frequência (absoluta ou relativa) das dezenas.
    """
    if df_frequency is None or df_frequency.empty:
        logger.warning(f"DataFrame de frequência {metric_type.lower()} está vazio. Não é possível gerar o gráfico.")
        return

    if metric_type == 'Absoluta':
        freq_col_to_plot = next((col for col in ['Frequencia Absoluta', 'frequencia_absoluta'] if col in df_frequency.columns), None)
    elif metric_type == 'Relativa':
        freq_col_to_plot = next((col for col in ['Frequencia Relativa', 'frequencia_relativa'] if col in df_frequency.columns), None)
    else:
        logger.error(f"Tipo de métrica de frequência desconhecido: {metric_type}")
        return
        
    if not freq_col_to_plot:
        logger.error(f"Coluna de frequência para '{metric_type}' não encontrada. Colunas: {df_frequency.columns.tolist()}")
        return
        
    dezena_col = next((col for col in ['Dezena', 'dezena'] if col in df_frequency.columns), None)
    if not dezena_col:
        logger.error(f"Coluna 'Dezena' ou 'dezena' não encontrada. Colunas: {df_frequency.columns.tolist()}")
        return

    ensure_output_dir(output_dir)
    
    plt.figure(figsize=(14, 7)) # Aumentado um pouco para melhor visualização dos ticks
    plot_data = df_frequency.sort_values(by=freq_col_to_plot, ascending=False)
    
    # Garante que a coluna de dezenas seja tratada como categórica para ordenação correta no barplot se for numérica
    plot_data[dezena_col] = plot_data[dezena_col].astype(str) 
    
    sns.barplot(x=dezena_col, y=freq_col_to_plot, data=plot_data, palette="viridis", order=plot_data[dezena_col])
    plt.title(f'Frequência {metric_type} das Dezenas da Lotofácil')
    plt.xlabel('Dezena')
    plt.ylabel(f'Frequência {metric_type}')
    # Ajusta os ticks do eixo X para mostrar todos os números se ALL_NUMBERS estiver disponível e for o caso
    # Isso é mais útil se as dezenas forem de 1 a 25 e quisermos garantir que todas apareçam.
    # No entanto, a ordenação por frequência é mais comum.
    # Se as dezenas forem numéricas e quisermos ordenação numérica no eixo:
    # df_frequency[dezena_col] = pd.to_numeric(df_frequency[dezena_col])
    # sns.barplot(x=dezena_col, y=freq_col_to_plot, data=df_frequency.sort_values(by=dezena_col), ...)
    
    plt.xticks(rotation=70, ha="right") # Aumentada a rotação para melhor visualização
    plt.grid(axis='y', linestyle='--', alpha=0.7)
    plt.tight_layout()
    
    plot_filename = f"frequencia_{metric_type.lower().replace(' ', '_')}_dezenas.png"
    full_plot_path = os.path.join(output_dir, plot_filename)
    
    try:
        plt.savefig(full_plot_path)
        logger.info(f"Gráfico de frequência {metric_type.lower()} salvo em: {full_plot_path}")
    except Exception as e:
        logger.error(f"Erro ao salvar o gráfico de frequência {metric_type.lower()} em '{full_plot_path}': {e}", exc_info=True)
    finally:
        plt.close()


def plot_delay(
    df_delay: pd.DataFrame,
    delay_type: str = 'Atual', 
    output_dir: str = str(PLOT_DIR_CONFIG)
):
    """
    Plota o atraso (atual, máximo ou médio) das dezenas.
    """
    if df_delay is None or df_delay.empty:
        logger.warning(f"DataFrame de atraso '{delay_type}' está vazio. Não é possível gerar o gráfico.")
        return

    if delay_type == 'Atual':
        delay_col_to_plot = next((col for col in ['Atraso Atual', 'atraso_atual', 'Atraso'] if col in df_delay.columns), None)
    elif delay_type == 'Maximo':
        delay_col_to_plot = next((col for col in ['Atraso Maximo', 'atraso_maximo'] if col in df_delay.columns), None)
    elif delay_type == 'Medio':
        delay_col_to_plot = next((col for col in ['Atraso Medio', 'atraso_medio'] if col in df_delay.columns), None)
    else:
        logger.error(f"Tipo de atraso desconhecido: {delay_type}")
        return

    if not delay_col_to_plot:
        logger.error(f"Coluna de atraso para '{delay_type}' não encontrada. Colunas: {df_delay.columns.tolist()}")
        return

    dezena_col = next((col for col in ['Dezena', 'dezena'] if col in df_delay.columns), None)
    if not dezena_col:
        logger.error(f"Coluna 'Dezena' ou 'dezena' não encontrada. Colunas: {df_delay.columns.tolist()}")
        return

    ensure_output_dir(output_dir)

    plt.figure(figsize=(14, 7)) # Aumentado
    plot_data = df_delay.sort_values(by=delay_col_to_plot, ascending=False)
    plot_data[dezena_col] = plot_data[dezena_col].astype(str) # Para ordenação correta no barplot

    sns.barplot(x=dezena_col, y=delay_col_to_plot, data=plot_data, palette="coolwarm", order=plot_data[dezena_col])
    plt.title(f'Atraso {delay_type} das Dezenas da Lotofácil')
    plt.xlabel('Dezena')
    plt.ylabel(f'Atraso {delay_type} (em concursos)')
    plt.xticks(rotation=70, ha="right") # Aumentada a rotação
    plt.grid(axis='y', linestyle='--', alpha=0.7)
    plt.tight_layout()

    plot_filename = f"atraso_{delay_type.lower()}_dezenas.png"
    full_plot_path = os.path.join(output_dir, plot_filename)
    
    try:
        plt.savefig(full_plot_path)
        logger.info(f"Gráfico de atraso {delay_type.lower()} salvo em: {full_plot_path}")
    except Exception as e:
        logger.error(f"Erro ao salvar o gráfico de atraso {delay_type.lower()} em '{full_plot_path}': {e}", exc_info=True)
    finally:
        plt.close()


def plot_chunk_metric_evolution(
    db_manager: DatabaseManager,
    chunk_type: str,
    chunk_size: int,
    metric_to_plot: str, 
    dezenas_to_plot: List[int],
    output_dir: str = str(PLOT_DIR_CONFIG)
):
    """
    Plota a evolução de uma métrica específica para um conjunto de dezenas
    ao longo de diferentes chunks sequenciais.
    """
    logger.info(f"Gerando gráfico de evolução da métrica '{metric_to_plot}' para dezenas {dezenas_to_plot} em chunks {chunk_type}_{chunk_size}.")

    metric_map: Dict[str, Dict[str, str]] = { # Type hint para metric_map
        "Frequencia Absoluta": {"table_suffix": "frequency", "column_name": "frequencia_absoluta"},
    }

    if metric_to_plot not in metric_map:
        logger.error(f"Métrica '{metric_to_plot}' não mapeada. Métricas: {list(metric_map.keys())}")
        return

    metric_info = metric_map[metric_to_plot]
    table_name_core = metric_info["table_suffix"] 
    metric_column = metric_info["column_name"]    

    table_name = f"evol_metric_{table_name_core}_{chunk_type}_{chunk_size}"

    df_evolution: Optional[pd.DataFrame] = None
    try:
        if not db_manager.table_exists(table_name):
            logger.error(f"Tabela '{table_name}' não encontrada.")
            return
        df_evolution = db_manager.load_dataframe_from_db(table_name)
    except Exception as e:
        logger.error(f"Erro ao carregar dados da tabela '{table_name}': {e}", exc_info=True)
        return

    if df_evolution is None or df_evolution.empty:
        logger.warning(f"DataFrame da tabela '{table_name}' vazio ou não carregado.")
        return

    df_plot = df_evolution[df_evolution['dezena'].isin(dezenas_to_plot)]

    if df_plot.empty:
        logger.warning(f"Nenhum dado para as dezenas {dezenas_to_plot} na tabela '{table_name}'.")
        return

    ensure_output_dir(output_dir) # Chamada de ensure_output_dir
    plt.figure(figsize=(15, 8))
    
    # Definir um conjunto de cores para as dezenas para melhor distinção se houver muitas
    # default_colors = plt.cm.get_cmap('tab10', len(dezenas_to_plot)) # 'tab10' tem 10 cores distintas
    # Ou usar seaborn's default palette que é geralmente boa
    
    for i, dezena_val in enumerate(sorted(list(set(dezenas_to_plot)))): # sorted para ordem consistente na legenda
        df_dezena = df_plot[df_plot['dezena'] == dezena_val].sort_values(by='chunk_seq_id')
        if not df_dezena.empty:
            # color = default_colors(i % default_colors.N) # Ciclar cores se mais de 10 dezenas
            plt.plot(
                df_dezena['chunk_seq_id'], 
                df_dezena[metric_column], 
                marker='o', 
                linestyle='-', 
                label=f'Dezena {dezena_val}'
                # color=color # Opcional: para controle manual de cor
            )
        else:
            logger.debug(f"Sem dados para a dezena {dezena_val} para plotagem.")

    plt.title(f"Evolução: {metric_to_plot}\nChunks: {chunk_type.capitalize()} (Tamanho {chunk_size})", fontsize=16)
    plt.xlabel(f"ID Sequencial do Bloco ({chunk_type.capitalize()} de {chunk_size} concursos)", fontsize=12)
    plt.ylabel(metric_to_plot, fontsize=12)
    
    ax = plt.gca()
    ax.xaxis.set_major_locator(ticker.MaxNLocator(integer=True, nbins='auto')) # nbins='auto' para melhor espaçamento
    ax.tick_params(axis='both', which='major', labelsize=10) # Tamanho dos ticks

    plt.legend(title="Dezenas", loc="best", fontsize=10)
    plt.grid(True, linestyle='--', alpha=0.7)
    plt.tight_layout()
    
    dezenas_str = "_".join(map(str, sorted(list(set(dezenas_to_plot)))))
    plot_filename = f"evol_{table_name_core}_{chunk_type}_{chunk_size}_dezenas_{dezenas_str}.png"
    full_plot_path = os.path.join(output_dir, plot_filename)

    try:
        plt.savefig(full_plot_path, dpi=120) # Aumentar DPI para melhor qualidade
        logger.info(f"Gráfico salvo em: {full_plot_path}")
    except Exception as e:
        logger.error(f"Erro ao salvar o gráfico em '{full_plot_path}': {e}", exc_info=True)
    finally:
        plt.close()