# src/analysis/chunk_analysis.py

import pandas as pd
import numpy as np
import sqlite3
import sys
from typing import Optional, List, Dict

# Importa do config e db_manager
from src.config import logger, ALL_NUMBERS, DATABASE_PATH
from src.database_manager import get_chunk_final_stats_table_name, create_chunk_stats_final_table

# Fallback
if 'ALL_NUMBERS' not in globals(): ALL_NUMBERS = list(range(1, 26))


def get_chunk_final_stats(interval_size: int, concurso_maximo: Optional[int] = None) -> Optional[pd.DataFrame]:
    """ Lê as estatísticas finais salvas para cada bloco completo. """
    table_name = get_chunk_final_stats_table_name(interval_size)
    logger.info(f"Buscando stats finais chunks de {interval_size} em '{table_name}'...")
    # create_chunk_stats_final_table(interval_size) # Não cria aqui, assume existente

    db_path = DATABASE_PATH
    freq_cols = [f'd{i}_freq' for i in ALL_NUMBERS]
    rank_cols = [f'd{i}_rank' for i in ALL_NUMBERS]
    all_cols = ['concurso_fim'] + freq_cols + rank_cols
    all_cols_str = ', '.join(f'"{col}"' for col in all_cols)

    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?;", (table_name,))
            if cursor.fetchone() is None: logger.error(f"Tabela '{table_name}' não encontrada."); return None

            sql_query = f"SELECT {all_cols_str} FROM {table_name}"; params = []
            if concurso_maximo is not None:
                # Calcula o último concurso final de chunk <= concurso_maximo
                last_relevant_chunk_end = (concurso_maximo // interval_size) * interval_size
                if last_relevant_chunk_end > 0:
                     sql_query += " WHERE concurso_fim <= ?"
                     params.append(last_relevant_chunk_end)
                else:
                     return pd.DataFrame(columns=all_cols).set_index('concurso_fim') # Retorna vazio se nenhum chunk completo

            sql_query += " ORDER BY concurso_fim ASC;"
            df_stats = pd.read_sql_query(sql_query, conn, params=params)
            logger.info(f"{len(df_stats)} registros de stats finais de chunk lidos.")

            if df_stats.empty: return pd.DataFrame(columns=all_cols).set_index('concurso_fim')

            df_stats.set_index('concurso_fim', inplace=True)
            for col in df_stats.columns: # Garante tipos int
                df_stats[col] = pd.to_numeric(df_stats[col], errors='coerce').fillna(0).astype(int)
            return df_stats

    except sqlite3.Error as e: logger.error(f"Erro SQLite ao ler '{table_name}': {e}"); return None
    except Exception as e: logger.error(f"Erro inesperado em get_chunk_final_stats: {e}"); return None


# --- FUNÇÃO IMPLEMENTADA ---
def calculate_historical_rank_stats(interval_size: int, concurso_maximo: Optional[int] = None) -> Optional[pd.DataFrame]:
    """
    Calcula estatísticas (média, std dev) sobre o RANK histórico das dezenas
    nos chunks completos de um determinado intervalo.
    """
    logger.info(f"Calculando stats históricos de rank para chunks de {interval_size} até {concurso_maximo or 'fim'}...")

    # 1. Obtém os ranks finais de cada chunk completo
    df_chunk_stats = get_chunk_final_stats(interval_size, concurso_maximo)

    if df_chunk_stats is None: return None # Erro ao ler
    if df_chunk_stats.empty:
        logger.warning(f"Nenhum chunk stat encontrado para intervalo {interval_size}. Retornando NaNs.")
        nan_series = pd.Series(np.nan, index=ALL_NUMBERS)
        return pd.DataFrame({ f'avg_rank_chunk{interval_size}': nan_series, f'std_rank_chunk{interval_size}': nan_series }, index=pd.Index(ALL_NUMBERS, name='dezena'))
    if len(df_chunk_stats) < 2:
        logger.warning(f"Apenas {len(df_chunk_stats)} chunk(s) encontrados. Std Dev será NaN.")
        # Calcula a média mesmo com 1 chunk
        rank_cols = [f'd{i}_rank' for i in ALL_NUMBERS if f'd{i}_rank' in df_chunk_stats.columns]
        if not rank_cols: return None # Se não houver colunas de rank
        df_ranks_only = df_chunk_stats[rank_cols]
        avg_ranks = df_ranks_only.mean(axis=0, skipna=True)
        # Cria DataFrame de resultado com std dev como NaN
        stats_data = []
        for i in ALL_NUMBERS:
            col_rank = f'd{i}_rank'
            stats_data.append({ 'dezena': i, f'avg_rank_chunk{interval_size}': avg_ranks.get(col_rank, np.nan), f'std_rank_chunk{interval_size}': np.nan })
        return pd.DataFrame(stats_data).set_index('dezena')


    # 2. Seleciona apenas as colunas de rank
    rank_cols = [f'd{i}_rank' for i in ALL_NUMBERS if f'd{i}_rank' in df_chunk_stats.columns]
    if not rank_cols: logger.error(f"Colunas de rank não encontradas p/ intervalo {interval_size}."); return None
    df_ranks_only = df_chunk_stats[rank_cols]

    # 3. Calcula Média e Desvio Padrão
    avg_ranks = df_ranks_only.mean(axis=0, skipna=True)
    std_ranks = df_ranks_only.std(axis=0, skipna=True, ddof=1)

    # 4. Formata o resultado
    stats_data = []
    for i in ALL_NUMBERS:
        col_rank = f'd{i}_rank'
        stats_data.append({
            'dezena': i,
            f'avg_rank_chunk{interval_size}': avg_ranks.get(col_rank, np.nan),
            f'std_rank_chunk{interval_size}': std_ranks.get(col_rank, np.nan) # Pode ser NaN se houver apenas 1 valor não-nulo
        })

    results_df = pd.DataFrame(stats_data).set_index('dezena')
    # Preenche NaNs restantes (ex: se std não pôde ser calculado) com um valor padrão?
    # Média NaN pode significar que a dezena nunca teve rank (sempre freq 0?). Usar rank 26?
    # Std NaN significa poucos dados. Usar 0? Ou um valor alto indicando incerteza? Usar NaN.
    # results_df.fillna({'avg_rank_chunk...': 26, 'std_rank_chunk...': 0}, inplace=True) # Opcional

    logger.info(f"Stats históricos de rank para chunks de {interval_size} calculados.")
    return results_df