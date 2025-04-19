# src/analysis/chunk_analysis.py

import pandas as pd
import numpy as np
import sqlite3
import sys
from typing import Optional, List, Dict
from pathlib import Path # Garante import

# Importa do config e db_manager
from src.config import logger, ALL_NUMBERS, DATABASE_PATH
from src.database_manager import get_chunk_final_stats_table_name, create_chunk_stats_final_table

# Fallback
if 'ALL_NUMBERS' not in globals(): ALL_NUMBERS = list(range(1, 26))


def get_chunk_final_stats(interval_size: int, concurso_maximo: Optional[int] = None, db_path: Path = DATABASE_PATH) -> Optional[pd.DataFrame]: # Adiciona db_path
    """ Lê as estatísticas finais salvas para cada bloco completo. """
    table_name = get_chunk_final_stats_table_name(interval_size)
    logger.info(f"Buscando stats finais chunks de {interval_size} em '{table_name}' (até {concurso_maximo or 'fim'})...")
    # create_chunk_stats_final_table(interval_size, db_path) # Não cria aqui

    freq_cols = [f'd{i}_freq' for i in ALL_NUMBERS]
    rank_cols = [f'd{i}_rank' for i in ALL_NUMBERS]
    all_cols = ['concurso_fim'] + freq_cols + rank_cols
    all_cols_str = ', '.join(f'"{col}"' for col in all_cols)
    empty_df = pd.DataFrame(columns=all_cols[1:], index=pd.Index([], name='concurso_fim')).astype(int) # Cria DF vazio padrão

    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?;", (table_name,))
            if cursor.fetchone() is None: logger.error(f"Tabela '{table_name}' não encontrada."); return None

            sql_query = f"SELECT {all_cols_str} FROM {table_name}"; params = []
            if concurso_maximo is not None:
                last_relevant_chunk_end = (concurso_maximo // interval_size) * interval_size
                if last_relevant_chunk_end >= interval_size : sql_query += " WHERE concurso_fim <= ?"; params.append(last_relevant_chunk_end)
                else: return empty_df

            sql_query += " ORDER BY concurso_fim ASC;"
            df_stats = pd.read_sql_query(sql_query, conn, params=params)
            logger.info(f"{len(df_stats)} registros de stats finais lidos (int={interval_size}).")

            if df_stats.empty: return empty_df

            df_stats.set_index('concurso_fim', inplace=True)
            for col in df_stats.columns: df_stats[col] = pd.to_numeric(df_stats[col], errors='coerce').fillna(0).astype(int)
            return df_stats

    except Exception as e: logger.error(f"Erro ao ler stats finais chunk '{table_name}': {e}"); return None


# --- FUNÇÃO IMPLEMENTADA ---
def calculate_historical_rank_stats(interval_size: int, concurso_maximo: Optional[int] = None) -> Optional[pd.DataFrame]:
    """ Calcula média e std dev do RANK histórico das dezenas nos chunks. """
    logger.info(f"Calculando stats históricos de rank para chunks de {interval_size}...")

    df_chunk_stats = get_chunk_final_stats(interval_size, concurso_maximo)

    nan_series = pd.Series(np.nan, index=ALL_NUMBERS); default_result_df = pd.DataFrame({ f'avg_rank_chunk{interval_size}': nan_series, f'std_rank_chunk{interval_size}': nan_series }, index=pd.Index(ALL_NUMBERS, name='dezena'))

    if df_chunk_stats is None: return None
    if df_chunk_stats.empty: return default_result_df # Retorna NaNs se vazio

    rank_cols = [f'd{i}_rank' for i in ALL_NUMBERS if f'd{i}_rank' in df_chunk_stats.columns]
    if len(rank_cols) != 25: logger.error(f"Colunas de rank insuficientes p/ int {interval_size}."); return None
    df_ranks_only = df_chunk_stats[rank_cols]

    avg_ranks = df_ranks_only.mean(axis=0, skipna=True)
    std_ranks = pd.Series(np.nan, index=rank_cols)
    if len(df_ranks_only) >= 2: std_ranks = df_ranks_only.std(axis=0, skipna=True, ddof=1)

    stats_data = []
    for i in ALL_NUMBERS:
        col_rank_key = f'd{i}_rank'
        stats_data.append({ 'dezena': i, f'avg_rank_chunk{interval_size}': avg_ranks.get(col_rank_key, np.nan), f'std_rank_chunk{interval_size}': std_ranks.get(col_rank_key, np.nan) })

    results_df = pd.DataFrame(stats_data).set_index('dezena')
    logger.info(f"Stats históricos de rank para chunks de {interval_size} calculados.")
    return results_df