# src/analysis/chunk_analysis.py

import pandas as pd
import numpy as np
import sqlite3
import sys
from typing import Optional, List, Dict

# Importa do config e db_manager
from src.config import logger, ALL_NUMBERS, DATABASE_PATH
# Importa a função de nome de tabela e criação (para garantir)
from src.database_manager import get_chunk_final_stats_table_name, create_chunk_stats_final_table

# Fallback
if 'ALL_NUMBERS' not in globals(): ALL_NUMBERS = list(range(1, 26))


def get_chunk_final_stats(interval_size: int, concurso_maximo: Optional[int] = None) -> Optional[pd.DataFrame]:
    """
    Lê as estatísticas finais salvas para cada bloco completo de tamanho 'interval_size'
    até 'concurso_maximo' da tabela correspondente.

    Args:
        interval_size (int): Tamanho do bloco (ex: 10, 25).
        concurso_maximo (Optional[int]): Último concurso final de chunk a considerar.

    Returns:
        Optional[pd.DataFrame]: DataFrame indexado por 'concurso_fim',
                                com colunas _freq e _rank, ou None se erro.
                                Retorna DataFrame vazio se nenhum chunk completo for encontrado.
    """
    table_name = get_chunk_final_stats_table_name(interval_size)
    logger.info(f"Buscando estatísticas finais dos chunks de {interval_size} na tabela '{table_name}'...")
    create_chunk_stats_final_table(interval_size) # Garante que a tabela exista

    db_path = DATABASE_PATH
    freq_cols = [f'd{i}_freq' for i in ALL_NUMBERS]
    rank_cols = [f'd{i}_rank' for i in ALL_NUMBERS]
    all_cols = ['concurso_fim'] + freq_cols + rank_cols
    all_cols_str = ', '.join(f'"{col}"' for col in all_cols)

    try:
        with sqlite3.connect(db_path) as conn:
            sql_query = f"SELECT {all_cols_str} FROM {table_name}"; params = []
            if concurso_maximo is not None:
                # Filtra pelos concursos finais de chunk que são <= concurso_maximo
                sql_query += " WHERE concurso_fim <= ?"
                params.append(concurso_maximo)
            sql_query += " ORDER BY concurso_fim ASC;"

            df_stats = pd.read_sql_query(sql_query, conn, params=params)
            logger.info(f"{len(df_stats)} registros de stats finais de chunk lidos.")

            if df_stats.empty: return pd.DataFrame(columns=all_cols).set_index('concurso_fim') # Retorna DF vazio com colunas

            # Garante tipos corretos (todos devem ser inteiros)
            df_stats.set_index('concurso_fim', inplace=True)
            for col in df_stats.columns:
                df_stats[col] = pd.to_numeric(df_stats[col], errors='coerce').fillna(0).astype(int)

            return df_stats

    except sqlite3.Error as e: logger.error(f"Erro SQLite ao ler '{table_name}': {e}"); return None
    except Exception as e: logger.error(f"Erro inesperado em get_chunk_final_stats: {e}"); return None


# --- Funções futuras ---
# def calculate_historical_rank_stats(...): ...