# src/analysis/chunk_analysis.py

import pandas as pd
import numpy as np
import sqlite3
import sys # Para ordem de bytes
from typing import Optional, List

# Importa do config e db_manager
from src.config import logger, ALL_NUMBERS, DATABASE_PATH
# get_chunk_table_name vem do db_manager (corrigido)
from src.database_manager import get_chunk_table_name, read_data_from_db

# Fallback
if 'ALL_NUMBERS' not in globals(): ALL_NUMBERS = list(range(1, 26))


def get_chunk_final_stats(interval_size: int, concurso_maximo: Optional[int] = None) -> Optional[pd.DataFrame]:
    """
    Busca os dados de frequência do ÚLTIMO concurso de cada bloco completo
    e calcula o rank interno, tratando corretamente os tipos de dados.
    """
    table_name = get_chunk_table_name(interval_size)
    logger.info(f"Buscando stats finais chunks de {interval_size} em '{table_name}'...")

    # 1. Determinar limite superior
    df_max_c = read_data_from_db(columns=['concurso'])
    if df_max_c is None or df_max_c.empty: return None
    effective_max_concurso = int(df_max_c['concurso'].max())
    limit_concurso = min(concurso_maximo, effective_max_concurso) if concurso_maximo else effective_max_concurso

    # 2. Determinar concursos finais dos chunks
    if limit_concurso < interval_size: return pd.DataFrame()
    last_chunk_end = (limit_concurso // interval_size) * interval_size
    chunk_end_contests = list(range(interval_size, last_chunk_end + 1, interval_size))
    if not chunk_end_contests: return pd.DataFrame()

    logger.info(f"Buscando dados para {len(chunk_end_contests)} chunks completos (finais: {chunk_end_contests[-1]})...")

    # 3. Buscar dados SOMENTE dos concursos finais
    db_path = DATABASE_PATH
    col_names_d = [f'd{i}' for i in ALL_NUMBERS]
    col_names_sql = ['concurso'] + col_names_d
    col_names_str = ', '.join(f'"{col}"' for col in col_names_sql)
    placeholders = ', '.join(['?'] * len(chunk_end_contests))
    sql = f"SELECT {col_names_str} FROM {table_name} WHERE concurso IN ({placeholders}) ORDER BY concurso ASC"

    df_chunk_ends = pd.DataFrame() # Inicializa vazio
    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?;", (table_name,))
            if cursor.fetchone() is None: logger.error(f"Tabela '{table_name}' não encontrada."); return None
            # Usar read_sql_query pode ser mais robusto com tipos que fetchall
            df_chunk_ends = pd.read_sql_query(sql, conn, params=chunk_end_contests)

    except Exception as e: logger.error(f"Erro ao ler chunks '{table_name}': {e}"); return None

    if df_chunk_ends.empty: logger.warning(f"Nenhum dado encontrado para chunks finais em '{table_name}'."); return pd.DataFrame()

    # 4. *** CORREÇÃO DE TIPO EXPLÍCITA ***
    logger.debug("Convertendo colunas de frequência para inteiro...")
    freq_col_data = {}
    for i in ALL_NUMBERS:
        col_d = f'd{i}'
        # Tenta converter para numérico, tratando erros e usando Int64 para NAs
        numeric_col = pd.to_numeric(df_chunk_ends[col_d], errors='coerce').astype('Int64')
        freq_col_data[f'{col_d}_freq'] = numeric_col # Renomeia para _freq

    # Cria DataFrame com colunas de frequência corrigidas
    df_freq_corrected = pd.DataFrame(freq_col_data, index=df_chunk_ends['concurso'])
    df_freq_corrected.fillna(0, inplace=True) # Preenche NAs com 0 (caso Int64 falhe)
    df_freq_corrected = df_freq_corrected.astype(int) # Converte para int padrão após tratar NAs

    # Define o índice
    df_freq_corrected.index.name = 'concurso_fim_chunk'

    # 5. Calcula Ranks a partir das frequências corrigidas
    logger.info("Calculando ranks dentro de cada chunk...")
    rank_cols = [f'd{i}_rank' for i in ALL_NUMBERS]
    df_ranks = df_freq_corrected[[f'd{i}_freq' for i in ALL_NUMBERS]].rank(axis=1, method='min', ascending=False, pct=False).astype(int)
    df_ranks.columns = rank_cols

    # 6. Junta frequências corrigidas e ranks
    df_final_stats = pd.concat([df_freq_corrected, df_ranks], axis=1)

    logger.info(f"Estatísticas finais para {len(df_final_stats)} chunks de {interval_size} calculadas.")
    return df_final_stats

# --- Funções futuras para calcular rank médio, std dev, etc. ---
# (Ainda não implementadas)