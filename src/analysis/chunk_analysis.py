# src/analysis/chunk_analysis.py

import pandas as pd
import numpy as np
import sqlite3
import sys
from typing import Optional, List, Dict # Adicionado Dict

# Importa do config e db_manager
from src.config import logger, ALL_NUMBERS, DATABASE_PATH
from src.database_manager import get_chunk_table_name, read_data_from_db # read_data_from_db não é mais usado aqui diretamente

# Fallback
if 'ALL_NUMBERS' not in globals(): ALL_NUMBERS = list(range(1, 26))


def get_chunk_final_stats(interval_size: int, concurso_maximo: Optional[int] = None) -> Optional[pd.DataFrame]:
    """
    Busca os dados de frequência do ÚLTIMO concurso de cada bloco completo
    e calcula o rank interno, com conversão de tipo robusta na leitura.
    """
    table_name = get_chunk_table_name(interval_size)
    logger.info(f"Buscando stats finais chunks de {interval_size} em '{table_name}'...")

    # 1. Determinar limite superior (usando read_data_from_db helper)
    temp_df_max_c = read_data_from_db(columns=['concurso'])
    if temp_df_max_c is None or temp_df_max_c.empty: return None
    effective_max_concurso = int(temp_df_max_c['concurso'].max())
    limit_concurso = min(concurso_maximo, effective_max_concurso) if concurso_maximo else effective_max_concurso

    # 2. Determinar concursos finais dos chunks
    if limit_concurso < interval_size: return pd.DataFrame()
    last_chunk_end = (limit_concurso // interval_size) * interval_size
    chunk_end_contests = list(range(interval_size, last_chunk_end + 1, interval_size))
    if not chunk_end_contests: return pd.DataFrame()

    logger.info(f"Buscando dados para {len(chunk_end_contests)} chunks (último: {chunk_end_contests[-1]})...")

    # 3. Buscar dados SOMENTE dos concursos finais usando cursor para controle fino
    db_path = DATABASE_PATH
    col_names_d = [f'd{i}' for i in ALL_NUMBERS]
    col_names_sql = ['concurso'] + col_names_d
    col_names_str = ', '.join(f'"{col}"' for col in col_names_sql)
    placeholders = ', '.join(['?'] * len(chunk_end_contests))
    sql = f"SELECT {col_names_str} FROM {table_name} WHERE concurso IN ({placeholders}) ORDER BY concurso ASC"

    chunk_data_list = [] # Lista para armazenar dicionários de cada linha processada

    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            # Verifica se a tabela existe
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?;", (table_name,))
            if cursor.fetchone() is None: logger.error(f"Tabela '{table_name}' não encontrada."); return None

            # Executa a consulta principal
            cursor.execute(sql, chunk_end_contests)
            rows = cursor.fetchall()

            if not rows:
                logger.warning(f"Nenhum dado encontrado para chunks finais em '{table_name}'.")
                return pd.DataFrame()

            # Processa cada linha manualmente para conversão robusta
            col_names_map = {name: idx for idx, name in enumerate(col_names_sql)} # Mapeia nome -> índice

            for row_tuple in rows:
                row_data = {}
                concurso = int(row_tuple[col_names_map['concurso']])
                row_data['concurso_fim_chunk'] = concurso # Nomeia a coluna do índice futuro
                for i in ALL_NUMBERS:
                    col_d_name = f'd{i}'
                    col_idx = col_names_map[col_d_name]
                    count_val = row_tuple[col_idx]
                    dezena = i
                    # --- LÓGICA DE CONVERSÃO ROBUSTA (igual a de get_closest_freq_snapshot) ---
                    try:
                        if isinstance(count_val, int): converted_count = count_val
                        elif isinstance(count_val, bytes): converted_count = int.from_bytes(count_val, byteorder=sys.byteorder, signed=False); # logger.warning(...) Opcional
                        elif count_val is None or pd.isna(count_val): converted_count = 0
                        else: converted_count = int(count_val)
                    except (ValueError, TypeError, OverflowError) as e: logger.error(f"Erro converter chunk d{dezena}, c={concurso}: {e}. Usando 0."); converted_count = 0
                    # --- FIM CONVERSÃO ---
                    row_data[f'{col_d_name}_freq'] = converted_count # Salva com nome _freq
                chunk_data_list.append(row_data)

    except sqlite3.Error as e: logger.error(f"Erro SQLite ao ler chunks '{table_name}': {e}"); return None
    except Exception as e: logger.error(f"Erro inesperado ao ler chunks: {e}"); return None

    # Cria o DataFrame a partir da lista de dicionários
    df_chunk_stats = pd.DataFrame(chunk_data_list)
    if df_chunk_stats.empty: return pd.DataFrame() # Retorna vazio se a lista ficou vazia
    df_chunk_stats.set_index('concurso_fim_chunk', inplace=True)

    # 4. Calcula Ranks a partir das frequências corrigidas
    logger.info("Calculando ranks dentro de cada chunk...")
    rank_cols_names = [f'd{i}_rank' for i in ALL_NUMBERS]
    freq_cols_to_rank = [f'd{i}_freq' for i in ALL_NUMBERS]
    df_ranks = df_chunk_stats[freq_cols_to_rank].rank(axis=1, method='min', ascending=False, pct=False).astype(int)
    df_ranks.columns = rank_cols_names

    # 5. Junta frequências corrigidas e ranks
    df_final_stats = pd.concat([df_chunk_stats, df_ranks], axis=1)

    logger.info(f"Estatísticas finais para {len(df_final_stats)} chunks de {interval_size} calculadas.")
    return df_final_stats


# --- Funções futuras para calcular rank médio, std dev, etc. ---
# (Não implementadas)