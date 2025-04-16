# src/analysis/chunk_analysis.py

import pandas as pd
import numpy as np
import sqlite3
from typing import Optional, List

# Importa do config e db_manager
from src.config import logger, ALL_NUMBERS, DATABASE_PATH
from src.database_manager import get_chunk_table_name, read_data_from_db # Usamos read_data_from_db por enquanto

# Fallback
if 'ALL_NUMBERS' not in globals(): ALL_NUMBERS = list(range(1, 26))


def get_chunk_final_stats(interval_size: int, concurso_maximo: Optional[int] = None) -> Optional[pd.DataFrame]:
    """
    Busca os dados de frequência do ÚLTIMO concurso de cada bloco completo
    de tamanho 'interval_size' até 'concurso_maximo' e calcula o rank interno.

    Args:
        interval_size (int): Tamanho do bloco (ex: 10, 25).
        concurso_maximo (Optional[int]): Último concurso a considerar. Se None, usa todos.

    Returns:
        Optional[pd.DataFrame]: DataFrame indexado por 'concurso_fim_chunk',
                                com colunas d1_freq...d25_freq e d1_rank...d25_rank,
                                ou None se a tabela não existir ou erro.
                                Retorna DataFrame vazio se nenhum chunk completo for encontrado.
    """
    table_name = get_chunk_table_name(interval_size)
    logger.info(f"Buscando estatísticas finais dos chunks de {interval_size} na tabela '{table_name}'...")

    # 1. Determinar o limite superior real para buscar os chunks
    # Poderíamos ler o MAX(concurso) da tabela de chunks, mas ler da tabela principal é mais seguro
    # para garantir que não buscamos chunks além dos sorteios existentes.
    df_max_c = read_data_from_db(columns=['concurso']) # Leitura rápida para pegar o último concurso
    if df_max_c is None or df_max_c.empty:
        logger.error("Não foi possível determinar o último concurso na tabela principal.")
        return None
    effective_max_concurso = int(df_max_c['concurso'].max())
    limit_concurso = min(concurso_maximo, effective_max_concurso) if concurso_maximo else effective_max_concurso

    # 2. Determinar os concursos que marcam o fim de cada chunk completo
    if limit_concurso < interval_size:
        logger.warning(f"Concurso máximo {limit_concurso} é menor que o intervalo {interval_size}. Nenhum chunk completo.")
        return pd.DataFrame() # Retorna DF vazio
    # O último concurso que finaliza um chunk é o maior múltiplo de interval_size <= limit_concurso
    last_chunk_end = (limit_concurso // interval_size) * interval_size
    chunk_end_contests = list(range(interval_size, last_chunk_end + 1, interval_size))

    if not chunk_end_contests:
        logger.warning(f"Nenhum ponto final de chunk encontrado até {limit_concurso} para intervalo {interval_size}.")
        return pd.DataFrame()

    logger.info(f"Buscando dados para {len(chunk_end_contests)} chunks completos (concursos finais: {chunk_end_contests[:3]}...{chunk_end_contests[-1]})...")

    # 3. Buscar os dados SOMENTE para esses concursos finais na tabela de chunk
    # Usar pd.read_sql_query para buscar múltiplos concursos de forma eficiente
    db_path = DATABASE_PATH
    col_names = ['concurso'] + [f'd{i}' for i in ALL_NUMBERS]
    col_names_str = ', '.join(f'"{col}"' for col in col_names)
    placeholders = ', '.join(['?'] * len(chunk_end_contests))
    sql = f"SELECT {col_names_str} FROM {table_name} WHERE concurso IN ({placeholders}) ORDER BY concurso ASC"

    try:
        with sqlite3.connect(db_path) as conn:
            # Verifica se a tabela existe antes de consultar
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?;", (table_name,))
            if cursor.fetchone() is None:
                logger.error(f"Tabela de chunk '{table_name}' não encontrada no banco. Execute a reconstrução primeiro.")
                return None

            # Executa a consulta
            df_chunk_ends = pd.read_sql_query(sql, conn, params=chunk_end_contests)

    except sqlite3.Error as e:
        logger.error(f"Erro ao ler dados da tabela '{table_name}': {e}")
        return None
    except Exception as e:
        logger.error(f"Erro inesperado ao ler chunks: {e}")
        return None

    if df_chunk_ends.empty:
        logger.warning(f"Nenhum dado encontrado para os concursos finais dos chunks na tabela '{table_name}'.")
        return pd.DataFrame()

    # Define o concurso final como índice
    df_chunk_ends.set_index('concurso', inplace=True)
    df_chunk_ends.index.name = 'concurso_fim_chunk'

    # Renomeia colunas de frequência para clareza
    freq_cols = {f'd{i}': f'd{i}_freq' for i in ALL_NUMBERS}
    df_chunk_ends.rename(columns=freq_cols, inplace=True)

    # 4. Calcula os Ranks para cada linha (cada chunk final)
    logger.info("Calculando ranks dentro de cada chunk...")
    rank_cols = [f'd{i}_rank' for i in ALL_NUMBERS]
    # axis=1 aplica o rank em cada linha (concurso)
    # ascending=False faz com que a maior frequência receba rank 1
    # pct=False retorna o rank numérico (1, 2, 3...)
    # method='min' atribui o mesmo rank para empates (o menor rank do grupo)
    df_ranks = df_chunk_ends[[f'd{i}_freq' for i in ALL_NUMBERS]].rank(axis=1, method='min', ascending=False, pct=False).astype(int)
    df_ranks.columns = rank_cols # Renomeia colunas do DataFrame de ranks

    # 5. Junta os ranks ao DataFrame original
    df_final_stats = pd.concat([df_chunk_ends, df_ranks], axis=1)

    logger.info(f"Estatísticas finais para {len(df_final_stats)} chunks de {interval_size} calculadas.")
    return df_final_stats

# --- Funções futuras para calcular rank médio, std dev, etc. ---
# def calculate_historical_rank_stats(interval_size: int, concurso_maximo: Optional[int] = None):
#    df_stats = get_chunk_final_stats(interval_size, concurso_maximo)
#    if df_stats is None: return None
#    rank_cols = [f'd{i}_rank' for i in ALL_NUMBERS]
#    df_ranks_only = df_stats[rank_cols]
#    # Calcular média, mediana, std dev para cada coluna dX_rank
#    # ...
#    pass