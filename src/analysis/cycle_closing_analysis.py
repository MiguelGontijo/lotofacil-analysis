# src/analysis/cycle_closing_analysis.py

import pandas as pd
from collections import Counter
import numpy as np
import sqlite3
import sys
from typing import List, Dict, Optional, Tuple, Set
from pathlib import Path # Garante que Path está importado

# Importações
from src.database_manager import read_data_from_db, get_draw_numbers # Removido create_cycles_table, get_last_cycle_end, save_to_db
# Importa do config
from src.config import (
    logger, NEW_BALL_COLUMNS, ALL_NUMBERS, ALL_NUMBERS_SET,
    DATABASE_PATH, TABLE_NAME, CYCLES_TABLE_NAME, BASE_COLS
)
# Importa de frequency_analysis
from src.analysis.frequency_analysis import calculate_frequency
# Importa get_cycles_df de cycle_analysis (onde ele está definido)
# from src.analysis.cycle_analysis import get_cycles_df # Não precisa importar, pois este arquivo define as funções de ciclo agora? NÃO. Precisa importar

# Re-adicionando a função get_cycles_df aqui para garantir que está disponível
# Idealmente, esta função estaria em database_manager ou cycle_analysis_base, mas vamos mantê-la aqui por enquanto
from src.database_manager import create_cycles_table # Precisa para get_cycles_df
def get_cycles_df(concurso_maximo: Optional[int] = None, db_path: Path = DATABASE_PATH) -> Optional[pd.DataFrame]:
    """ Lê os dados da tabela 'ciclos' e garante tipos INT padrão. """
    cycles_table_name = CYCLES_TABLE_NAME
    logger.info(f"Buscando dados da tabela '{cycles_table_name}' {('ate ' + str(concurso_maximo)) if concurso_maximo else ''}...")
    create_cycles_table(db_path);
    try:
        with sqlite3.connect(db_path) as conn:
            sql_query = f"SELECT numero_ciclo, concurso_inicio, concurso_fim, duracao FROM {cycles_table_name}"; params = []
            if concurso_maximo is not None: sql_query += " WHERE concurso_fim <= ?"; params.append(concurso_maximo)
            sql_query += " ORDER BY numero_ciclo ASC;"
            df = pd.read_sql_query(sql_query, conn, params=params);
            log_msg = f"{len(df)} ciclos lidos da tabela '{cycles_table_name}'."
            if df is None: logger.error(f"Leitura de '{cycles_table_name}' retornou None."); return None
            if not df.empty:
                 logger.debug(f"Tipos ANTES da conversão em get_cycles_df:\n{df.dtypes}")
                 for col in ['numero_ciclo', 'concurso_inicio', 'concurso_fim', 'duracao']:
                     if col in df.columns: df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
                 logger.debug(f"Tipos DEPOIS da conversão em get_cycles_df:\n{df.dtypes}")
            else: logger.info("Nenhum ciclo encontrado na tabela para o período especificado.")
            logger.info(log_msg)
            return df
    except Exception as e: logger.error(f"Erro ao ler '{cycles_table_name}': {e}"); return None


# --- FUNÇÃO calculate_closing_number_stats CORRIGIDA ---
def calculate_closing_number_stats(cycles_df: Optional[pd.DataFrame]) -> Optional[pd.DataFrame]: # Recebe o DF como argumento
    """ Analisa quais dezenas foram responsáveis por fechar cada ciclo completo. """
    logger.info(f"Calculando estatísticas de fechamento de ciclo...")

    # *** USA O cycles_df RECEBIDO COMO ARGUMENTO ***
    # cycles_df = get_cycles_df(concurso_maximo=concurso_maximo) # <<< REMOVE CHAMADA INTERNA

    # Verificação padrão usando .empty
    if cycles_df is None or cycles_df.empty:
        logger.warning(f"Nenhum ciclo completo fornecido para análise de fechamento.")
        return pd.DataFrame({'closing_freq': 0,'sole_closing_freq': 0}, index=pd.Index(ALL_NUMBERS, name='dezena'))

    df_len = len(cycles_df)
    logger.info(f"Analisando {df_len} ciclos completos para fechamento...") # Log OK
    closing_counter = Counter(); sole_closing_counter = Counter(); processed_cycles = 0

    # Loop principal (Restaurado e usa o cycles_df do argumento)
    for index, cycle_row in cycles_df.iterrows():
        try: cycle_num, start_c, end_c = int(cycle_row['numero_ciclo']), int(cycle_row['concurso_inicio']), int(cycle_row['concurso_fim'])
        except Exception as e: logger.error(f"Erro linha ciclo {index}: {e}"); continue
        concurso_fim_menos_1 = end_c - 1; seen_before_end: Set[int] = set()
        if start_c <= concurso_fim_menos_1:
            # Usa BASE_COLS importado do config
            df_before_end = read_data_from_db(columns=BASE_COLS, concurso_minimo=start_c, concurso_maximo=concurso_fim_menos_1)
            if df_before_end is None: continue
            if not df_before_end.empty:
                 # Usa NEW_BALL_COLUMNS importado do config
                 for _, draw_row in df_before_end.iterrows(): seen_before_end.update({int(n) for n in draw_row[NEW_BALL_COLUMNS].dropna().values})
        drawn_at_end = get_draw_numbers(end_c)
        if drawn_at_end is None: continue
        union_set = seen_before_end.union(drawn_at_end)
        # Usa ALL_NUMBERS_SET importado do config
        if union_set != ALL_NUMBERS_SET: logger.warning(f"Inconsistência ciclo {cycle_num}."); continue
        missing_before_end = ALL_NUMBERS_SET - seen_before_end; closing_numbers = drawn_at_end.intersection(missing_before_end)
        if not closing_numbers: logger.warning(f"Ciclo {cycle_num} sem fechadoras?"); continue
        closing_counter.update(closing_numbers)
        if len(closing_numbers) == 1: sole_closing_counter.update(closing_numbers)
        processed_cycles += 1
        if processed_cycles % 100 == 0: logger.info(f"{processed_cycles}/{df_len} ciclos proc. p/ fechamento...")

    # Cria DataFrame final (Usa ALL_NUMBERS importado)
    stats_df = pd.DataFrame(index=pd.Index(ALL_NUMBERS, name='dezena')); stats_df['closing_freq'] = stats_df.index.map(closing_counter).fillna(0).astype(int); stats_df['sole_closing_freq'] = stats_df.index.map(sole_closing_counter).fillna(0).astype(int)
    logger.info("Cálculo de stats de fechamento concluído."); return stats_df


# --- As outras funções DESTE ARQUIVO (_find_new_cycles, update_cycles_table, etc.) ---
# --- PRECISAM ESTAR AQUI COMPLETAS ---
# (Omitidas por brevidade, mas use a versão completa da última resposta onde elas funcionavam)
# ... (Definição completa de _find_new_cycles_in_data) ...
# ... (Definição completa de update_cycles_table) ...
# ... (Definição completa de calculate_frequency_per_cycle) ...
# ... (Definição completa de calculate_current_incomplete_cycle_stats) ...
# ... (Definição completa de calculate_current_intra_cycle_delay) ...
# ... (Definição completa de calculate_historical_intra_cycle_delay_stats) ...
# ... (Definição completa de run_cycle_frequency_analysis) ...