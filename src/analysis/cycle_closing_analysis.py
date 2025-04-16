# src/analysis/cycle_closing_analysis.py

import pandas as pd
from collections import Counter
from typing import Optional, Dict, Set, Tuple, List # Adicionado List

# Importa do config (SEM BASE_COLS)
from src.config import logger, ALL_NUMBERS, ALL_NUMBERS_SET, NEW_BALL_COLUMNS
# Importa do database_manager (SEM get_cycles_df)
from src.database_manager import get_draw_numbers, read_data_from_db
# Importa do cycle_analysis
from src.analysis.cycle_analysis import get_cycles_df

# Fallbacks e definições locais
if 'ALL_NUMBERS' not in globals(): ALL_NUMBERS = list(range(1, 26))
if 'ALL_NUMBERS_SET' not in globals(): ALL_NUMBERS_SET = set(ALL_NUMBERS)
if 'NEW_BALL_COLUMNS' not in globals(): NEW_BALL_COLUMNS = [f'b{i}' for i in range(1,16)]
# *** DEFINE BASE_COLS LOCALMENTE ***
BASE_COLS: List[str] = ['concurso'] + NEW_BALL_COLUMNS


def calculate_closing_number_stats(concurso_maximo: Optional[int] = None) -> Optional[pd.DataFrame]:
    """
    Analisa quais dezenas foram responsáveis por fechar cada ciclo completo.
    """
    logger.info(f"Calculando estatísticas de fechamento de ciclo até {concurso_maximo or 'último'}...")

    # 1. Obter ciclos completos relevantes
    cycles_df = get_cycles_df(concurso_maximo=concurso_maximo)
    if cycles_df is None or cycles_df.empty:
        logger.warning("Nenhum ciclo completo encontrado para análise de fechamento.")
        # Usa ALL_NUMBERS definido localmente/importado
        return pd.DataFrame({'closing_freq': 0,'sole_closing_freq': 0}, index=pd.Index(ALL_NUMBERS, name='dezena'))

    logger.info(f"Analisando {len(cycles_df)} ciclos completos...")
    closing_counter = Counter(); sole_closing_counter = Counter(); processed_cycles = 0

    # 2. Iterar sobre cada ciclo
    for index, cycle_row in cycles_df.iterrows():
        try:
            cycle_num = int(cycle_row['numero_ciclo'])
            start_c = int(cycle_row['concurso_inicio'])
            end_c = int(cycle_row['concurso_fim'])
        except Exception as e: logger.error(f"Erro linha ciclo {index}: {e}"); continue

        concurso_fim_menos_1 = end_c - 1
        seen_before_end: Set[int] = set()

        # Busca dados até um antes do fim
        if start_c <= concurso_fim_menos_1:
            # Usa BASE_COLS definido localmente
            df_before_end = read_data_from_db(columns=BASE_COLS, concurso_minimo=start_c, concurso_maximo=concurso_fim_menos_1)
            if df_before_end is None: continue # Pula se erro na leitura
            # Calcula conjunto visto antes
            for _, draw_row in df_before_end.iterrows():
                # Usa NEW_BALL_COLUMNS importado
                drawn = {int(n) for n in draw_row[NEW_BALL_COLUMNS].dropna().values}
                seen_before_end.update(drawn)

        drawn_at_end = get_draw_numbers(end_c)
        if drawn_at_end is None: continue # Pula se erro na leitura

        # Verifica consistência (Usa ALL_NUMBERS_SET importado)
        if seen_before_end.union(drawn_at_end) != ALL_NUMBERS_SET:
             logger.warning(f"Inconsistência ciclo {cycle_num} ({start_c}-{end_c}). União!=25.")
             continue

        # Identifica fechadoras (Usa ALL_NUMBERS_SET importado)
        missing_before_end = ALL_NUMBERS_SET - seen_before_end
        closing_numbers = drawn_at_end.intersection(missing_before_end)
        if not closing_numbers: logger.warning(f"Ciclo {cycle_num} sem dezenas fechadoras?"); continue

        logger.debug(f"Ciclo {cycle_num}: Fechadoras: {closing_numbers}")
        closing_counter.update(closing_numbers)
        if len(closing_numbers) == 1: sole_closing_counter.update(closing_numbers)

        processed_cycles += 1
        if processed_cycles % 100 == 0: logger.info(f"{processed_cycles}/{len(cycles_df)} ciclos proc...")

    # 3. Cria DataFrame final (Usa ALL_NUMBERS local/importado)
    stats_df = pd.DataFrame(index=pd.Index(ALL_NUMBERS, name='dezena'))
    stats_df['closing_freq'] = stats_df.index.map(closing_counter).fillna(0).astype(int)
    stats_df['sole_closing_freq'] = stats_df.index.map(sole_closing_counter).fillna(0).astype(int)
    logger.info("Cálculo de stats de fechamento concluído.")
    return stats_df