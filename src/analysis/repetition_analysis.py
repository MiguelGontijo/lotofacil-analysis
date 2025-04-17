# src/analysis/repetition_analysis.py

import pandas as pd
from collections import Counter
from typing import Optional, Set, List

# Importa do config
from src.config import logger, ALL_NUMBERS, NEW_BALL_COLUMNS, TABLE_NAME, BASE_COLS
# Importa do database_manager
from src.database_manager import read_data_from_db

# Fallbacks
if 'ALL_NUMBERS' not in globals(): ALL_NUMBERS = list(range(1, 26))
if 'NEW_BALL_COLUMNS' not in globals(): NEW_BALL_COLUMNS = [f'b{i}' for i in range(1,16)]
if 'BASE_COLS' not in globals(): BASE_COLS = ['concurso'] + NEW_BALL_COLUMNS


def calculate_historical_repetition_rate(concurso_maximo: Optional[int] = None) -> Optional[pd.Series]:
    """
    Calcula a taxa histórica de repetição imediata para cada dezena.
    Taxa = (Nº de vezes que repetiu N | N-1) / (Nº total de aparições em N-1)

    Args:
        concurso_maximo (Optional[int]): Último concurso a considerar na análise histórica.

    Returns:
        Optional[pd.Series]: Series indexada por dezena (1-25) com a taxa de repetição (0.0 a 1.0),
                             ou None se erro. Retorna zeros se dados insuficientes.
    """
    logger.info(f"Calculando taxa histórica de repetição até {concurso_maximo or 'último'}...")
    df_draws = read_data_from_db(columns=BASE_COLS, concurso_maximo=concurso_maximo)

    if df_draws is None or len(df_draws) < 2:
        logger.warning(f"Dados insuficientes para taxa de repetição.")
        return pd.Series(0.0, index=ALL_NUMBERS, name='repetition_rate')

    appearances_N_minus_1 = Counter() # Conta aparições em N-1 (oportunidades de repetir)
    repetitions_N_given_N_minus_1 = Counter() # Conta repetições N | N-1
    set_N_minus_1: Optional[Set[int]] = None

    # Itera pelas linhas do DataFrame
    for index, row in df_draws.iterrows():
        try: # Valida linha
            set_N = {int(n) for n in row[NEW_BALL_COLUMNS].dropna().values}
            if len(set_N) != 15: raise ValueError("Bolas inválidas")
        except Exception as e:
            logger.warning(f"Pulando linha inválida {row.get('concurso','N/A')} no calc repetição: {e}")
            set_N_minus_1 = None # Anula para próxima iteração não comparar com inválido
            continue

        # Se set_N_minus_1 não for None (ou seja, a linha anterior foi válida)
        if set_N_minus_1 is not None:
            for num in ALL_NUMBERS:
                # Verifica se estava no sorteio anterior (oportunidade)
                if num in set_N_minus_1:
                    appearances_N_minus_1[num] += 1
                    # Verifica se repetiu no sorteio atual
                    if num in set_N:
                        repetitions_N_given_N_minus_1[num] += 1

        # Atualiza o conjunto anterior para a próxima iteração
        set_N_minus_1 = set_N

    # Calcula a taxa final
    rates = {}
    for num in ALL_NUMBERS:
        possible_reps = appearances_N_minus_1.get(num, 0)
        actual_reps = repetitions_N_given_N_minus_1.get(num, 0)
        # Taxa = Repetições / Oportunidades (Aparições em N-1)
        rates[num] = (actual_reps / possible_reps) if possible_reps > 0 else 0.0

    repetition_series = pd.Series(rates, name='repetition_rate').sort_index()
    logger.info("Cálculo da taxa histórica de repetição concluído.")
    return repetition_series