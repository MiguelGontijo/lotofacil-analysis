# src/analysis/combination_analysis.py

import pandas as pd
from itertools import combinations
from collections import Counter
from typing import Optional, List, Tuple

from src.database_manager import read_data_from_db
from src.config import logger, NEW_BALL_COLUMNS

BASE_COLS = ['concurso'] + NEW_BALL_COLUMNS

def calculate_combination_frequency(combination_size: int,
                                    top_n: int = 20,
                                    concurso_maximo: Optional[int] = None) -> List[Tuple[Tuple[int, ...], int]]:
    """
    Calcula a frequência de combinações de N dezenas sorteadas juntas.
    RETORNA: Lista de tuplas [(combinacao, contagem)] ou lista vazia.
    """
    if not 2 <= combination_size <= 15:
        logger.error(f"Tamanho inválido: {combination_size}. Use 2 a 15.")
        return []

    size_name_map = {2:"pares", 3:"trios", 4:"quartetos", 5:"quintetos", 6:"sextetos"}
    size_name = size_name_map.get(combination_size, f"{combination_size}-tuplas")
    logger.info(f"Calculando frequência de {size_name} até {concurso_maximo or 'último'}...")

    df = read_data_from_db(columns=BASE_COLS, concurso_maximo=concurso_maximo)
    if df is None or df.empty:
        logger.warning("Nenhum dado para calcular frequências de combinação.")
        return []
    if not all(col in df.columns for col in NEW_BALL_COLUMNS):
        logger.error("Dados lidos não contêm colunas de bolas esperadas.")
        return []

    combination_counter = Counter()
    for index, row in df.iterrows():
        # Garante que são inteiros e remove NAs
        drawn_numbers = [int(num) for num in row[NEW_BALL_COLUMNS].dropna().values]
        if len(drawn_numbers) >= combination_size:
            for combo in combinations(drawn_numbers, combination_size):
                combination_counter[tuple(sorted(combo))] += 1
        # else: logger.warning(...) # Aviso se concurso tiver < 15 bolas

    most_common_combos = combination_counter.most_common(top_n)
    logger.info(f"Cálculo de frequência de {size_name} concluído. {len(combination_counter)} únicas encontradas.")
    return most_common_combos # <<< RETORNA A LISTA