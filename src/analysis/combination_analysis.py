# src/analysis/combination_analysis.py

import pandas as pd
from itertools import combinations
from collections import Counter
from typing import Optional, List, Tuple

# Importações locais
from src.database_manager import read_data_from_db
from src.config import logger, NEW_BALL_COLUMNS # NEW_BALL_COLUMNS são b1 a b15

# Colunas necessárias do banco de dados
BASE_COLS = ['concurso'] + NEW_BALL_COLUMNS

def calculate_combination_frequency(combination_size: int,
                                    top_n: int = 20,
                                    concurso_maximo: Optional[int] = None) -> List[Tuple[Tuple[int, ...], int]]:
    """
    Calcula a frequência de combinações de N dezenas sorteadas juntas.

    Args:
        combination_size (int): O tamanho da combinação (ex: 3 para trios, 4 para quartetos).
        top_n (int): Quantas das combinações mais frequentes retornar.
        concurso_maximo (Optional[int]): O último concurso a ser considerado. Se None, usa todos.

    Returns:
        List[Tuple[Tuple[int, ...], int]]: Uma lista das top_n combinações mais frequentes,
                                           onde cada item é uma tupla contendo:
                                           (tupla_ordenada_da_combinacao, contagem).
                                           Retorna lista vazia se não houver dados ou erro.
    """
    if not 2 <= combination_size <= 15:
        logger.error(f"Tamanho da combinação inválido: {combination_size}. Deve ser entre 2 e 15.")
        return []

    size_name = {3: "trios", 4: "quartetos", 5: "quintetos", 6: "sextetos"}.get(combination_size, f"{combination_size}-tuplas")
    logger.info(f"Calculando frequência de {size_name} até o concurso {concurso_maximo or 'último'}...")

    df = read_data_from_db(columns=BASE_COLS, concurso_maximo=concurso_maximo)

    if df is None or df.empty:
        logger.warning("Nenhum dado encontrado para calcular frequências de combinação.")
        return []

    # Verifica se todas as colunas de bolas esperadas estão presentes
    if not all(col in df.columns for col in NEW_BALL_COLUMNS):
        logger.error("Dados lidos do banco não contêm todas as colunas de bolas esperadas (b1 a b15).")
        return []

    combination_counter = Counter()

    # Itera sobre cada linha (sorteio) do DataFrame
    for index, row in df.iterrows():
        # Pega as 15 dezenas sorteadas nesta linha, ignorando possíveis NaNs e convertendo para int
        drawn_numbers = [int(num) for num in row[NEW_BALL_COLUMNS].dropna().values]

        # Verifica se temos pelo menos o número necessário de dezenas para formar a combinação
        if len(drawn_numbers) >= combination_size:
            # Gera todas as combinações do tamanho especificado
            # combinations retorna tuplas já ordenadas se a entrada estiver ordenada,
            # mas garantimos ordenando a tupla resultante para segurança.
            for combo in combinations(drawn_numbers, combination_size):
                sorted_combo = tuple(sorted(combo)) # Garante a ordem para contagem
                combination_counter[sorted_combo] += 1
        else:
             # Isso não deve acontecer com dados da Lotofácil (sempre 15 bolas)
             logger.warning(f"Sorteio {row['concurso']} tem menos de {combination_size} números válidos. Ignorando.")


    # Pega as top_n combinações mais comuns
    most_common_combos = combination_counter.most_common(top_n)

    logger.info(f"Cálculo de frequência de {size_name} concluído. {len(combination_counter)} combinações únicas encontradas.")
    return most_common_combos