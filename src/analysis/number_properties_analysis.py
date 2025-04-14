# src/analysis/number_properties_analysis.py

import pandas as pd
from typing import Optional, Dict, Tuple

# Importações locais
from src.database_manager import read_data_from_db
from src.config import logger, NEW_BALL_COLUMNS

# --- Definições das Propriedades ---
ALL_NUMBERS_SET = set(range(1, 26))
PRIMES_SET = {2, 3, 5, 7, 11, 13, 17, 19, 23}
FRAME_SET = {1, 2, 3, 4, 5, 6, 10, 11, 15, 16, 20, 21, 22, 23, 24, 25}
CENTER_SET = ALL_NUMBERS_SET - FRAME_SET # Miolo é o que não está na moldura

# Colunas necessárias do banco de dados
BASE_COLS = ['concurso'] + NEW_BALL_COLUMNS

def analyze_number_properties(concurso_maximo: Optional[int] = None) -> Optional[pd.DataFrame]:
    """
    Analisa propriedades das dezenas sorteadas em cada concurso (par/ímpar, primos, moldura/miolo).

    Args:
        concurso_maximo (Optional[int]): O último concurso a ser considerado. Se None, usa todos.

    Returns:
        Optional[pd.DataFrame]: DataFrame com as contagens de cada propriedade por concurso,
                                 ou None se erro/sem dados.
    """
    logger.info(f"Analisando propriedades dos números até o concurso {concurso_maximo or 'último'}...")

    df = read_data_from_db(columns=BASE_COLS, concurso_maximo=concurso_maximo)

    if df is None or df.empty:
        logger.warning("Nenhum dado encontrado para analisar propriedades dos números.")
        return None

    if not all(col in df.columns for col in NEW_BALL_COLUMNS):
        logger.error("Dados lidos não contêm todas as colunas de bolas esperadas.")
        return None

    results = []
    for index, row in df.iterrows():
        concurso = row['concurso']
        # Garante que temos apenas inteiros válidos
        drawn_numbers = {int(num) for num in row[NEW_BALL_COLUMNS].dropna().values}

        if len(drawn_numbers) != 15:
             logger.warning(f"Concurso {concurso} não tem 15 dezenas válidas. Ignorando.")
             continue # Pula este sorteio se não tiver 15 números

        even_count = sum(1 for n in drawn_numbers if n % 2 == 0)
        odd_count = 15 - even_count
        prime_count = sum(1 for n in drawn_numbers if n in PRIMES_SET)
        frame_count = sum(1 for n in drawn_numbers if n in FRAME_SET)
        center_count = 15 - frame_count

        results.append({
            'concurso': concurso,
            'pares': even_count,
            'impares': odd_count,
            'primos': prime_count,
            'moldura': frame_count,
            'miolo': center_count
        })

    if not results:
        logger.warning("Nenhum resultado gerado na análise de propriedades.")
        return pd.DataFrame() # Retorna DataFrame vazio

    logger.info("Análise de propriedades por concurso concluída.")
    return pd.DataFrame(results)

def summarize_properties(props_df: pd.DataFrame) -> Dict[str, pd.Series]:
    """
    Calcula estatísticas resumidas (frequência de cada distribuição)
    a partir do DataFrame de propriedades por concurso.

    Args:
        props_df (pd.DataFrame): DataFrame gerado por analyze_number_properties.

    Returns:
        Dict[str, pd.Series]: Dicionário contendo Series de value_counts para
                               cada propriedade ('par_impar', 'primos', 'moldura_miolo').
    """
    summaries = {}
    if props_df is None or props_df.empty:
        return summaries

    # Resumo Pares/Ímpares
    # Cria uma tupla (pares, impares) para cada linha para poder contar as distribuições
    props_df['par_impar'] = props_df.apply(lambda row: f"{row['impares']}I / {row['pares']}P", axis=1)
    summaries['par_impar'] = props_df['par_impar'].value_counts().sort_index()

    # Resumo Primos
    summaries['primos'] = props_df['primos'].value_counts().sort_index()

    # Resumo Moldura/Miolo
    props_df['moldura_miolo'] = props_df.apply(lambda row: f"{row['moldura']}M / {row['miolo']}C", axis=1)
    summaries['moldura_miolo'] = props_df['moldura_miolo'].value_counts().sort_index()

    return summaries