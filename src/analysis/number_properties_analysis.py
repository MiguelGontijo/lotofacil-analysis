# src/analysis/number_properties_analysis.py

import pandas as pd
from typing import Optional, Dict, Set, List # Adicionado List

# Importações locais
from src.database_manager import read_data_from_db
from src.config import logger, NEW_BALL_COLUMNS

# --- Definições das Propriedades ---
ALL_NUMBERS_SET: Set[int] = set(range(1, 26))
PRIMES_SET: Set[int] = {2, 3, 5, 7, 11, 13, 17, 19, 23}
FRAME_SET: Set[int] = {1, 2, 3, 4, 5, 6, 10, 11, 15, 16, 20, 21, 22, 23, 24, 25}
CENTER_SET: Set[int] = ALL_NUMBERS_SET - FRAME_SET

BASE_COLS: List[str] = ['concurso'] + NEW_BALL_COLUMNS

def analyze_number_properties(concurso_maximo: Optional[int] = None) -> Optional[pd.DataFrame]:
    """
    Analisa propriedades das dezenas sorteadas em cada concurso.
    RETORNA: DataFrame com contagens por concurso ou None.
    """
    logger.info(f"Analisando propriedades dos números até {concurso_maximo or 'último'}...")
    df = read_data_from_db(columns=BASE_COLS, concurso_maximo=concurso_maximo)
    if df is None or df.empty:
        logger.warning("Nenhum dado para analisar propriedades.")
        return None # Retorna None em caso de falha na leitura
    if not all(col in df.columns for col in NEW_BALL_COLUMNS):
        logger.error("Dados lidos não contêm colunas de bolas esperadas.")
        return None

    results = []
    for index, row in df.iterrows():
        concurso_val = row['concurso']
        if pd.isna(concurso_val): continue
        concurso = int(concurso_val)

        drawn_numbers = {int(num) for num in row[NEW_BALL_COLUMNS].dropna().values}
        if len(drawn_numbers) != 15:
             logger.warning(f"Concurso {concurso} não tem 15 dezenas válidas. Ignorando.")
             continue

        even_count = sum(1 for n in drawn_numbers if n % 2 == 0)
        prime_count = sum(1 for n in drawn_numbers if n in PRIMES_SET)
        frame_count = sum(1 for n in drawn_numbers if n in FRAME_SET)

        results.append({
            'concurso': concurso,
            'pares': even_count,
            'impares': 15 - even_count,
            'primos': prime_count,
            'moldura': frame_count,
            'miolo': 15 - frame_count
        })

    if not results:
        logger.warning("Nenhum resultado gerado na análise de propriedades.")
        # Retorna DataFrame vazio se nenhum concurso válido foi processado
        return pd.DataFrame(columns=['concurso', 'pares', 'impares', 'primos', 'moldura', 'miolo'])

    logger.info("Análise de propriedades por concurso concluída.")
    return pd.DataFrame(results) # <<< RETORNA O DATAFRAME


def summarize_properties(props_df: pd.DataFrame) -> Dict[str, pd.Series]:
    """
    Calcula estatísticas resumidas (frequência de cada distribuição).
    RETORNA: Dicionário com Series de value_counts.
    """
    summaries: Dict[str, pd.Series] = {}
    if props_df is None or props_df.empty:
        logger.warning("DataFrame de propriedades vazio para sumarização.")
        return summaries

    logger.debug("Sumarizando propriedades...")
    props_df_copy = props_df.copy() # Evitar SettingWithCopyWarning

    try:
        # Resumo Pares/Ímpares
        props_df_copy['par_impar_key'] = props_df_copy.apply(lambda row: f"{row['impares']}I / {row['pares']}P", axis=1)
        summaries['par_impar'] = props_df_copy['par_impar_key'].value_counts().sort_index()

        # Resumo Primos
        summaries['primos'] = props_df_copy['primos'].value_counts().sort_index()

        # Resumo Moldura/Miolo
        props_df_copy['moldura_miolo_key'] = props_df_copy.apply(lambda row: f"{row['moldura']}M / {row['miolo']}C", axis=1)
        summaries['moldura_miolo'] = props_df_copy['moldura_miolo_key'].value_counts().sort_index()
    except KeyError as e:
        logger.error(f"Erro ao acessar coluna esperada durante sumarização: {e}. DataFrame:\n{props_df_copy.head()}")
        return {} # Retorna vazio em caso de erro

    logger.debug("Sumarização de propriedades concluída.")
    return summaries # <<< RETORNA O DICIONÁRIO