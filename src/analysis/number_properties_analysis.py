# src/analysis/number_properties_analysis.py
import pandas as pd
import numpy as np
from typing import List, Dict, Any
import logging

# Importa ALL_NUMBERS para definir PRIMES_UP_TO_25 globalmente
# Se preferir, PRIMES_UP_TO_25 pode ser um atributo de config também.
from src.config import ALL_NUMBERS as CONFIG_ALL_NUMBERS # Renomeado para evitar conflito se config for passado

logger = logging.getLogger(__name__)

_PRIMES_UP_TO_25_CACHE: List[int] = []

def get_prime_numbers(limit: int) -> List[int]:
    global _PRIMES_UP_TO_25_CACHE
    if limit == 25 and _PRIMES_UP_TO_25_CACHE:
        return _PRIMES_UP_TO_25_CACHE
    primes = []
    if limit < 2: return primes
    sieve = [True] * (limit + 1)
    for p in range(2, int(limit**0.5) + 1):
        if sieve[p]:
            for multiple in range(p*p, limit + 1, p): sieve[multiple] = False
    for p in range(2, limit + 1):
        if sieve[p]: primes.append(p)
    if limit == 25: _PRIMES_UP_TO_25_CACHE = primes
    return primes

limit_for_primes = max(CONFIG_ALL_NUMBERS) if CONFIG_ALL_NUMBERS else 25
PRIMES_UP_TO_25: List[int] = get_prime_numbers(limit_for_primes)
logger.debug(f"PRIMES_UP_TO_25 (módulo): {PRIMES_UP_TO_25}")

def analyze_draw_properties(draw: List[int], config_obj_param: Any) -> Dict[str, Any]: # Renomeado config para evitar conflito
    properties: Dict[str, Any] = {}
    if not draw or len(draw) != config_obj_param.NUMBERS_PER_DRAW:
        logger.warning(f"Sorteio inválido ou número incorreto de dezenas: {draw}")
        return {'soma_dezenas': 0, 'pares': 0, 'impares': 0, 'primos': 0}

    draw_series = pd.Series(draw)
    properties['soma_dezenas'] = int(draw_series.sum())
    properties['pares'] = int(draw_series.apply(lambda x: x % 2 == 0).sum())
    properties['impares'] = int(draw_series.apply(lambda x: x % 2 != 0).sum())
    properties['primos'] = int(draw_series.apply(lambda x: x in PRIMES_UP_TO_25).sum())
    return properties

def analyze_number_properties(all_data_df: pd.DataFrame, config: Any) -> pd.DataFrame: # Recebe config
    logger.info("Iniciando análise de propriedades numéricas dos sorteios.")
    if all_data_df is None or all_data_df.empty:
        logger.warning("DataFrame para analyze_number_properties está vazio.")
        return pd.DataFrame()

    contest_col = config.CONTEST_ID_COLUMN_NAME
    ball_cols = config.BALL_NUMBER_COLUMNS
    numbers_per_draw_val = config.NUMBERS_PER_DRAW

    if contest_col not in all_data_df.columns:
        logger.error(f"Coluna '{contest_col}' não encontrada. Colunas: {all_data_df.columns.tolist()}")
        return pd.DataFrame()

    actual_ball_cols = [col for col in ball_cols if col in all_data_df.columns]
    if len(actual_ball_cols) < numbers_per_draw_val :
        logger.warning(f"Esperava {numbers_per_draw_val} colunas de bolas, encontrou {len(actual_ball_cols)}. Usando: {actual_ball_cols}")
        if not actual_ball_cols:
            logger.error("Nenhuma coluna de bola encontrada."); return pd.DataFrame()

    results = []
    for index, row in all_data_df.iterrows():
        try:
            draw = [int(row[col]) for col in actual_ball_cols if pd.notna(row[col])]
            if len(draw) != numbers_per_draw_val:
                logger.debug(f"Sorteio {row[contest_col]} tem {len(draw)} dezenas, esperado {numbers_per_draw_val}. Pulando.")
                continue
            properties = analyze_draw_properties(draw, config) # Passa config
            properties_entry: Dict[str,Any] = {contest_col: int(row[contest_col])}
            properties_entry.update(properties)
            results.append(properties_entry)
        except Exception as e:
            logger.error(f"Erro processar props concurso {row.get(contest_col, 'UKN')}: {e}", exc_info=False)
    
    if not results: logger.warning("Nenhuma propriedade calculada."); return pd.DataFrame()
    properties_df = pd.DataFrame(results)
    
    final_col_order = [contest_col] + [col for col in properties_df.columns if col != contest_col]
    # Garante que apenas colunas existentes sejam selecionadas
    final_col_order = [col for col in final_col_order if col in properties_df.columns]
    properties_df = properties_df[final_col_order]
    
    # Renomeia a coluna de concurso para "Concurso" se a tabela final no DB espera esse nome
    properties_df.rename(columns={contest_col: "Concurso"}, inplace=True, errors='ignore')

    logger.info(f"Análise de propriedades numéricas concluída para {len(properties_df)} concursos.")
    return properties_df