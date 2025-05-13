# src/analysis/number_properties_analysis.py
import pandas as pd
import numpy as np # Útil para algumas operações numéricas
from typing import List, Dict, Any # Adicionado Any
import logging # ADICIONADO

# Importar constantes necessárias de config.py
from src.config import ALL_NUMBERS # Pode ser usado para definir o universo de números primos

logger = logging.getLogger(__name__) # Logger específico para este módulo

# --- Funções Auxiliares para Propriedades Numéricas ---

def get_prime_numbers(limit: int) -> List[int]:
    """Retorna uma lista de números primos até um certo limite."""
    primes = []
    for num in range(2, limit + 1):
        is_p = True
        for i in range(2, int(num**0.5) + 1):
            if num % i == 0:
                is_p = False
                break
        if is_p:
            primes.append(num)
    return primes

# Obter os números primos uma vez, usando o maior número possível da Lotofácil (25)
PRIMES_UP_TO_25 = get_prime_numbers(max(ALL_NUMBERS) if ALL_NUMBERS else 25)
logger.debug(f"Números primos até 25 identificados: {PRIMES_UP_TO_25}")

def analyze_draw_properties(draw: List[int]) -> Dict[str, Any]:
    """
    Analisa as propriedades de um único sorteio (lista de 15 dezenas).

    Args:
        draw: Lista de dezenas sorteadas.

    Returns:
        Dicionário com as propriedades calculadas (ex: pares, ímpares, soma, etc.).
    """
    properties = {}
    draw_series = pd.Series(draw)

    properties['soma_dezenas'] = int(draw_series.sum())
    properties['pares'] = int(draw_series.apply(lambda x: x % 2 == 0).sum())
    properties['impares'] = int(draw_series.apply(lambda x: x % 2 != 0).sum())
    properties['primos'] = int(draw_series.apply(lambda x: x in PRIMES_UP_TO_25).sum())
    
    # Adicionar outras propriedades se desejar, como:
    # - Números da Moldura / Retrato
    # - Números por Linha / Coluna do volante
    # - Sequências (ex: 3 números seguidos)
    # - etc.

    return properties

def analyze_number_properties(all_data_df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula várias propriedades numéricas para cada concurso no DataFrame.

    Args:
        all_data_df: DataFrame com todos os concursos. 
                     Esperadas colunas 'Concurso' e 'bola_1'...'bola_15'.

    Returns:
        DataFrame com 'Concurso' e colunas para cada propriedade calculada.
        Retorna um DataFrame vazio em caso de erro.
    """
    logger.info("Iniciando análise de propriedades numéricas dos sorteios.")
    if all_data_df is None or all_data_df.empty:
        logger.warning("DataFrame de entrada para analyze_number_properties está vazio.")
        return pd.DataFrame()

    if 'Concurso' not in all_data_df.columns:
        logger.error("Coluna 'Concurso' não encontrada no DataFrame. Não é possível analisar propriedades.")
        return pd.DataFrame()

    dezena_cols = [f'bola_{i}' for i in range(1, 16)]
    actual_dezena_cols = [col for col in dezena_cols if col in all_data_df.columns]
    
    if len(actual_dezena_cols) < 15 : # Pode ser flexível se alguns jogos tiverem menos bolas, mas Lotofácil é sempre 15.
        logger.warning(f"Esperava 15 colunas de bolas, encontrou {len(actual_dezena_cols)}. Usando as existentes: {actual_dezena_cols}")
        if not actual_dezena_cols:
            logger.error("Nenhuma coluna de bola encontrada. Não é possível analisar propriedades.")
            return pd.DataFrame()

    results = []
    for index, row in all_data_df.iterrows():
        try:
            draw = [int(row[col]) for col in actual_dezena_cols if pd.notna(row[col])] # Garante que são números
            if len(draw) != 15: # Verifica se temos 15 dezenas válidas
                logger.debug(f"Sorteio do concurso {row['Concurso']} tem {len(draw)} dezenas válidas, esperado 15. Pulando.")
                continue

            properties = analyze_draw_properties(draw)
            properties['Concurso'] = int(row['Concurso']) # Adiciona o número do concurso
            results.append(properties)
        except Exception as e:
            logger.error(f"Erro ao processar propriedades do concurso {row.get('Concurso', 'Desconhecido')}: {e}", exc_info=False) # exc_info=False para não poluir muito com tracebacks individuais
            # logger.debug("Detalhe do erro:", exc_info=True) # Para depuração mais detalhada

    if not results:
        logger.warning("Nenhuma propriedade de concurso pôde ser calculada.")
        return pd.DataFrame()

    properties_df = pd.DataFrame(results)
    
    # Reordenar colunas para ter 'Concurso' primeiro
    if 'Concurso' in properties_df.columns:
        cols = ['Concurso'] + [col for col in properties_df.columns if col != 'Concurso']
        properties_df = properties_df[cols]
    
    logger.info(f"Análise de propriedades numéricas concluída para {len(properties_df)} concursos.")
    return properties_df