# src/analysis/combination_analysis.py
import pandas as pd
from itertools import combinations as iter_combinations # Renomeado para evitar conflito de nome se houver uma variável combinations
from collections import Counter
import logging # ADICIONADO
from typing import List, Tuple, Any # Adicionado para type hints

# Importar constantes necessárias de config.py, se houver.
# Para esta análise, ALL_NUMBERS pode não ser estritamente necessário,
# mas é bom ter se quisermos garantir que apenas números válidos sejam considerados.
from src.config import ALL_NUMBERS

logger = logging.getLogger(__name__) # Logger específico para este módulo

def get_draws_from_dataframe(all_data_df: pd.DataFrame) -> List[List[int]]:
    """
    Extrai todos os sorteios (listas de dezenas) do DataFrame principal.
    """
    if all_data_df is None or all_data_df.empty:
        logger.warning("DataFrame de entrada para get_draws_from_dataframe está vazio.")
        return []

    dezena_cols = [f'bola_{i}' for i in range(1, 16)]
    actual_dezena_cols = [col for col in dezena_cols if col in all_data_df.columns]

    if len(actual_dezena_cols) < 15: # Lotofácil sempre tem 15 dezenas
        logger.warning(f"Esperava 15 colunas de bolas, encontrou {len(actual_dezena_cols)}. Usando as existentes: {actual_dezena_cols}")
        if not actual_dezena_cols:
            logger.error("Nenhuma coluna de bola encontrada em all_data_df para extrair sorteios.")
            return []
    
    draws = []
    for index, row in all_data_df.iterrows():
        try:
            # Converte para int e remove NaNs que podem ter vindo de dados brutos
            current_draw = [int(num) for num in row[actual_dezena_cols].dropna().values]
            if len(current_draw) == 15: # Validar se temos 15 dezenas válidas
                draws.append(sorted(current_draw)) # Ordena para consistência nas combinações
            else:
                logger.debug(f"Sorteio do concurso {row.get('Concurso', index)} não tem 15 dezenas válidas ({len(current_draw)} encontradas). Pulando.")
        except ValueError as ve:
            logger.warning(f"Erro ao converter dezenas para int no concurso {row.get('Concurso', index)}: {ve}. Pulando sorteio.")
        except Exception as e:
            logger.error(f"Erro inesperado ao processar sorteio do concurso {row.get('Concurso', index)}: {e}", exc_info=True)
            
    return draws


def calculate_pair_frequencies(all_data_df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula a frequência de todos os pares de dezenas que foram sorteados juntos.

    Args:
        all_data_df: DataFrame com todos os concursos.

    Returns:
        DataFrame com colunas 'Dezena1', 'Dezena2', 'Frequencia'.
        Retorna DataFrame vazio em caso de erro.
    """
    logger.info("Calculando frequência de pares de dezenas.")
    draws = get_draws_from_dataframe(all_data_df)

    if not draws:
        logger.warning("Nenhum sorteio válido encontrado para calcular frequência de pares.")
        return pd.DataFrame({'Dezena1': [], 'Dezena2': [], 'Frequencia': []})

    pair_counter: Counter = Counter()
    for draw in draws:
        # Gera todas as combinações de 2 dezenas para o sorteio atual
        # A lista 'draw' já está ordenada por get_draws_from_dataframe
        for pair in iter_combinations(draw, 2):
            pair_counter[pair] += 1
    
    if not pair_counter:
        logger.info("Nenhum par encontrado ou contado.")
        return pd.DataFrame({'Dezena1': [], 'Dezena2': [], 'Frequencia': []})

    # Converte o Counter para um DataFrame
    pairs_data = []
    for pair_tuple, freq in pair_counter.items():
        pairs_data.append({'Dezena1': pair_tuple[0], 'Dezena2': pair_tuple[1], 'Frequencia': freq})
    
    pairs_df = pd.DataFrame(pairs_data)
    
    if pairs_df.empty:
        logger.info("DataFrame de frequência de pares resultante está vazio.")
    else:
        logger.info(f"Frequência de pares calculada para {len(pairs_df)} pares distintos.")
        pairs_df = pairs_df.sort_values(by='Frequencia', ascending=False)
        
    return pairs_df


def calculate_trio_frequencies(all_data_df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula a frequência de todos os trios de dezenas que foram sorteados juntos.
    (Implementação similar a calculate_pair_frequencies)

    Args:
        all_data_df: DataFrame com todos os concursos.

    Returns:
        DataFrame com colunas 'Dezena1', 'Dezena2', 'Dezena3', 'Frequencia'.
        Retorna DataFrame vazio em caso de erro.
    """
    logger.info("Calculando frequência de trios de dezenas.")
    draws = get_draws_from_dataframe(all_data_df)

    if not draws:
        logger.warning("Nenhum sorteio válido encontrado para calcular frequência de trios.")
        return pd.DataFrame({'Dezena1': [], 'Dezena2': [], 'Dezena3': [], 'Frequencia': []})

    trio_counter: Counter = Counter()
    for draw in draws:
        # Gera todas as combinações de 3 dezenas para o sorteio atual
        for trio in iter_combinations(draw, 3):
            trio_counter[trio] += 1
            
    if not trio_counter:
        logger.info("Nenhum trio encontrado ou contado.")
        return pd.DataFrame({'Dezena1': [], 'Dezena2': [], 'Dezena3': [], 'Frequencia': []})

    trios_data = []
    for trio_tuple, freq in trio_counter.items():
        trios_data.append({
            'Dezena1': trio_tuple[0], 
            'Dezena2': trio_tuple[1], 
            'Dezena3': trio_tuple[2], 
            'Frequencia': freq
        })
        
    trios_df = pd.DataFrame(trios_data)

    if trios_df.empty:
        logger.info("DataFrame de frequência de trios resultante está vazio.")
    else:
        logger.info(f"Frequência de trios calculada para {len(trios_df)} trios distintos.")
        trios_df = trios_df.sort_values(by='Frequencia', ascending=False)
        
    return trios_df

# Você pode adicionar calculate_quadra_frequencies, etc., seguindo o mesmo padrão.