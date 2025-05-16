# src/analysis/frequent_itemset_metrics_analysis.py
import pandas as pd
import numpy as np
import json 
from typing import List, Dict, Any, Set

try:
    # Importar CONTEST_ID_COLUMN_NAME do config para consistência
    from ..config import Config, CONTEST_ID_COLUMN_NAME 
except ImportError:
    from src.config import Config, CONTEST_ID_COLUMN_NAME

import logging
logger = logging.getLogger(__name__)

def parse_itemset_str(itemset_str: str) -> Set[int]:
    """Converte uma string de itemset (ex: '01-05-12') para um conjunto de inteiros."""
    if not itemset_str or not isinstance(itemset_str, str):
        logger.warning(f"Tentativa de parsear itemset_str inválido: {itemset_str}")
        return set()
    try:
        return set(map(int, itemset_str.split('-')))
    except ValueError:
        logger.warning(f"Erro ao converter partes de itemset_str para int: {itemset_str}")
        return set()

def calculate_frequent_itemset_delay_metrics(
    all_draws_df: pd.DataFrame, 
    frequent_itemsets_df: pd.DataFrame, 
    latest_contest_id: int, # <<< PARÂMETRO RENOMEADO
    config: Config # Tipagem mais específica para config
) -> pd.DataFrame:
    """
    Calcula métricas de atraso e outras informações para cada itemset frequente.

    Args:
        all_draws_df (pd.DataFrame): DataFrame com todos os sorteios. 
                                     Deve conter CONTEST_ID_COLUMN_NAME (ex: 'contest_id') e 'numbers_drawn_set'.
        frequent_itemsets_df (pd.DataFrame): DataFrame da tabela 'frequent_itemsets'.
                                             Deve conter 'itemset_str', 'length', 'support', 'frequency_count'.
        latest_contest_id (int): O ID do concurso mais recente no histórico. # <<< ATUALIZADO
        config (Config): Objeto de configuração.

    Returns:
        pd.DataFrame: DataFrame com métricas de atraso para cada itemset.
    """
    logger.info(f"Iniciando cálculo de métricas de atraso para {len(frequent_itemsets_df)} itemsets frequentes, usando '{CONTEST_ID_COLUMN_NAME}' como ID.")
    
    if CONTEST_ID_COLUMN_NAME not in all_draws_df.columns:
        msg = f"DataFrame all_draws_df deve conter a coluna de ID do concurso: '{CONTEST_ID_COLUMN_NAME}'."
        logger.error(msg)
        raise ValueError(msg)

    # Garantir que 'numbers_drawn_set' exista
    # (seu main.py e data_loader já criam 'drawn_numbers' que é uma lista, precisamos do set)
    if 'numbers_drawn_set' not in all_draws_df.columns:
        if 'drawn_numbers' in all_draws_df.columns:
            logger.info("Coluna 'numbers_drawn_set' não encontrada em all_draws_df, criando a partir de 'drawn_numbers'.")
            try:
                # Garante que os elementos da lista são convertidos para set
                all_draws_df['numbers_drawn_set'] = all_draws_df['drawn_numbers'].apply(lambda x: set(x) if isinstance(x, list) else set())
            except Exception as e_set_conversion:
                logger.error(f"Erro ao converter 'drawn_numbers' para 'numbers_drawn_set': {e_set_conversion}", exc_info=True)
                raise ValueError("Falha ao preparar a coluna 'numbers_drawn_set'.")
        else:
            msg = "DataFrame all_draws_df deve conter 'numbers_drawn_set' (Set[int]) ou 'drawn_numbers' (List[int])."
            logger.error(msg)
            raise ValueError(msg)


    if frequent_itemsets_df.empty:
        logger.warning("DataFrame de itemsets frequentes está vazio. Retornando DataFrame vazio.")
        cols = ['itemset_str', 'length', 'support', 'frequency_count', 
                'last_occurrence_contest_id', 'current_delay', 
                'mean_delay', 'max_delay', 'std_dev_delay', 'occurrences_draw_ids']
        return pd.DataFrame(columns=cols)

    processed_metrics = []

    # Certifique-se de que a coluna de ID do concurso em all_draws_df é do tipo int para comparações
    all_draws_df[CONTEST_ID_COLUMN_NAME] = all_draws_df[CONTEST_ID_COLUMN_NAME].astype(int)


    for _, row in frequent_itemsets_df.iterrows():
        itemset_str = row['itemset_str']
        itemset_set = parse_itemset_str(itemset_str)

        if not itemset_set:
            continue

        occurrences_mask = all_draws_df['numbers_drawn_set'].apply(lambda s: itemset_set.issubset(s))
        occurrence_draws_df = all_draws_df[occurrences_mask]
        
        occurrence_contest_ids = sorted(list(occurrence_draws_df[CONTEST_ID_COLUMN_NAME].unique()))

        # Inicialização das métricas
        last_occurrence_contest_id_val = None
        current_delay_val = None
        mean_delay_val = np.nan
        max_delay_val = np.nan
        std_dev_delay_val = np.nan
        occurrences_json = json.dumps([])

        if not occurrence_contest_ids:
            # Se o itemset nunca ocorreu, o atraso atual é desde o "início" até o último concurso.
            # O primeiro concurso no dataset pode ser usado como referência se necessário, ou latest_contest_id.
            # Por simplicidade, se não houver ocorrências, current_delay é a "idade" total dos dados.
            if not all_draws_df.empty:
                 first_contest_id_in_data = all_draws_df[CONTEST_ID_COLUMN_NAME].min()
                 current_delay_val = latest_contest_id - first_contest_id_in_data + 1 # Ou apenas latest_contest_id se o start for 0
                 max_delay_val = current_delay_val # O atraso máximo é a própria duração se nunca ocorreu
            else: # Caso extremo de all_draws_df ser vazio, mas já foi checado antes
                current_delay_val = latest_contest_id 
                max_delay_val = latest_contest_id

        else:
            last_occurrence_contest_id_val = int(occurrence_contest_ids[-1])
            current_delay_val = latest_contest_id - last_occurrence_contest_id_val
            
            gaps = []
            if len(occurrence_contest_ids) > 1:
                # Gap inicial (desde o primeiro concurso geral até a primeira ocorrência do itemset)
                # Se quiser incluir o gap desde o início do dataset até a primeira ocorrência do itemset:
                # if not all_draws_df.empty:
                #     first_contest_id_in_data = all_draws_df[CONTEST_ID_COLUMN_NAME].min()
                #     gaps.append(occurrence_contest_ids[0] - first_contest_id_in_data)
                
                for i in range(len(occurrence_contest_ids) - 1):
                    gaps.append(occurrence_contest_ids[i+1] - occurrence_contest_ids[i] - 1)
            
            # Gap final (desde a última ocorrência do itemset até o último concurso geral)
            # Se quiser incluir o gap desde a última ocorrência até o final do dataset:
            # gaps.append(latest_contest_id - occurrence_contest_ids[-1])


            if gaps: 
                mean_delay_val = np.mean(gaps)
                max_delay_val = np.max(gaps) 
                std_dev_delay_val = pd.Series(gaps).std(ddof=1) 
            elif len(occurrence_contest_ids) == 1: # Apenas uma ocorrência
                # O que fazer com média, max, std de gaps?
                # Atraso atual já foi calculado.
                # Gaps entre ocorrências não existem.
                mean_delay_val = np.nan
                max_delay_val = np.nan # Ou 0 se considerarmos que não houve "intervalo"
                std_dev_delay_val = np.nan
            
            occurrences_json = json.dumps([int(cid) for cid in occurrence_contest_ids]) # Garante que são ints para JSON

        processed_metrics.append({
            'itemset_str': itemset_str,
            'length': int(row['length']),
            'support': float(row['support']),
            'frequency_count': int(row['frequency_count']),
            'last_occurrence_contest_id': last_occurrence_contest_id_val,
            'current_delay': current_delay_val,
            'mean_delay': mean_delay_val,
            'max_delay': max_delay_val,
            'std_dev_delay': std_dev_delay_val,
            'occurrences_draw_ids': occurrences_json
        })
        
        if len(processed_metrics) % 10000 == 0: # Log de progresso menos frequente
             logger.info(f"Processadas métricas de atraso para {len(processed_metrics)}/{len(frequent_itemsets_df)} itemsets...")

    logger.info(f"Cálculo de métricas de atraso para itemsets frequentes concluído. {len(processed_metrics)} itemsets processados.")
    
    final_cols = ['itemset_str', 'length', 'support', 'frequency_count', 
                  'last_occurrence_contest_id', 'current_delay', 
                  'mean_delay', 'max_delay', 'std_dev_delay', 'occurrences_draw_ids']
    
    result_df = pd.DataFrame(processed_metrics, columns=final_cols)
    
    return result_df