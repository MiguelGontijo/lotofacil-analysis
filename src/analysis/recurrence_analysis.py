# src/analysis/recurrence_analysis.py
import pandas as pd
import numpy as np
import json
import logging
from typing import List, Dict, Any, Tuple, Optional

logger = logging.getLogger(__name__)

def get_gaps_for_all_numbers(
    draw_matrix: pd.DataFrame,
    config: Any 
) -> Dict[int, List[int]]:
    logger.debug("Interno: Calculando gaps históricos para todas as dezenas.") # Mantido DEBUG
    if not hasattr(config, 'ALL_NUMBERS'):
        logger.error("Atributo 'ALL_NUMBERS' não encontrado no config para get_gaps_for_all_numbers.")
        return {i: [] for i in range(1, 26)} 

    if draw_matrix.empty:
        logger.warning("Matriz de sorteios para get_gaps_for_all_numbers está vazia.")
        return {dezena: [] for dezena in config.ALL_NUMBERS}

    all_gaps: Dict[int, List[int]] = {dezena: [] for dezena in config.ALL_NUMBERS}
    draw_matrix = draw_matrix.sort_index()

    for dezena_col in draw_matrix.columns:
        try:
            dezena = int(dezena_col)
            if dezena not in config.ALL_NUMBERS: continue
        except ValueError:
            logger.warning(f"Coluna não numérica '{dezena_col}' na draw_matrix. Pulando.")
            continue
        
        occurrences = draw_matrix.index[draw_matrix.loc[:, dezena] == 1].tolist()
        
        if len(occurrences) > 1:
            for i in range(len(occurrences) - 1):
                gap = occurrences[i+1] - occurrences[i] - 1
                all_gaps[dezena].append(gap)
            
    logger.debug("Interno: Cálculo de gaps históricos concluído.") # Mantido DEBUG
    return all_gaps

def calculate_recurrence_stats_for_number(
    gaps_list: List[int],
    current_delay: int 
) -> Tuple[Optional[float], int, Optional[float], Optional[int], Optional[float], Optional[int], str]:
    total_gaps = len(gaps_list)
    gaps_series = pd.Series(gaps_list, dtype=float)

    cdf_current_delay: Optional[float] = None
    mean_gaps: Optional[float] = None
    median_gaps_int: Optional[int] = None
    std_dev_gaps: Optional[float] = None
    max_gap_observed_int: Optional[int] = None

    if total_gaps > 0:
        count_le_delay = gaps_series[gaps_series <= current_delay].count()
        cdf_current_delay = round(count_le_delay / total_gaps, 6) if total_gaps > 0 else None
        mean_gaps = round(gaps_series.mean(), 2) if not gaps_series.empty else None
        median_val = gaps_series.median()
        median_gaps_int = int(median_val) if pd.notna(median_val) else None
        std_dev_gaps = round(gaps_series.std(ddof=0), 2) if total_gaps > 1 else (0.0 if total_gaps == 1 else None)
        max_gap_val = gaps_series.max()
        max_gap_observed_int = int(max_gap_val) if pd.notna(max_gap_val) else None
    
    gaps_for_json = [int(g) for g in gaps_list]
    gaps_json_str = json.dumps(gaps_for_json)

    return (
        cdf_current_delay, total_gaps, mean_gaps, median_gaps_int,
        std_dev_gaps, max_gap_observed_int, gaps_json_str
    )

def analyze_recurrence(
    draw_matrix: pd.DataFrame,
    current_delays_df: pd.DataFrame, 
    config: Any 
) -> pd.DataFrame:
    # logger.info("Iniciando análise de recorrência das dezenas.") # MUDADO PARA DEBUG
    logger.debug("Interno: Iniciando analyze_recurrence.")
    
    # ... (validações como antes) ...
    if not hasattr(config, 'ALL_NUMBERS'): # etc.
        # ...
        return pd.DataFrame() # ou com estrutura default
    if draw_matrix.empty: # etc.
        # ...
        # Exemplo de retorno com estrutura para consistência
        empty_cols = ['Atraso_Atual_Input', 'CDF_Atraso_Atual', 'Total_Gaps_Observados', 'Media_Gaps', 'Mediana_Gaps', 'Std_Dev_Gaps', 'Max_Gap_Observado', 'Gaps_Observados_json']
        empty_data = {col: (0 if 'Input' in col or 'Total' in col else (json.dumps([]) if 'json' in col else pd.NA)) for col in empty_cols}
        empty_data[config.DEZENA_COLUMN_NAME] = config.ALL_NUMBERS
        return pd.DataFrame(empty_data)


    all_historical_gaps = get_gaps_for_all_numbers(draw_matrix, config)
    recurrence_data = []
    dezena_col_input = config.DEZENA_COLUMN_NAME # Usar constante
    current_delay_col_input = 'current_delay' # Nome da coluna no current_delays_df

    if dezena_col_input not in current_delays_df.columns or current_delay_col_input not in current_delays_df.columns:
        logger.error(f"Colunas '{dezena_col_input}' ou '{current_delay_col_input}' não encontradas em current_delays_df.")
        return pd.DataFrame() # Retornar DataFrame vazio ou com estrutura default

    for dezena_val_loop in config.ALL_NUMBERS:
        gaps_list = all_historical_gaps.get(dezena_val_loop, [])
        
        current_delay_series = current_delays_df[current_delays_df[dezena_col_input] == dezena_val_loop][current_delay_col_input]
        
        current_delay_as_int: int
        if current_delay_series.empty or pd.isna(current_delay_series.iloc[0]):
             current_delay_as_int = len(draw_matrix) 
        else:
            try:
                current_delay_as_int = int(current_delay_series.iloc[0])
            except ValueError:
                logger.warning(f"Não convertível para int: atraso atual para dezena {dezena_val_loop}. Usando atraso máximo.")
                current_delay_as_int = len(draw_matrix)

        (cdf_val, total_gaps, mean_g, med_g, std_g, max_g, gaps_j_str) = calculate_recurrence_stats_for_number(
            gaps_list, current_delay_as_int
        )
        
        recurrence_data.append({
            config.DEZENA_COLUMN_NAME: dezena_val_loop,
            'Atraso_Atual_Input': current_delay_as_int, # Mantido para possível uso futuro, mas não vai para a tabela final
            'CDF_Atraso_Atual': cdf_val, 
            'Total_Gaps_Observados': total_gaps,
            'Media_Gaps': mean_g,
            'Mediana_Gaps': med_g,
            'Std_Dev_Gaps': std_g,
            'Max_Gap_Observado': max_g,
            'Gaps_Observados_json': gaps_j_str
        })

    result_df = pd.DataFrame(recurrence_data)
    # logger.info(f"Análise de recorrência concluída. {len(result_df)} dezenas processadas.") # MUDADO PARA DEBUG
    logger.debug(f"Interno: Análise de recorrência concluída. {len(result_df)} dezenas processadas.")
    return result_df