# src/analysis/recurrence_analysis.py
import pandas as pd
import numpy as np
import json
import logging
from typing import List, Dict, Any, Tuple

logger = logging.getLogger(__name__)

def get_gaps_for_all_numbers(
    draw_matrix: pd.DataFrame, 
    config: Any
) -> Dict[int, List[int]]:
    """
    Calcula a lista de gaps (intervalos entre ocorrências) para cada dezena.
    """
    logger.debug("Calculando gaps históricos para todas as dezenas.")
    if draw_matrix.empty:
        logger.warning("Matriz de sorteios para get_gaps_for_all_numbers está vazia.")
        return {dezena: [] for dezena in config.ALL_NUMBERS}

    all_gaps: Dict[int, List[int]] = {dezena: [] for dezena in config.ALL_NUMBERS}
    
    draw_matrix = draw_matrix.sort_index()

    for dezena in draw_matrix.columns:
        if dezena not in config.ALL_NUMBERS:
            continue
        
        occurrences = draw_matrix.index[draw_matrix[dezena] == 1].tolist()
        
        if len(occurrences) > 1:
            for i in range(len(occurrences) - 1):
                gap = occurrences[i+1] - occurrences[i] - 1
                all_gaps[dezena].append(gap)
    
    logger.debug("Cálculo de gaps históricos concluído.")
    return all_gaps

def calculate_recurrence_stats_for_number(
    gaps_list: List[int], 
    current_delay: int # Este é o valor numérico do atraso atual
) -> Tuple[float, int, float, int, float, int, str]:
    """
    Calcula estatísticas de recorrência para uma dezena com base em seus gaps e atraso atual.
    """
    total_gaps = len(gaps_list)
    gaps_series = pd.Series(gaps_list, dtype=float)

    if total_gaps == 0:
        return np.nan, 0, np.nan, np.nan, np.nan, np.nan, json.dumps([])

    # current_delay já é o valor numérico
    cdf_current_delay = gaps_series[gaps_series <= current_delay].count() / total_gaps if total_gaps > 0 else np.nan
    
    mean_gaps = gaps_series.mean()
    median_gaps = int(gaps_series.median()) if not gaps_series.empty else np.nan
    std_dev_gaps = gaps_series.std(ddof=0) if total_gaps > 1 else np.nan
    max_gap_observed = int(gaps_series.max()) if not gaps_series.empty else np.nan
    
    gaps_for_json = [int(g) for g in gaps_list] if gaps_list else []
    gaps_json = json.dumps(gaps_for_json)

    return (
        round(cdf_current_delay, 6) if pd.notna(cdf_current_delay) else np.nan,
        total_gaps,
        round(mean_gaps, 2) if pd.notna(mean_gaps) else np.nan,
        median_gaps if pd.notna(median_gaps) else np.nan,
        round(std_dev_gaps, 2) if pd.notna(std_dev_gaps) else np.nan,
        max_gap_observed if pd.notna(max_gap_observed) else np.nan,
        gaps_json
    )


def analyze_recurrence(
    draw_matrix: pd.DataFrame, 
    current_delays_df: pd.DataFrame, 
    config: Any
) -> pd.DataFrame:
    """
    Realiza a análise de recorrência para todas as dezenas.
    """
    logger.info("Iniciando análise de recorrência das dezenas.")
    if draw_matrix.empty:
        logger.warning("Matriz de sorteios para analyze_recurrence está vazia.")
        return pd.DataFrame()
    if current_delays_df.empty:
        logger.warning("DataFrame de atrasos atuais para analyze_recurrence está vazio.")
        return pd.DataFrame()

    all_historical_gaps = get_gaps_for_all_numbers(draw_matrix, config)
    
    recurrence_data = []

    # Nome da coluna como no DataFrame carregado do banco de dados
    actual_current_delay_col_name = "Atraso Atual" 

    if 'Dezena' not in current_delays_df.columns:
        logger.error("Coluna 'Dezena' não encontrada em current_delays_df.")
        return pd.DataFrame()
    if actual_current_delay_col_name not in current_delays_df.columns:
        # Log já usa a variável, que agora tem o nome correto
        logger.error(f"Coluna '{actual_current_delay_col_name}' não encontrada em current_delays_df.")
        return pd.DataFrame()

    for dezena in config.ALL_NUMBERS:
        gaps_list = all_historical_gaps.get(dezena, [])
        
        current_delay_series = current_delays_df[current_delays_df['Dezena'] == dezena][actual_current_delay_col_name]
        
        current_delay_val: int
        if current_delay_series.empty:
            logger.warning(f"Atraso atual não encontrado para a dezena {dezena} na tabela '{current_delays_df}'. Assumindo 0.")
            current_delay_val = 0 
        else:
            try:
                current_delay_val = int(current_delay_series.iloc[0])
            except ValueError:
                logger.warning(f"Não foi possível converter o atraso atual para int para a dezena {dezena}. Valor: {current_delay_series.iloc[0]}. Assumindo 0.")
                current_delay_val = 0


        (cdf_val, total_gaps, mean_g, med_g, std_g, max_g, gaps_j) = calculate_recurrence_stats_for_number(
            gaps_list, current_delay_val # Passa o valor numérico
        )
        
        recurrence_data.append({
            'Dezena': dezena,
            'Atraso_Atual': current_delay_val, # Usa o valor numérico
            'CDF_Atraso_Atual': cdf_val,
            'Total_Gaps_Observados': total_gaps,
            'Media_Gaps': mean_g,
            'Mediana_Gaps': med_g,
            'Std_Dev_Gaps': std_g,
            'Max_Gap_Observado': max_g,
            'Gaps_Observados': gaps_j
        })

    result_df = pd.DataFrame(recurrence_data)
    logger.info(f"Análise de recorrência concluída. {len(result_df)} dezenas processadas.")
    return result_df