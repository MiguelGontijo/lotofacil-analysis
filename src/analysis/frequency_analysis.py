# Lotofacil_Analysis/src/analysis/frequency_analysis.py
import pandas as pd
from typing import List, Dict, Any 
import logging

logger = logging.getLogger(__name__)

def calculate_frequency(all_data_df: pd.DataFrame, config: Any) -> pd.DataFrame:
    """
    Calcula a frequência absoluta de cada dezena em todos os concursos.
    """
    step_name_interno = "calculate_frequency (analysis)"
    logger.info(f"Interno: Iniciando {step_name_interno}.")

    if all_data_df is None or all_data_df.empty:
        logger.warning(f"DataFrame de entrada para {step_name_interno} está vazio.")
        return pd.DataFrame({'Dezena': [], 'Frequencia Absoluta': []})

    try:
        if not hasattr(config, 'BALL_NUMBER_COLUMNS') or not config.BALL_NUMBER_COLUMNS:
            logger.error(f"config.BALL_NUMBER_COLUMNS não definido ou vazio em {step_name_interno}.")
            return pd.DataFrame({'Dezena': [], 'Frequencia Absoluta': []})
        if not hasattr(config, 'ALL_NUMBERS') or not config.ALL_NUMBERS:
            logger.error(f"config.ALL_NUMBERS não definido ou vazio em {step_name_interno}.")
            return pd.DataFrame({'Dezena': [], 'Frequencia Absoluta': []})

        dezena_cols = config.BALL_NUMBER_COLUMNS
        actual_dezena_cols = [col for col in dezena_cols if col in all_data_df.columns]

        if not actual_dezena_cols:
             logger.error(f"Nenhuma coluna de bola ({dezena_cols}) encontrada em all_data_df para {step_name_interno}. Colunas disponíveis: {all_data_df.columns.tolist()}")
             return pd.DataFrame({'Dezena': [], 'Frequencia Absoluta': []})
        
        # Coleta todos os números de todas as colunas de bolas relevantes
        all_numeric_balls = []
        for col in actual_dezena_cols:
            # Converte para numérico, forçando erros para NaN, depois remove NaNs e converte para int
            numeric_col = pd.to_numeric(all_data_df[col], errors='coerce')
            all_numeric_balls.extend(numeric_col.dropna().astype(int).tolist())

        if not all_numeric_balls:
            logger.warning(f"Nenhum número sorteado válido encontrado em {step_name_interno} após processar colunas de bolas.")
            return pd.DataFrame({'Dezena': [], 'Frequencia Absoluta': []})

        frequency_series = pd.Series(all_numeric_balls).value_counts()
        
        frequency_df = frequency_series.reindex(config.ALL_NUMBERS, fill_value=0).reset_index()
        frequency_df.columns = ['Dezena', 'Frequencia Absoluta'] # Nomes de coluna fixos, como no seu original
        frequency_df['Dezena'] = frequency_df['Dezena'].astype(int)
        frequency_df['Frequencia Absoluta'] = frequency_df['Frequencia Absoluta'].astype(int)
        
        logger.info(f"Frequência absoluta calculada para {len(frequency_df)} dezenas em {step_name_interno}.")
        return frequency_df.sort_values(by='Frequencia Absoluta', ascending=False)

    except AttributeError as ae: 
        logger.error(f"Erro de atributo em {step_name_interno} (verifique 'config'): {ae}", exc_info=True)
        return pd.DataFrame({'Dezena': [], 'Frequencia Absoluta': []})
    except Exception as e:
        logger.error(f"Erro ao calcular a frequência absoluta em {step_name_interno}: {e}", exc_info=True)
        return pd.DataFrame({'Dezena': [], 'Frequencia Absoluta': []})

def calculate_relative_frequency(absolute_freq_df: pd.DataFrame, total_contests: int, config: Any) -> pd.DataFrame:
    step_name_interno = "calculate_relative_frequency (analysis)"
    logger.info(f"Interno: Iniciando {step_name_interno}.")
    if absolute_freq_df is None or absolute_freq_df.empty:
        logger.warning(f"DataFrame de frequência absoluta para {step_name_interno} está vazio.")
        return pd.DataFrame({'Dezena': [], 'Frequencia Relativa': []})
    
    if total_contests <= 0:
        logger.error(f"Número total de concursos inválido ({total_contests}) para {step_name_interno}.")
        return pd.DataFrame({'Dezena': [], 'Frequencia Relativa': []})

    try:
        if 'Dezena' not in absolute_freq_df.columns or 'Frequencia Absoluta' not in absolute_freq_df.columns:
            logger.error(f"Colunas 'Dezena' ou 'Frequencia Absoluta' ausentes em {step_name_interno}.")
            return pd.DataFrame({'Dezena': [], 'Frequencia Relativa': []})
            
        relative_freq_df = absolute_freq_df.copy()
        relative_freq_df['Frequencia Relativa'] = (relative_freq_df['Frequencia Absoluta'] / total_contests)
        
        logger.info(f"Frequência relativa calculada para {len(relative_freq_df)} dezenas em {step_name_interno}.")
        return relative_freq_df[['Dezena', 'Frequencia Relativa']].sort_values(by='Frequencia Relativa', ascending=False)
        
    except Exception as e:
        logger.error(f"Erro ao calcular a frequência relativa em {step_name_interno}: {e}", exc_info=True)
        return pd.DataFrame({'Dezena': [], 'Frequencia Relativa': []})