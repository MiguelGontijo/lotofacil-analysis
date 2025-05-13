# src/analysis/frequency_analysis.py
import pandas as pd
from typing import List, Dict, Any # Adicionado Any
import logging # ADICIONADO

# Importar APENAS as constantes necessárias e existentes de config.py
from src.config import ALL_NUMBERS
# logger e NEW_BALL_COLUMNS REMOVIDOS da importação de config

logger = logging.getLogger(__name__) # Logger específico para este módulo

def calculate_frequency(all_data_df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula a frequência absoluta de cada dezena em todos os concursos.

    Args:
        all_data_df: DataFrame com todos os concursos, contendo colunas 'bola_1' a 'bola_15'.
                     Estas colunas são esperadas após o processamento em data_loader.py.

    Returns:
        DataFrame com as colunas 'Dezena' e 'Frequencia Absoluta'.
        Retorna um DataFrame vazio em caso de erro ou dados insuficientes.
    """
    if all_data_df is None or all_data_df.empty:
        logger.warning("DataFrame de entrada para calculate_frequency está vazio.")
        return pd.DataFrame({'Dezena': [], 'Frequencia Absoluta': []})

    try:
        # As colunas de bolas que data_loader.py produz são 'bola_1', ..., 'bola_15'
        # Não precisamos de NEW_BALL_COLUMNS ou BALL_NUMBER_COLUMNS de config.py aqui,
        # pois já sabemos os nomes esperados das colunas de bolas no DataFrame processado.
        dezena_cols = [f'bola_{i}' for i in range(1, 16)] 
        
        actual_dezena_cols = [col for col in dezena_cols if col in all_data_df.columns]
        if len(actual_dezena_cols) == 0: # Nenhuma coluna de bola encontrada
             logger.error("Nenhuma coluna de bola (bola_1 a bola_15) encontrada em all_data_df para calcular frequência.")
             return pd.DataFrame({'Dezena': [], 'Frequencia Absoluta': []})
        if len(actual_dezena_cols) < 15: # Menos de 15 colunas de bolas
            logger.warning(f"Nem todas as 15 colunas de bolas foram encontradas em all_data_df. Usando: {actual_dezena_cols}")

        all_drawn_numbers = all_data_df[actual_dezena_cols].values.flatten()
        all_drawn_numbers = all_drawn_numbers[~pd.isna(all_drawn_numbers)] # Remove NaNs

        if len(all_drawn_numbers) == 0:
            logger.warning("Nenhum número sorteado encontrado após o flatten e remoção de NaNs. Não é possível calcular frequência.")
            return pd.DataFrame({'Dezena': [], 'Frequencia Absoluta': []})

        frequency_series = pd.Series(all_drawn_numbers).value_counts()
        
        frequency_df = frequency_series.reindex(ALL_NUMBERS, fill_value=0).reset_index()
        frequency_df.columns = ['Dezena', 'Frequencia Absoluta']
        frequency_df['Dezena'] = frequency_df['Dezena'].astype(int)
        frequency_df['Frequencia Absoluta'] = frequency_df['Frequencia Absoluta'].astype(int)
        
        logger.info(f"Frequência absoluta calculada para {len(frequency_df)} dezenas.")
        return frequency_df.sort_values(by='Frequencia Absoluta', ascending=False)

    except Exception as e:
        logger.error(f"Erro ao calcular a frequência absoluta: {e}", exc_info=True)
        return pd.DataFrame({'Dezena': [], 'Frequencia Absoluta': []})


def calculate_relative_frequency(absolute_freq_df: pd.DataFrame, total_contests: int) -> pd.DataFrame:
    """
    Calcula a frequência relativa de cada dezena.

    Args:
        absolute_freq_df: DataFrame da função calculate_frequency (colunas 'Dezena', 'Frequencia Absoluta').
        total_contests: Número total de concursos para calcular a porcentagem.

    Returns:
        DataFrame com as colunas 'Dezena' e 'Frequencia Relativa'.
        Retorna um DataFrame vazio em caso de erro ou dados insuficientes.
    """
    if absolute_freq_df is None or absolute_freq_df.empty:
        logger.warning("DataFrame de frequência absoluta de entrada para calculate_relative_frequency está vazio.")
        return pd.DataFrame({'Dezena': [], 'Frequencia Relativa': []})
    
    if total_contests <= 0:
        logger.error(f"Número total de concursos inválido ({total_contests}) para calcular frequência relativa.")
        return pd.DataFrame({'Dezena': [], 'Frequencia Relativa': []})

    try:
        # Certifique-se de que as colunas esperadas existem
        if 'Dezena' not in absolute_freq_df.columns or 'Frequencia Absoluta' not in absolute_freq_df.columns:
            logger.error("DataFrame de frequência absoluta não contém as colunas 'Dezena' ou 'Frequencia Absoluta'.")
            return pd.DataFrame({'Dezena': [], 'Frequencia Relativa': []})
            
        relative_freq_df = absolute_freq_df.copy()
        relative_freq_df['Frequencia Relativa'] = (relative_freq_df['Frequencia Absoluta'] / total_contests)
        
        logger.info(f"Frequência relativa calculada para {len(relative_freq_df)} dezenas.")
        return relative_freq_df[['Dezena', 'Frequencia Relativa']].sort_values(by='Frequencia Relativa', ascending=False)
        
    except Exception as e:
        logger.error(f"Erro ao calcular a frequência relativa: {e}", exc_info=True)
        return pd.DataFrame({'Dezena': [], 'Frequencia Relativa': []})