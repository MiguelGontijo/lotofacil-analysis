# src/analysis/frequency_analysis.py
import pandas as pd
import logging
from typing import Dict, Optional, Any # Adicionado Any

logger = logging.getLogger(__name__)

def calculate_frequency(
    all_data_df: pd.DataFrame, 
    config: Any, # Espera config_obj
    # target_contest_id: Optional[int] = None # Removido, pois o df já vem filtrado
    # run_all: bool = True # Removido, pois o df já vem filtrado
    specific_numbers: Optional[list[int]] = None
) -> Optional[pd.DataFrame]:
    """
    Calcula a frequência absoluta das dezenas.
    O DataFrame de entrada 'all_data_df' já deve estar filtrado até o concurso desejado.
    """
    # logger.info("Interno: Iniciando calculate_frequency (analysis).") # MUDADO PARA DEBUG ou REMOVIDO
    logger.debug("Interno: Iniciando calculate_frequency (analysis).")

    if all_data_df.empty:
        logger.warning("DataFrame vazio fornecido para calculate_frequency.")
        # Retorna um DataFrame com a estrutura esperada, mas com frequência zero
        dezenas_para_retorno = specific_numbers if specific_numbers else config.ALL_NUMBERS
        return pd.DataFrame({'Dezena': dezenas_para_retorno, 'Frequencia Absoluta': 0})

    # Usa as colunas de bolas definidas no config
    ball_columns_to_use = [col for col in config.BALL_NUMBER_COLUMNS if col in all_data_df.columns]
    if not ball_columns_to_use:
        logger.error("Nenhuma coluna de bola encontrada no DataFrame para calculate_frequency.")
        return None # Ou DataFrame com zeros

    # Concatena todas as colunas de bolas em uma única série
    all_drawn_numbers = pd.concat([all_data_df[col] for col in ball_columns_to_use], ignore_index=True)
    all_drawn_numbers.dropna(inplace=True) # Remove NaNs que podem surgir de colunas de bolas parcialmente preenchidas
    
    try:
        # Tenta converter para inteiro, tratando erros
        all_drawn_numbers = pd.to_numeric(all_drawn_numbers, errors='coerce')
        all_drawn_numbers.dropna(inplace=True) # Remove NaNs após conversão
        all_drawn_numbers = all_drawn_numbers.astype(int)
    except Exception as e:
        logger.error(f"Erro ao converter dezenas para inteiro em calculate_frequency: {e}")
        return None

    # Calcula a contagem de frequência
    frequency_counts = all_drawn_numbers.value_counts().sort_index()
    
    # Prepara o DataFrame de resultado
    dezenas_para_analise = specific_numbers if specific_numbers else config.ALL_NUMBERS
    df_frequency = pd.DataFrame({
        'Dezena': dezenas_para_analise # Usa a constante DEZENA_COLUMN_NAME do config
    })
    
    # Mapeia as frequências calculadas, preenchendo com 0 para dezenas não sorteadas
    df_frequency['Frequencia Absoluta'] = df_frequency['Dezena'].map(frequency_counts).fillna(0).astype(int)
    
    # logger.info(f"Frequência absoluta calculada para {len(df_frequency)} dezenas em calculate_frequency (analysis).") # MUDADO PARA DEBUG ou REMOVIDO
    logger.debug(f"Frequência absoluta calculada para {len(df_frequency)} dezenas em calculate_frequency (analysis).")
    return df_frequency


def calculate_relative_frequency(
    df_frequency_abs: pd.DataFrame, 
    total_draws: int, 
    config: Any # Espera config_obj
) -> Optional[pd.DataFrame]:
    """
    Calcula a frequência relativa das dezenas.
    Espera um DataFrame com colunas 'Dezena' e 'Frequencia Absoluta'.
    """
    # logger.info("Interno: Iniciando calculate_relative_frequency (analysis).") # MUDADO PARA DEBUG ou REMOVIDO
    logger.debug("Interno: Iniciando calculate_relative_frequency (analysis).")

    if df_frequency_abs.empty:
        logger.warning("DataFrame de frequência absoluta vazio para calculate_relative_frequency.")
        return pd.DataFrame({'Dezena': config.ALL_NUMBERS, 'Frequencia Relativa': 0.0})
    
    if 'Dezena' not in df_frequency_abs.columns or 'Frequencia Absoluta' not in df_frequency_abs.columns:
        logger.error("Colunas 'Dezena' ou 'Frequencia Absoluta' não encontradas no DataFrame de entrada.")
        return None

    if total_draws == 0:
        logger.warning("Total de sorteios é zero. Frequência relativa será zero.")
        df_relative_frequency = df_frequency_abs[['Dezena']].copy()
        df_relative_frequency['Frequencia Relativa'] = 0.0
        return df_relative_frequency

    df_relative_frequency = df_frequency_abs.copy()
    df_relative_frequency['Frequencia Relativa'] = (df_relative_frequency['Frequencia Absoluta'] / total_draws).round(6)
    
    # logger.info(f"Frequência relativa calculada para {len(df_relative_frequency)} dezenas em calculate_relative_frequency (analysis).") # MUDADO PARA DEBUG ou REMOVIDO
    logger.debug(f"Frequência relativa calculada para {len(df_relative_frequency)} dezenas em calculate_relative_frequency (analysis).")
    return df_relative_frequency