# src/analysis/seasonality_analysis.py
import pandas as pd
import logging
from typing import Any # Para o objeto config

logger = logging.getLogger(__name__)

def analyze_monthly_number_frequency(
    all_draws_df: pd.DataFrame, 
    config: Any
) -> pd.DataFrame:
    """
    Analisa a frequência de ocorrência de cada dezena por mês, agregando todos os anos.

    Args:
        all_draws_df (pd.DataFrame): DataFrame com todos os sorteios.
                                     Deve conter a coluna de data (config.DATE_COLUMN_NAME)
                                     e a coluna de dezenas sorteadas (config.DRAWN_NUMBERS_COLUMN_NAME).
        config (Any): Objeto de configuração, que deve ter os atributos:
                      DATE_COLUMN_NAME, DRAWN_NUMBERS_COLUMN_NAME, ALL_NUMBERS.

    Returns:
        pd.DataFrame: DataFrame com a frequência mensal das dezenas.
                      Colunas: 'Dezena', 'Mes', 'Frequencia_Absoluta_Total_Mes',
                               'Total_Sorteios_Considerados_Mes', 'Frequencia_Relativa_Mes'.
    """
    step_name = "Análise de Frequência Mensal de Dezenas"
    logger.info(f"Iniciando {step_name}.")

    date_col = config.DATE_COLUMN_NAME
    drawn_numbers_col = config.DRAWN_NUMBERS_COLUMN_NAME
    all_numbers = config.ALL_NUMBERS

    if date_col not in all_draws_df.columns:
        logger.error(f"Coluna de data '{date_col}' não encontrada no DataFrame de sorteios.")
        return pd.DataFrame(columns=['Dezena', 'Mes', 'Frequencia_Absoluta_Total_Mes', 'Total_Sorteios_Considerados_Mes', 'Frequencia_Relativa_Mes'])
    if drawn_numbers_col not in all_draws_df.columns:
        logger.error(f"Coluna de dezenas sorteadas '{drawn_numbers_col}' não encontrada no DataFrame de sorteios.")
        return pd.DataFrame(columns=['Dezena', 'Mes', 'Frequencia_Absoluta_Total_Mes', 'Total_Sorteios_Considerados_Mes', 'Frequencia_Relativa_Mes'])

    # Garante que a coluna de data seja do tipo datetime
    try:
        # Cria uma cópia para evitar SettingWithCopyWarning
        df_analysis = all_draws_df[[date_col, drawn_numbers_col]].copy()
        df_analysis[date_col] = pd.to_datetime(df_analysis[date_col], errors='coerce')
        df_analysis.dropna(subset=[date_col], inplace=True) # Remove linhas onde a data não pôde ser convertida
    except Exception as e:
        logger.error(f"Erro ao converter a coluna de data '{date_col}' para datetime: {e}", exc_info=True)
        return pd.DataFrame(columns=['Dezena', 'Mes', 'Frequencia_Absoluta_Total_Mes', 'Total_Sorteios_Considerados_Mes', 'Frequencia_Relativa_Mes'])

    if df_analysis.empty:
        logger.warning("DataFrame vazio após processamento da coluna de data. Nenhuma análise sazonal será realizada.")
        return pd.DataFrame(columns=['Dezena', 'Mes', 'Frequencia_Absoluta_Total_Mes', 'Total_Sorteios_Considerados_Mes', 'Frequencia_Relativa_Mes'])

    df_analysis['Mes'] = df_analysis[date_col].dt.month

    # 1. Contar total de sorteios por mês (agregando todos os anos)
    total_draws_per_month = df_analysis['Mes'].value_counts().sort_index().to_dict()

    # 2. Calcular frequência absoluta de cada dezena por mês
    monthly_freq_data = []
    for month in range(1, 13): # Meses de 1 a 12
        draws_in_month_df = df_analysis[df_analysis['Mes'] == month]
        
        # Total de sorteios ocorridos neste mês específico em todos os anos
        total_draws_this_month_all_years = total_draws_per_month.get(month, 0)
        
        if not draws_in_month_df.empty and total_draws_this_month_all_years > 0:
            all_numbers_in_month = []
            for numbers_list in draws_in_month_df[drawn_numbers_col]:
                if isinstance(numbers_list, list):
                    all_numbers_in_month.extend(numbers_list)
            
            month_counts = pd.Series(all_numbers_in_month).value_counts()
            
            for dezena in all_numbers:
                abs_freq = month_counts.get(dezena, 0)
                rel_freq = abs_freq / (total_draws_this_month_all_years * config.NUMBERS_PER_DRAW) if total_draws_this_month_all_years > 0 else 0.0
                # A frequência relativa aqui é em relação ao total de "slots" de dezenas sorteadas naquele mês.
                # Outra opção seria FreqAbs / Total de vezes que a dezena poderia ter saído (Total_Sorteios_No_Mes).
                # Vamos usar a frequência absoluta da dezena e o total de sorteios no mês para a tabela final.
                # A coluna 'Frequencia_Relativa_Mes' na tabela será FreqAbsTotalMes / TotalSorteiosConsideradosMes
                # (que é a proporção de sorteios do mês em que a dezena saiu)

                monthly_freq_data.append({
                    'Dezena': dezena,
                    'Mes': month,
                    'Frequencia_Absoluta_Total_Mes': abs_freq,
                    'Total_Sorteios_Considerados_Mes': total_draws_this_month_all_years,
                    'Frequencia_Relativa_Mes': (abs_freq / total_draws_this_month_all_years) if total_draws_this_month_all_years > 0 else 0.0
                })
        else: # Caso não haja sorteios para um determinado mês em todo o histórico
            for dezena in all_numbers:
                monthly_freq_data.append({
                    'Dezena': dezena,
                    'Mes': month,
                    'Frequencia_Absoluta_Total_Mes': 0,
                    'Total_Sorteios_Considerados_Mes': 0,
                    'Frequencia_Relativa_Mes': 0.0
                })


    result_df = pd.DataFrame(monthly_freq_data)
    if not result_df.empty:
        result_df = result_df.sort_values(by=['Dezena', 'Mes']).reset_index(drop=True)
        
    logger.info(f"{step_name} concluída. {len(result_df)} registros gerados.")
    return result_df