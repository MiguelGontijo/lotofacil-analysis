# src/analysis/seasonality_analysis.py
import pandas as pd
import numpy as np
import logging
from typing import Any, Dict, List # Adicionado List
from collections import Counter

logger = logging.getLogger(__name__)

def analyze_monthly_number_frequency(
    all_draws_df: pd.DataFrame, 
    config: Any
) -> pd.DataFrame:
    """
    Analisa a frequência de ocorrência de cada dezena por mês, agregando todos os anos.
    """
    step_name = "Análise de Frequência Mensal de Dezenas" # Corrigido para nome da sub-análise
    logger.info(f"Iniciando {step_name}.")

    date_col = config.DATE_COLUMN_NAME
    drawn_numbers_col = config.DRAWN_NUMBERS_COLUMN_NAME
    all_numbers = config.ALL_NUMBERS

    # Validações de colunas
    if date_col not in all_draws_df.columns:
        logger.error(f"Coluna de data '{date_col}' não encontrada no DataFrame de sorteios.")
        return pd.DataFrame(columns=['Dezena', 'Mes', 'Frequencia_Absoluta_Total_Mes', 'Total_Sorteios_Considerados_Mes', 'Frequencia_Relativa_Mes'])
    if drawn_numbers_col not in all_draws_df.columns:
        logger.error(f"Coluna de dezenas sorteadas '{drawn_numbers_col}' não encontrada.")
        return pd.DataFrame(columns=['Dezena', 'Mes', 'Frequencia_Absoluta_Total_Mes', 'Total_Sorteios_Considerados_Mes', 'Frequencia_Relativa_Mes'])

    try:
        df_analysis = all_draws_df[[date_col, drawn_numbers_col]].copy()
        df_analysis[date_col] = pd.to_datetime(df_analysis[date_col], errors='coerce')
        df_analysis.dropna(subset=[date_col], inplace=True)
    except Exception as e:
        logger.error(f"Erro ao processar a coluna de data '{date_col}': {e}", exc_info=True)
        return pd.DataFrame(columns=['Dezena', 'Mes', 'Frequencia_Absoluta_Total_Mes', 'Total_Sorteios_Considerados_Mes', 'Frequencia_Relativa_Mes'])

    if df_analysis.empty:
        logger.warning("DataFrame vazio após processamento da coluna de data. Nenhuma análise de frequência mensal será realizada.")
        return pd.DataFrame(columns=['Dezena', 'Mes', 'Frequencia_Absoluta_Total_Mes', 'Total_Sorteios_Considerados_Mes', 'Frequencia_Relativa_Mes'])

    df_analysis['Mes'] = df_analysis[date_col].dt.month
    total_draws_overall = len(df_analysis) # Usado para logs ou outras métricas, não para a freq relativa mensal direta

    # Contar total de sorteios por mês (agregando todos os anos)
    total_draws_per_month_map = df_analysis['Mes'].value_counts().sort_index().to_dict()

    monthly_freq_data = []
    for month in range(1, 13):
        draws_in_month_df = df_analysis[df_analysis['Mes'] == month]
        total_draws_this_month_all_years = total_draws_per_month_map.get(month, 0)
        
        if not draws_in_month_df.empty and total_draws_this_month_all_years > 0:
            all_numbers_in_month_list = []
            for numbers_list in draws_in_month_df[drawn_numbers_col]:
                if isinstance(numbers_list, list):
                    all_numbers_in_month_list.extend(numbers_list)
            
            month_counts = pd.Series(all_numbers_in_month_list).value_counts()
            
            for dezena in all_numbers:
                abs_freq = month_counts.get(dezena, 0)
                # Frequencia_Relativa_Mes = Proporção de sorteios DO MÊS em que a dezena apareceu
                rel_freq = (abs_freq / total_draws_this_month_all_years) if total_draws_this_month_all_years > 0 else 0.0
                
                monthly_freq_data.append({
                    'Dezena': dezena,
                    'Mes': month,
                    'Frequencia_Absoluta_Total_Mes': abs_freq,
                    'Total_Sorteios_Considerados_Mes': total_draws_this_month_all_years,
                    'Frequencia_Relativa_Mes': round(rel_freq, 6)
                })
        else:
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

# --- NOVA FUNÇÃO PARA PROPRIEDADES NUMÉRICAS MENSAIS ---
def analyze_monthly_draw_properties(
    all_draws_df: pd.DataFrame, # Usado para obter a coluna de data e fazer o merge
    properties_df: pd.DataFrame, # DataFrame da tabela 'propriedades_numericas_por_concurso'
    config: Any
) -> pd.DataFrame:
    """
    Analisa o sumário de propriedades numéricas dos sorteios (soma, pares, ímpares, primos)
    agregados por mês.

    Args:
        all_draws_df (pd.DataFrame): DataFrame com todos os sorteios, deve conter
                                     config.DATE_COLUMN_NAME e config.CONTEST_ID_COLUMN_NAME.
        properties_df (pd.DataFrame): DataFrame com as propriedades numéricas por concurso.
                                      Deve conter config.CONTEST_ID_COLUMN_NAME e as colunas
                                      de propriedades (ex: 'soma_dezenas', 'pares', 'impares', 'primos').
        config (Any): Objeto de configuração.

    Returns:
        pd.DataFrame: DataFrame com o sumário de propriedades médias por mês.
                      Colunas: 'Mes', 'Total_Sorteios_Mes', 'Soma_Media_Mensal', 
                               'Media_Pares_Mensal', 'Media_Impares_Mensal', 'Media_Primos_Mensal'.
    """
    step_name = "Análise de Sumário Mensal de Propriedades Numéricas"
    logger.info(f"Iniciando {step_name}.")

    date_col = config.DATE_COLUMN_NAME
    contest_id_col = config.CONTEST_ID_COLUMN_NAME

    # Colunas de propriedades que esperamos em properties_df e queremos agregar
    # A coluna 'Concurso' em properties_df corresponde a contest_id_col
    property_cols_to_aggregate = ['soma_dezenas', 'pares', 'impares', 'primos']

    if date_col not in all_draws_df.columns:
        logger.error(f"Coluna de data '{date_col}' não encontrada em all_draws_df.")
        return pd.DataFrame(columns=['Mes', 'Total_Sorteios_Mes'] + [f"Media_{col}_Mensal" for col in property_cols_to_aggregate]) # Ajustado nome da coluna soma
    if contest_id_col not in all_draws_df.columns:
        logger.error(f"Coluna de ID do concurso '{contest_id_col}' não encontrada em all_draws_df.")
        return pd.DataFrame(columns=['Mes', 'Total_Sorteios_Mes'] + [f"Media_{col}_Mensal" for col in property_cols_to_aggregate])
    
    if properties_df.empty:
        logger.warning("DataFrame de propriedades numéricas está vazio.")
        return pd.DataFrame(columns=['Mes', 'Total_Sorteios_Mes'] + [f"Media_{col}_Mensal" for col in property_cols_to_aggregate])
    
    # Verifica se a coluna de ID e as colunas de propriedades existem em properties_df
    # A coluna de ID em properties_df é "Concurso" (hardcoded no _create_table)
    properties_contest_id_col = "Concurso" # Nome da coluna em propriedades_numericas_por_concurso
    if properties_contest_id_col not in properties_df.columns:
        logger.error(f"Coluna de ID do concurso '{properties_contest_id_col}' não encontrada em properties_df.")
        return pd.DataFrame(columns=['Mes', 'Total_Sorteios_Mes'] + [f"Media_{col}_Mensal" for col in property_cols_to_aggregate])
    
    missing_prop_cols = [p_col for p_col in property_cols_to_aggregate if p_col not in properties_df.columns]
    if missing_prop_cols:
        logger.error(f"Colunas de propriedades ausentes em properties_df: {missing_prop_cols}.")
        return pd.DataFrame(columns=['Mes', 'Total_Sorteios_Mes'] + [f"Media_{col}_Mensal" for col in property_cols_to_aggregate])


    # Prepara df_dates com [contest_id, Mes]
    try:
        df_dates = all_draws_df[[contest_id_col, date_col]].copy()
        df_dates[date_col] = pd.to_datetime(df_dates[date_col], errors='coerce')
        df_dates.dropna(subset=[date_col], inplace=True)
        df_dates['Mes'] = df_dates[date_col].dt.month
        # Garante que contest_id_col seja do mesmo tipo que properties_contest_id_col para o merge
        df_dates[contest_id_col] = df_dates[contest_id_col].astype(properties_df[properties_contest_id_col].dtype)
    except Exception as e:
        logger.error(f"Erro ao processar datas e IDs de concurso: {e}", exc_info=True)
        return pd.DataFrame(columns=['Mes', 'Total_Sorteios_Mes'] + [f"Media_{col}_Mensal" for col in property_cols_to_aggregate])

    if df_dates.empty:
        logger.warning("DataFrame de datas vazio após processamento. Nenhuma análise será realizada.")
        return pd.DataFrame(columns=['Mes', 'Total_Sorteios_Mes'] + [f"Media_{col}_Mensal" for col in property_cols_to_aggregate])

    # Faz o merge para adicionar o Mês ao properties_df
    # Renomeia a coluna de ID em properties_df para corresponder a contest_id_col para o merge, se necessário,
    # ou usa left_on e right_on.
    # Vamos assumir que config.CONTEST_ID_COLUMN_NAME em all_draws_df é 'contest_id'
    # e a coluna em properties_df é 'Concurso'.
    merged_df = pd.merge(
        df_dates[[contest_id_col, 'Mes']], 
        properties_df[[properties_contest_id_col] + property_cols_to_aggregate],
        left_on=contest_id_col,
        right_on=properties_contest_id_col,
        how='inner' # Usa inner para garantir que apenas concursos com ambas as informações sejam usados
    )

    if merged_df.empty:
        logger.warning("DataFrame merged_df vazio após merge de datas e propriedades. Nenhuma análise será realizada.")
        return pd.DataFrame(columns=['Mes', 'Total_Sorteios_Mes'] + [f"Media_{col}_Mensal" for col in property_cols_to_aggregate])

    # Agrupa por Mês e calcula as médias e contagens
    # Primeiro, a contagem de sorteios por mês
    monthly_summary_counts = merged_df.groupby('Mes')[contest_id_col].count().reset_index(name='Total_Sorteios_Mes')

    # Depois, as médias das propriedades
    aggregation_dict = {prop_col: 'mean' for prop_col in property_cols_to_aggregate}
    monthly_summary_means = merged_df.groupby('Mes').agg(aggregation_dict).reset_index()
    
    # Renomeia as colunas de médias
    rename_map_means = {prop_col: f"Media_{prop_col}_Mensal" for prop_col in property_cols_to_aggregate}
    # Ajuste para a coluna de soma, para manter o padrão:
    if 'soma_dezenas' in rename_map_means:
        rename_map_means['soma_dezenas'] = 'Soma_Media_Mensal' # Nome da coluna como definido no DB
    
    monthly_summary_means.rename(columns=rename_map_means, inplace=True)

    # Junta as contagens com as médias
    final_summary_df = pd.merge(monthly_summary_counts, monthly_summary_means, on='Mes', how='outer')
    
    # Garante que todos os meses (1-12) estejam presentes, preenchendo com 0 ou NaN se não houver dados
    all_months_df = pd.DataFrame({'Mes': range(1, 13)})
    final_summary_df = pd.merge(all_months_df, final_summary_df, on='Mes', how='left')
    
    # Preenche NaNs nas contagens com 0 e nas médias com np.nan (ou 0.0 se preferir)
    if 'Total_Sorteios_Mes' in final_summary_df.columns:
        final_summary_df['Total_Sorteios_Mes'] = final_summary_df['Total_Sorteios_Mes'].fillna(0).astype(int)
    
    for col in final_summary_df.columns:
        if col != 'Mes' and col != 'Total_Sorteios_Mes':
            final_summary_df[col] = final_summary_df[col].fillna(np.nan) # ou 0.0
            # Arredondar as médias para melhor apresentação
            if pd.api.types.is_numeric_dtype(final_summary_df[col]):
                 final_summary_df[col] = final_summary_df[col].round(2)


    final_summary_df = final_summary_df.sort_values(by='Mes').reset_index(drop=True)

    logger.info(f"{step_name} concluída. {len(final_summary_df)} registros gerados.")
    return final_summary_df