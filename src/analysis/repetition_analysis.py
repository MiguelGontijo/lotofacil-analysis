# src/analysis/repetition_analysis.py
import pandas as pd
from typing import List, Dict, Any, Set # Adicionado Set
import logging

logger = logging.getLogger(__name__)

def calculate_previous_draw_repetitions(all_data_df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula, para cada concurso, quantas dezenas foram repetidas do sorteio anterior
    e quais foram essas dezenas.

    Args:
        all_data_df: DataFrame com todos os concursos. 
                     Esperadas colunas 'Concurso' e 'bola_1'...'bola_15'.

    Returns:
        DataFrame com colunas: 'Concurso', 'Data' (opcional), 
                               'QtdDezenasRepetidas', 'DezenasRepetidas'.
        Retorna um DataFrame vazio em caso de erro ou se houver menos de 2 concursos.
    """
    logger.info("Iniciando análise de repetição de dezenas do concurso anterior.")
    if all_data_df is None or all_data_df.empty or len(all_data_df) < 2:
        logger.warning("DataFrame de entrada insuficiente para análise de repetição (necessário pelo menos 2 concursos).")
        return pd.DataFrame(columns=['Concurso', 'Data', 'QtdDezenasRepetidas', 'DezenasRepetidas'])

    required_cols = ['Concurso'] + [f'bola_{i}' for i in range(1, 16)]
    missing_cols = [col for col in required_cols if col not in all_data_df.columns]
    if missing_cols:
        logger.error(f"Colunas essenciais ausentes no DataFrame: {missing_cols}. Não é possível analisar repetições.")
        return pd.DataFrame(columns=['Concurso', 'Data', 'QtdDezenasRepetidas', 'DezenasRepetidas'])

    # Garante que os dados estão ordenados por 'Concurso'
    df_sorted = all_data_df.sort_values(by='Concurso').reset_index(drop=True)
    dezena_cols = [f'bola_{i}' for i in range(1, 16)]
    
    repetition_data: List[Dict[str, Any]] = []

    for i in range(1, len(df_sorted)): # Começa do segundo concurso
        current_row = df_sorted.iloc[i]
        previous_row = df_sorted.iloc[i-1]

        try:
            current_draw_numbers = set(int(num) for col in dezena_cols if pd.notna(current_row[col]) for num in [current_row[col]])
            previous_draw_numbers = set(int(num) for col in dezena_cols if pd.notna(previous_row[col]) for num in [previous_row[col]])

            if not current_draw_numbers or not previous_draw_numbers: # Segurança
                logger.debug(f"Sorteio atual ou anterior inválido para concurso {current_row['Concurso']}. Pulando repetição.")
                repeated_count = 0
                repeated_numbers_str = None
            else:
                repeated_numbers = current_draw_numbers.intersection(previous_draw_numbers)
                repeated_count = len(repeated_numbers)
                repeated_numbers_str = ",".join(map(str, sorted(list(repeated_numbers)))) if repeated_numbers else None
            
            data_entry: Dict[str, Any] = {
                'Concurso': int(current_row['Concurso']),
                'QtdDezenasRepetidas': repeated_count,
                'DezenasRepetidas': repeated_numbers_str
            }
            if 'Data' in current_row and pd.notna(current_row['Data']):
                data_entry['Data'] = current_row['Data']
            
            repetition_data.append(data_entry)

        except Exception as e:
            logger.error(f"Erro ao processar repetição para o concurso {current_row.get('Concurso', 'Desconhecido')}: {e}", exc_info=True)
            # Adiciona uma entrada com 0 repetições em caso de erro para não perder a linha do concurso
            repetition_data.append({
                'Concurso': int(current_row['Concurso']),
                'QtdDezenasRepetidas': 0,
                'DezenasRepetidas': None
            })


    if not repetition_data:
        logger.warning("Nenhum dado de repetição foi gerado.")
        return pd.DataFrame(columns=['Concurso', 'Data', 'QtdDezenasRepetidas', 'DezenasRepetidas'])
        
    df_repetitions = pd.DataFrame(repetition_data)
    
    # Reordenar colunas se 'Data' estiver presente
    final_columns = ['Concurso']
    if 'Data' in df_repetitions.columns:
        final_columns.append('Data')
    final_columns.extend(['QtdDezenasRepetidas', 'DezenasRepetidas'])
    df_repetitions = df_repetitions[final_columns]
    
    logger.info(f"Análise de repetição concluída. {len(df_repetitions)} registros gerados.")
    return df_repetitions