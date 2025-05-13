# src/analysis/block_aggregator.py
import pandas as pd
import logging
from typing import Dict, List, Any

from src.database_manager import DatabaseManager
from src.config import CHUNK_TYPES_CONFIG # Para saber quais chunks processar

logger = logging.getLogger(__name__)

def aggregate_block_data_to_wide_format(db_manager: DatabaseManager):
    """
    Lê as tabelas de métricas e ranks por chunk (formato longo),
    transforma-as para o formato largo (uma linha por tipo de análise por chunk,
    com colunas para cada dezena), e salva em novas tabelas consolidadas.
    """
    logger.info("Iniciando agregação de dados de bloco para formato largo.")

    # Configuração das métricas a serem processadas e seus detalhes
    # O 'value_column' é o nome da coluna na tabela de formato longo que contém o valor da métrica.
    # O 'analysis_type_name' é como essa métrica será identificada na coluna 'tipo_analise' da tabela larga.
    metric_configs = [
        {
            "source_table_prefix": "evol_metric_frequency", # Prefixo da tabela longa original
            "value_column": "frequencia_absoluta",         # Coluna com os valores na tabela longa
            "analysis_type_name": "frequencia_bloco"       # Nome para 'tipo_analise' na tabela larga
        },
        {
            "source_table_prefix": "evol_rank_frequency_bloco", # Prefixo da tabela longa de ranks
            "value_column": "rank_no_bloco",                    # Coluna com os valores de rank
            "analysis_type_name": "rank_freq_bloco"             # Nome para 'tipo_analise' na tabela larga
        },
        # Adicionar aqui futuras métricas por bloco, por exemplo:
        # {
        #     "source_table_prefix": "evol_metric_atraso_medio_bloco",
        #     "value_column": "atraso_medio_no_bloco", # Supondo que esta seja a coluna
        #     "analysis_type_name": "atraso_medio_bloco"
        # },
    ]

    for chunk_type, config in CHUNK_TYPES_CONFIG.items():
        chunk_sizes = config.get('sizes', [])
        for size in chunk_sizes:
            # Nome da nova tabela larga consolidada para esta configuração de chunk
            consolidated_table_name = f"bloco_analises_consolidadas_{chunk_type}_{size}"
            logger.info(f"Processando para tabela consolidada: '{consolidated_table_name}'")
            
            all_wide_dfs_for_this_chunk_config: List[pd.DataFrame] = []

            for metric_config in metric_configs:
                source_table_prefix = metric_config["source_table_prefix"]
                value_column_in_long_table = metric_config["value_column"]
                analysis_type_name_for_wide_table = metric_config["analysis_type_name"]

                long_format_table_name = f"{source_table_prefix}_{chunk_type}_{size}"

                if not db_manager.table_exists(long_format_table_name):
                    logger.warning(f"Tabela de formato longo '{long_format_table_name}' não encontrada. Pulando agregação para '{analysis_type_name_for_wide_table}'.")
                    continue
                
                df_long = db_manager.load_dataframe_from_db(long_format_table_name)

                if df_long is None or df_long.empty:
                    logger.warning(f"DataFrame carregado de '{long_format_table_name}' está vazio. Pulando agregação para '{analysis_type_name_for_wide_table}'.")
                    continue

                index_cols = ['chunk_seq_id', 'chunk_start_contest', 'chunk_end_contest']
                columns_col = 'dezena' # Coluna cujos valores virarão novas colunas (dezena_1, dezena_2, ...)
                
                required_for_pivot = index_cols + [columns_col, value_column_in_long_table]
                if not all(col in df_long.columns for col in required_for_pivot):
                    logger.error(f"Tabela '{long_format_table_name}' não contém todas as colunas necessárias para pivotar ({required_for_pivot}). Colunas presentes: {df_long.columns.tolist()}. Pulando '{analysis_type_name_for_wide_table}'.")
                    continue
                
                logger.debug(f"Pivotando tabela '{long_format_table_name}' para a métrica '{analysis_type_name_for_wide_table}'.")
                try:
                    # Realiza o pivot
                    df_wide_metric = df_long.pivot_table(
                        index=index_cols,
                        columns=columns_col,
                        values=value_column_in_long_table
                    ) # O resultado terá MultiIndex nas colunas se value_column_in_long_table não for único por dezena, mas aqui é.
                      # As colunas serão os números das dezenas (1, 2, ..., 25).
                    
                    # Renomeia as colunas numéricas (dezenas) para o formato 'dezena_X'
                    df_wide_metric.columns = [f'dezena_{int(col)}' for col in df_wide_metric.columns]
                    df_wide_metric.reset_index(inplace=True) # Transforma os index_cols de volta em colunas
                    
                    # Adiciona a coluna 'tipo_analise'
                    df_wide_metric['tipo_analise'] = analysis_type_name_for_wide_table
                    
                    # Garante a ordem final das colunas e que todas as colunas de dezena_X existam
                    final_cols_order = index_cols + ['tipo_analise'] + [f'dezena_{i}' for i in range(1, 26)]
                    for i in range(1, 26):
                        col_name_to_check = f'dezena_{i}'
                        if col_name_to_check not in df_wide_metric.columns:
                            df_wide_metric[col_name_to_check] = pd.NA # Ou np.nan, dependendo do tipo de dado esperado
                    
                    df_wide_metric = df_wide_metric[final_cols_order] # Seleciona e reordena
                    all_wide_dfs_for_this_chunk_config.append(df_wide_metric)
                    logger.debug(f"DataFrame largo para '{analysis_type_name_for_wide_table}' ({chunk_type}_{size}) criado com {len(df_wide_metric)} linhas.")

                except Exception as e:
                    logger.error(f"Erro ao pivotar ou processar '{long_format_table_name}' para '{analysis_type_name_for_wide_table}': {e}", exc_info=True)

            if all_wide_dfs_for_this_chunk_config:
                # Concatena todos os DataFrames largos (um para cada tipo_analise) para esta config de chunk
                df_consolidated_wide = pd.concat(all_wide_dfs_for_this_chunk_config, ignore_index=True)
                try:
                    # Salva a tabela consolidada, substituindo se já existir, pois estamos reconstruindo-a do zero.
                    db_manager.save_dataframe_to_db(df_consolidated_wide, consolidated_table_name, if_exists='replace')
                    logger.info(f"Tabela consolidada '{consolidated_table_name}' salva com {len(df_consolidated_wide)} linhas.")
                except Exception as e:
                    logger.error(f"Erro ao salvar tabela consolidada '{consolidated_table_name}': {e}", exc_info=True)
            else:
                logger.warning(f"Nenhum DataFrame largo gerado para '{consolidated_table_name}'. Nada a salvar.")

    logger.info("Agregação de dados de bloco para formato largo concluída.")