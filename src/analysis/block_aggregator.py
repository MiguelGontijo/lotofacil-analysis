# src/analysis/block_aggregator.py
import pandas as pd
import logging
from typing import Dict, List, Any

from src.database_manager import DatabaseManager
from src.config import CHUNK_TYPES_CONFIG

logger = logging.getLogger(__name__)

def aggregate_block_data_to_wide_format(db_manager: DatabaseManager):
    logger.info("Iniciando agregação de dados de bloco para formato largo (incluindo atraso médio).")

    metric_configs = [
        {
            "source_table_prefix": "evol_metric_frequency",
            "value_column": "frequencia_absoluta",
            "analysis_type_name": "frequencia_bloco"
        },
        {
            "source_table_prefix": "evol_rank_frequency_bloco",
            "value_column": "rank_no_bloco",
            "analysis_type_name": "rank_freq_bloco"
        },
        { # <<< NOVA MÉTRICA ADICIONADA AQUI >>>
            "source_table_prefix": "evol_metric_atraso_medio_bloco",
            "value_column": "atraso_medio_no_bloco",
            "analysis_type_name": "atraso_medio_bloco"
        },
    ]

    for chunk_type, config in CHUNK_TYPES_CONFIG.items():
        chunk_sizes = config.get('sizes', [])
        for size in chunk_sizes:
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
                columns_col = 'dezena'
                
                required_for_pivot = index_cols + [columns_col, value_column_in_long_table]
                if not all(col in df_long.columns for col in required_for_pivot):
                    logger.error(f"Tabela '{long_format_table_name}' não contém todas as colunas necessárias para pivotar ({required_for_pivot}). Colunas presentes: {df_long.columns.tolist()}. Pulando '{analysis_type_name_for_wide_table}'.")
                    continue
                
                logger.debug(f"Pivotando tabela '{long_format_table_name}' para a métrica '{analysis_type_name_for_wide_table}'.")
                try:
                    df_wide_metric = df_long.pivot_table(
                        index=index_cols,
                        columns=columns_col,
                        values=value_column_in_long_table,
                        fill_value=pd.NA # Usar NA para preencher valores ausentes no pivot se necessário
                    )
                    df_wide_metric.columns = [f'dezena_{int(col)}' for col in df_wide_metric.columns]
                    df_wide_metric.reset_index(inplace=True)
                    df_wide_metric['tipo_analise'] = analysis_type_name_for_wide_table
                    
                    final_cols_order = index_cols + ['tipo_analise'] + [f'dezena_{i}' for i in range(1, 26)]
                    for i in range(1, 26):
                        col_name_to_check = f'dezena_{i}'
                        if col_name_to_check not in df_wide_metric.columns:
                            df_wide_metric[col_name_to_check] = pd.NA
                    
                    df_wide_metric = df_wide_metric[final_cols_order]
                    all_wide_dfs_for_this_chunk_config.append(df_wide_metric)
                    logger.debug(f"DataFrame largo para '{analysis_type_name_for_wide_table}' ({chunk_type}_{size}) criado com {len(df_wide_metric)} linhas.")

                except Exception as e:
                    logger.error(f"Erro ao pivotar ou processar '{long_format_table_name}' para '{analysis_type_name_for_wide_table}': {e}", exc_info=True)

            if all_wide_dfs_for_this_chunk_config:
                df_consolidated_wide = pd.concat(all_wide_dfs_for_this_chunk_config, ignore_index=True)
                # Tratar tipos de dados antes de salvar, especialmente para colunas de dezenas
                for i in range(1, 26):
                    col_name = f'dezena_{i}'
                    # Se for uma coluna de rank ou frequência, deve ser Int64
                    # Se for atraso médio, pode ser float ou Nullable Float
                    if analysis_type_name_for_wide_table in ["frequencia_bloco", "rank_freq_bloco"]:
                         df_consolidated_wide[col_name] = pd.to_numeric(df_consolidated_wide[col_name], errors='coerce').astype('Int64')
                    elif analysis_type_name_for_wide_table == "atraso_medio_bloco":
                         df_consolidated_wide[col_name] = pd.to_numeric(df_consolidated_wide[col_name], errors='coerce') # Mantém float, permite NaNs
                
                try:
                    db_manager.save_dataframe_to_db(df_consolidated_wide, consolidated_table_name, if_exists='replace')
                    logger.info(f"Tabela consolidada '{consolidated_table_name}' salva com {len(df_consolidated_wide)} linhas.")
                except Exception as e:
                    logger.error(f"Erro ao salvar tabela consolidada '{consolidated_table_name}': {e}", exc_info=True)
            else:
                logger.warning(f"Nenhum DataFrame largo gerado para '{consolidated_table_name}'. Nada a salvar.")

    logger.info("Agregação de dados de bloco para formato largo (incluindo atraso médio) concluída.")