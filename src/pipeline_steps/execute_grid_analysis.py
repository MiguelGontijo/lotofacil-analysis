# src/pipeline_steps/execute_grid_analysis.py
import logging
import pandas as pd
from typing import Any, Dict # Para os type hints dos argumentos

# Importa a função de análise principal
from src.analysis.grid_analysis import analyze_grid_distribution

logger = logging.getLogger(__name__)

def run_grid_analysis_step(
    all_data_df: pd.DataFrame,
    db_manager: Any,  # Espera-se uma instância de DatabaseManager
    config: Any,      # Espera-se uma instância de Config
    shared_context: Dict[str, Any], # Dicionário de contexto compartilhado
    **kwargs
) -> bool:
    """
    Executa a etapa de Análise de Distribuição por Linhas e Colunas.
    Calcula a frequência com que 0 a 5 dezenas são sorteadas em cada linha/coluna.
    Os argumentos são injetados pelo Orchestrator.
    """
    step_name = "Análise de Distribuição por Linhas e Colunas"
    logger.info(f"==== Iniciando Etapa: {step_name} ====")
    
    # Validações básicas dos argumentos e configurações injetados
    if not hasattr(config, 'DRAWN_NUMBERS_COLUMN_NAME') or \
       not hasattr(config, 'LOTOFACIL_GRID_LINES') or \
       not hasattr(config, 'LOTOFACIL_GRID_COLUMNS') or \
       not hasattr(config, 'GRID_LINE_DISTRIBUTION_TABLE_NAME') or \
       not hasattr(config, 'GRID_COLUMN_DISTRIBUTION_TABLE_NAME'):
        logger.error("Atributos de configuração necessários para a Análise de Linhas e Colunas não encontrados.")
        logger.info(f"==== Etapa: {step_name} FALHOU ====")
        return False

    if not isinstance(all_data_df, pd.DataFrame) or all_data_df.empty:
        logger.error("DataFrame de sorteios (all_data_df) injetado está inválido ou vazio.")
        logger.info(f"==== Etapa: {step_name} FALHOU ====")
        return False
    
    if not hasattr(db_manager, 'save_dataframe'):
        logger.error("Objeto db_manager injetado não possui o método 'save_dataframe'.")
        logger.info(f"==== Etapa: {step_name} FALHOU ====")
        return False

    line_table_name = config.GRID_LINE_DISTRIBUTION_TABLE_NAME
    column_table_name = config.GRID_COLUMN_DISTRIBUTION_TABLE_NAME

    try:
        logger.info("Calculando distribuição de frequência por linhas e colunas...")
        line_distribution_df, column_distribution_df = analyze_grid_distribution(all_data_df, config)

        # Validação dos DataFrames retornados
        if not isinstance(line_distribution_df, pd.DataFrame) or \
           not isinstance(column_distribution_df, pd.DataFrame):
            logger.error("A análise de grid não retornou os DataFrames esperados.")
            logger.info(f"==== Etapa: {step_name} FALHOU ====")
            return False
        
        # Salvar distribuição por linhas
        if line_distribution_df.empty:
            logger.info(f"DataFrame de distribuição por linhas está vazio. Nada será salvo na tabela '{line_table_name}'.")
        else:
            db_manager.save_dataframe(line_distribution_df, line_table_name, if_exists='replace')
            logger.info(f"Distribuição por linhas salva na tabela: {line_table_name}")
        
        shared_context['grid_line_distribution_df'] = line_distribution_df
        logger.info("Resultado da distribuição por linhas adicionado ao shared_context.")

        # Salvar distribuição por colunas
        if column_distribution_df.empty:
            logger.info(f"DataFrame de distribuição por colunas está vazio. Nada será salvo na tabela '{column_table_name}'.")
        else:
            db_manager.save_dataframe(column_distribution_df, column_table_name, if_exists='replace')
            logger.info(f"Distribuição por colunas salva na tabela: {column_table_name}")

        shared_context['grid_column_distribution_df'] = column_distribution_df
        logger.info("Resultado da distribuição por colunas adicionado ao shared_context.")

    except Exception as e:
        logger.error(f"Erro durante a execução da {step_name}: {e}", exc_info=True)
        logger.info(f"==== Etapa: {step_name} FALHOU ====")
        return False
            
    logger.info(f"==== Etapa: {step_name} CONCLUÍDA ====")
    return True