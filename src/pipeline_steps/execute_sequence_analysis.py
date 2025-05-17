# src/pipeline_steps/execute_sequence_analysis.py
import pandas as pd
from typing import Dict, Any
import logging

# Importações robustas
try:
    from ..analysis.sequence_analysis import analyze_sequences
    from ..config import Config # Usaremos config_obj que é uma instância de Config
    from ..database_manager import DatabaseManager
except ImportError:
    from src.analysis.sequence_analysis import analyze_sequences
    from src.config import Config
    from src.database_manager import DatabaseManager

logger = logging.getLogger(__name__)

def run_sequence_analysis_step(
    all_data_df: pd.DataFrame, 
    db_manager: DatabaseManager, 
    config: Config, # Espera a instância config_obj
    shared_context: Dict[str, Any], # Para consistência e uso futuro se necessário
    **kwargs 
) -> None:
    """
    Executa a análise de sequências numéricas e salva os resultados.
    """
    logger.info("Iniciando etapa do pipeline: Análise de Sequências Numéricas.")

    if all_data_df is None or all_data_df.empty:
        logger.error("DataFrame 'all_data_df' é crucial e não está disponível ou está vazio. Abortando etapa de análise de sequências.")
        return
    
    # A função analyze_sequences já verifica a coluna 'drawn_numbers'
    # e as configurações de SEQUENCE_ANALYSIS_CONFIG dentro do objeto config.
    
    try:
        logger.info("Calculando métricas de sequências numéricas...")
        sequence_metrics_df = analyze_sequences(
            all_data_df.copy(), # Passa uma cópia para a análise, caso ela modifique o df
            config # Passa o objeto config_obj
        )
    except ValueError as ve: 
        logger.error(f"Erro de valor durante a análise de sequências: {ve}", exc_info=False) # Não mostrar traceback completo para ValueError esperado
        return
    except Exception as e:
        logger.error(f"Erro inesperado durante a análise de sequências: {e}", exc_info=True)
        return

    # Nome da tabela obtido do config
    output_table_name = getattr(config, 'SEQUENCE_METRICS_TABLE_NAME', 'sequence_metrics') # Default para 'sequence_metrics'

    if sequence_metrics_df is not None and not sequence_metrics_df.empty:
        try:
            db_manager.save_dataframe(sequence_metrics_df, output_table_name, if_exists="replace")
            logger.info(f"Métricas de sequências numéricas salvas na tabela '{output_table_name}'. {len(sequence_metrics_df)} registros.")
            # Adicionar ao shared_context se outras etapas precisarem
            if shared_context is not None:
                 shared_context[output_table_name + '_df'] = sequence_metrics_df
        except Exception as e:
            logger.error(f"Erro ao salvar métricas de sequências na tabela '{output_table_name}': {e}", exc_info=True)
    else:
        logger.info("Nenhuma métrica de sequência numérica foi gerada ou retornada para salvar (pode ser devido à configuração 'active': False ou nenhum dado).")

    logger.info("Etapa do pipeline: Análise de Sequências Numéricas concluída.")