# Lotofacil_Analysis/src/pipeline_steps/execute_frequent_itemsets.py
import logging
import pandas as pd
from typing import Dict, Any

# from src.config import Config
# from src.database_manager import DatabaseManager
# from src.analysis.combination_analysis import CombinationAnalyzer

logger = logging.getLogger(__name__)

def run_frequent_itemsets_analysis_step(
    all_data_df: pd.DataFrame, # Nome do parâmetro padronizado
    db_manager: Any, # DatabaseManager
    combination_analyzer: Any, # CombinationAnalyzer
    config: Any, # Config
    shared_context: Dict[str, Any],
    **kwargs
) -> None: # Mudado para None, pois não estava retornando bool explicitamente
    step_name = "Frequent Itemsets Analysis"
    logger.info(f"Iniciando etapa do pipeline: {step_name}")

    try:
        min_support = config.APRIORI_MIN_SUPPORT
        min_len = config.FREQUENT_ITEMSETS_MIN_LEN
        max_len = config.FREQUENT_ITEMSETS_MAX_LEN
        drawn_numbers_col = config.DRAWN_NUMBERS_COLUMN_NAME
        
        logger.info(f"Parâmetros da análise: min_support={min_support}, min_len={min_len}, max_len={max_len}")

        frequent_itemsets_df = combination_analyzer.analyze_frequent_itemsets(
            all_draws_df=all_data_df, # Passa o DataFrame correto
            min_support=min_support,
            min_len=min_len,
            max_len=max_len,
            drawn_numbers_col=drawn_numbers_col
        )

        if frequent_itemsets_df.empty:
            logger.info(f"Nenhum itemset frequente encontrado ou retornado pela análise para a etapa {step_name}. Nada a salvar.")
        else:
            logger.info(f"Análise de itemsets frequentes concluída. {len(frequent_itemsets_df)} itemsets encontrados.")
            table_name = "frequent_itemsets"
            db_manager.save_dataframe(frequent_itemsets_df, table_name, if_exists='replace')
            logger.info(f"Resultados da análise de itemsets frequentes salvos na tabela '{table_name}'.")
        # Removido retorno implícito, se precisar de status, adicione return True/False
    except AttributeError as e:
        logger.error(f"Erro na etapa {step_name}: Atributo de configuração ausente. Detalhes: {e}", exc_info=True)
    except Exception as e:
        logger.error(f"Erro ao executar a etapa {step_name}: {e}", exc_info=True)

    logger.info(f"Etapa do pipeline: {step_name} concluída.")
    # Adicionar return True se for esperado pelo orchestrator
    # return True