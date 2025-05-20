# Lotofacil_Analysis/src/pipeline_steps/execute_frequent_itemsets.py
import logging
import pandas as pd
from typing import Dict, Any, Optional

from src.analysis.combination_analysis import CombinationAnalyzer
# Para type hints mais específicos, se desejar:
# from src.config import Config
# from src.database_manager import DatabaseManager

logger = logging.getLogger(__name__)

def run_frequent_itemsets_analysis_step(
    all_data_df: pd.DataFrame,
    db_manager: Any, # Espera-se DatabaseManager
    config: Any, # Espera-se Config
    shared_context: Dict[str, Any],
    **kwargs
) -> Optional[CombinationAnalyzer]: # Retorna a instância do analyzer ou None
    step_name = "Frequent Itemsets Analysis"
    logger.info(f"==== Iniciando Etapa: {step_name} ====")
    analyzer_instance: Optional[CombinationAnalyzer] = None

    try:
        required_config_attrs = [
            'APRIORI_MIN_SUPPORT', 'FREQUENT_ITEMSETS_MIN_LEN',
            'FREQUENT_ITEMSETS_MAX_LEN', 'DRAWN_NUMBERS_COLUMN_NAME',
            'FREQUENT_ITEMSETS_TABLE_NAME', 'ALL_NUMBERS'
        ]
        for attr in required_config_attrs:
            if not hasattr(config, attr):
                logger.error(f"{step_name}: Atributo de config '{attr}' não encontrado.")
                logger.info(f"==== Etapa: {step_name} FALHOU ====")
                return None

        logger.info(f"{step_name}: Instanciando CombinationAnalyzer.")
        analyzer_instance = CombinationAnalyzer(all_numbers=config.ALL_NUMBERS)

        if not hasattr(analyzer_instance, 'analyze_frequent_itemsets'):
            logger.error(f"{step_name}: Instância 'CombinationAnalyzer' não tem método 'analyze_frequent_itemsets'.")
            logger.info(f"==== Etapa: {step_name} FALHOU ====")
            return None

        if not hasattr(db_manager, 'save_dataframe'):
            logger.error(f"{step_name}: Objeto 'db_manager' não tem método 'save_dataframe'.")
            logger.info(f"==== Etapa: {step_name} FALHOU ====")
            return None

        min_support = config.APRIORI_MIN_SUPPORT
        min_len = config.FREQUENT_ITEMSETS_MIN_LEN
        max_len = config.FREQUENT_ITEMSETS_MAX_LEN
        drawn_numbers_col = config.DRAWN_NUMBERS_COLUMN_NAME
        frequent_itemsets_table_name = config.FREQUENT_ITEMSETS_TABLE_NAME

        logger.info(f"Parâmetros da análise: min_support={min_support}, min_len={min_len}, max_len={max_len}")

        df_for_db, df_raw_for_rules = analyzer_instance.analyze_frequent_itemsets(
            all_draws_df=all_data_df,
            min_support=min_support,
            min_len=min_len,
            max_len=max_len,
            drawn_numbers_col=drawn_numbers_col
        )

        if not isinstance(df_for_db, pd.DataFrame):
            logger.error(f"{step_name}: analyze_frequent_itemsets não retornou DataFrame para 'df_for_db'.")
            logger.info(f"==== Etapa: {step_name} FALHOU ====")
            return None

        expected_db_cols = ['itemset_str', 'support', 'length', 'frequency_count']
        # Adicionar CONTEST_ID_COLUMN_NAME se sua tabela de itemsets frequentes o espera
        # Esta coluna é adicionada pela função get_frequent_itemsets em combination_analysis se necessário
        # Por isso, não adicionamos aqui, mas sim garantimos que ela exista se a tabela a espera.
        # No entanto, a lógica atual em combination_analysis.analyze_frequent_itemsets NÃO adiciona contest_id.
        # Se a tabela FREQUENT_ITEMSETS_TABLE_NAME precisa de contest_id, a lógica de adição deve estar
        # em combination_analysis.py ou aqui, se for um valor global (ex: último concurso).
        # Para itemsets históricos, a abordagem seria diferente.

        if df_for_db.empty:
            logger.info(f"{step_name}: Nenhum itemset frequente (DB) encontrado. Tabela '{frequent_itemsets_table_name}' vazia.")
            empty_db_df = pd.DataFrame(columns=expected_db_cols)
            db_manager.save_dataframe(empty_db_df, frequent_itemsets_table_name, if_exists='replace')
        else:
            cols_to_save_db = [col for col in expected_db_cols if col in df_for_db.columns]
            missing_expected_cols = [col for col in ['itemset_str', 'support', 'length'] if col not in cols_to_save_db]
            if missing_expected_cols:
                 logger.error(f"{step_name}: df_for_db não contém colunas essenciais: {missing_expected_cols}. Presentes: {df_for_db.columns}")
                 return None

            logger.info(f"{step_name}: Análise (DB) concluída. {len(df_for_db)} itemsets.")
            db_manager.save_dataframe(df_for_db[cols_to_save_db], frequent_itemsets_table_name, if_exists='replace')
            logger.info(f"{step_name}: Resultados (DB) salvos em '{frequent_itemsets_table_name}'.")

        shared_context['frequent_itemsets_df_for_db'] = df_for_db

        if not isinstance(df_raw_for_rules, pd.DataFrame):
            logger.error(f"{step_name}: analyze_frequent_itemsets não retornou DataFrame para 'df_raw_for_rules'.")
            logger.info(f"==== Etapa: {step_name} FALHOU (df_raw_for_rules) ====")
            return None

        if df_raw_for_rules.empty:
            logger.info(f"{step_name}: Nenhum itemset frequente (mlxtend bruto) encontrado.")
        else:
            logger.info(f"{step_name}: {len(df_raw_for_rules)} itemsets (mlxtend bruto) preparados.")

        shared_context['mlxtend_frequent_itemsets_df_for_rules'] = df_raw_for_rules
        logger.info(f"{step_name}: DataFrame (mlxtend bruto) adicionado ao shared_context.")

    except AttributeError as e:
        logger.error(f"Erro na etapa {step_name}: Atributo ausente. Detalhes: {e}", exc_info=True)
        return None
    except ValueError as e:
        logger.error(f"Erro na etapa {step_name}: Dados/parâmetros. Detalhes: {e}", exc_info=True)
        return None
    except Exception as e:
        logger.error(f"Erro inesperado na etapa {step_name}: {e}", exc_info=True)
        return None

    logger.info(f"==== Etapa: {step_name} CONCLUÍDA ====")
    return analyzer_instance