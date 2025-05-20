# Lotofacil_Analysis/src/pipeline_steps/execute_association_rules.py
import logging
import pandas as pd
from typing import Any, Dict

# Para type hints mais específicos:
from src.analysis.combination_analysis import CombinationAnalyzer
# from src.database_manager import DatabaseManager
# from src.config import Config

logger = logging.getLogger(__name__)

def run_association_rules_step(
    db_manager: Any, # DatabaseManager
    config: Any, # Config
    shared_context: Dict[str, Any],
    combination_analyzer_instance: CombinationAnalyzer, # CORRIGIDO
    **kwargs
) -> bool:
    step_name = "Geração de Regras de Associação"
    logger.info(f"==== Iniciando Etapa: {step_name} ====")

    required_attrs = [
        'ASSOCIATION_RULES_MIN_CONFIDENCE', 'ASSOCIATION_RULES_MIN_LIFT',
        'ASSOCIATION_RULES_TABLE_NAME'
    ]
    for attr in required_attrs:
        if not hasattr(config, attr):
            logger.error(f"{step_name}: Atributo de config '{attr}' não encontrado.")
            return False

    if combination_analyzer_instance is None:
        logger.error(f"{step_name}: Instância 'combination_analyzer_instance' é None.")
        return False

    if not hasattr(combination_analyzer_instance, 'generate_association_rules'):
        logger.error(f"{step_name}: 'combination_analyzer_instance' não possui 'generate_association_rules'.")
        return False

    if not hasattr(db_manager, 'save_dataframe'):
        logger.error(f"{step_name}: Objeto 'db_manager' não possui o método 'save_dataframe'.")
        return False

    mlxtend_frequent_itemsets_df = shared_context.get('mlxtend_frequent_itemsets_df_for_rules')
    
    empty_rules_cols = ['antecedents_str', 'consequents_str', 'antecedent support', 
                        'consequent support', 'support', 'confidence', 'lift', 
                        'leverage', 'conviction']

    if not isinstance(mlxtend_frequent_itemsets_df, pd.DataFrame) or mlxtend_frequent_itemsets_df.empty:
        logger.warning(f"{step_name}: 'mlxtend_frequent_itemsets_df_for_rules' não encontrado/vazio. Nenhuma regra gerada.")
        db_manager.save_dataframe(pd.DataFrame(columns=empty_rules_cols), config.ASSOCIATION_RULES_TABLE_NAME, if_exists='replace')
        shared_context['association_rules_df'] = pd.DataFrame(columns=empty_rules_cols)
        return True

    min_confidence = config.ASSOCIATION_RULES_MIN_CONFIDENCE
    min_lift = config.ASSOCIATION_RULES_MIN_LIFT
    association_rules_table_name = config.ASSOCIATION_RULES_TABLE_NAME

    try:
        logger.info(f"Gerando regras com min_confidence={min_confidence}, min_lift={min_lift}...")

        rules_df = combination_analyzer_instance.generate_association_rules(
            frequent_itemsets_mlxtend_df=mlxtend_frequent_itemsets_df,
            metric="confidence",
            min_threshold=min_confidence,
            min_lift=min_lift
        )

        if not isinstance(rules_df, pd.DataFrame):
            logger.error(f"{step_name}: Geração de regras não retornou um DataFrame.")
            db_manager.save_dataframe(pd.DataFrame(columns=empty_rules_cols), association_rules_table_name, if_exists='replace')
            shared_context['association_rules_df'] = pd.DataFrame(columns=empty_rules_cols)
            return False

        if rules_df.empty:
            logger.info(f"{step_name}: Nenhuma regra gerada. Tabela '{association_rules_table_name}' vazia.")
            db_manager.save_dataframe(pd.DataFrame(columns=empty_rules_cols), association_rules_table_name, if_exists='replace')
        else:
            final_rules_df = rules_df[[col for col in empty_rules_cols if col in rules_df.columns]].copy()
            db_manager.save_dataframe(final_rules_df,
                                      association_rules_table_name,
                                      if_exists='replace')
            logger.info(f"{len(final_rules_df)} regras de associação salvas em: {association_rules_table_name}")
        
        shared_context['association_rules_df'] = rules_df if not rules_df.empty else pd.DataFrame(columns=empty_rules_cols)
        logger.info(f"Resultado de {step_name} adicionado ao shared_context.")

    except Exception as e:
        logger.error(f"Erro durante a execução da {step_name}: {e}", exc_info=True)
        db_manager.save_dataframe(pd.DataFrame(columns=empty_rules_cols), association_rules_table_name, if_exists='replace')
        shared_context['association_rules_df'] = pd.DataFrame(columns=empty_rules_cols)
        return False

    logger.info(f"==== Etapa: {step_name} CONCLUÍDA ====")
    return True