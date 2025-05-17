# src/pipeline_steps/execute_association_rules.py
import logging
import pandas as pd
from typing import Any, Dict

logger = logging.getLogger(__name__)

def run_association_rules_step(
    db_manager: Any,
    combination_analyzer: Any,
    config: Any,
    shared_context: Dict[str, Any],
    **kwargs
) -> bool:
    step_name = "Geração de Regras de Associação"
    logger.info(f"==== Iniciando Etapa: {step_name} ====")

    if not hasattr(config, 'ASSOCIATION_RULES_MIN_CONFIDENCE') or \
       not hasattr(config, 'ASSOCIATION_RULES_MIN_LIFT') or \
       not hasattr(config, 'ASSOCIATION_RULES_TABLE_NAME'):
        logger.error("Atributos de configuração necessários para as Regras de Associação não encontrados.")
        logger.info(f"==== Etapa: {step_name} FALHOU ====")
        return False

    if not hasattr(combination_analyzer, 'generate_association_rules'):
        logger.error("Objeto 'combination_analyzer' não possui o método 'generate_association_rules'.")
        logger.info(f"==== Etapa: {step_name} FALHOU ====")
        return False
            
    if not hasattr(db_manager, 'save_dataframe'):
        logger.error("Objeto 'db_manager' não possui o método 'save_dataframe'.")
        logger.info(f"==== Etapa: {step_name} FALHOU ====")
        return False

    # CORREÇÃO AQUI: Usar a chave correta para buscar o DataFrame
    mlxtend_frequent_itemsets_df = shared_context.get('mlxtend_frequent_itemsets_df_for_rules')

    if not isinstance(mlxtend_frequent_itemsets_df, pd.DataFrame) or mlxtend_frequent_itemsets_df.empty:
        logger.warning("DataFrame 'mlxtend_frequent_itemsets_df_for_rules' não encontrado ou vazio no shared_context. Nenhuma regra será gerada.")
        logger.info(f"==== Etapa: {step_name} CONCLUÍDA (sem itemsets para processar) ====")
        shared_context['association_rules_df'] = pd.DataFrame()
        return True

    min_confidence = config.ASSOCIATION_RULES_MIN_CONFIDENCE
    min_lift = config.ASSOCIATION_RULES_MIN_LIFT
    association_rules_table_name = config.ASSOCIATION_RULES_TABLE_NAME

    try:
        logger.info(f"Gerando regras de associação com min_confidence={min_confidence}, min_lift={min_lift}...")
        
        rules_df = combination_analyzer.generate_association_rules(
            frequent_itemsets_mlxtend_df=mlxtend_frequent_itemsets_df,
            metric="confidence",
            min_threshold=min_confidence,
            min_lift=min_lift
        )

        if not isinstance(rules_df, pd.DataFrame):
            logger.error("A geração de regras de associação não retornou um DataFrame.")
            logger.info(f"==== Etapa: {step_name} FALHOU ====")
            return False
        
        if rules_df.empty:
            logger.info(f"Nenhuma regra de associação gerada com os critérios especificados. Nada será salvo na tabela '{association_rules_table_name}'.")
        else:
            db_manager.save_dataframe(rules_df, 
                                      association_rules_table_name, 
                                      if_exists='replace')
            logger.info(f"{len(rules_df)} regras de associação salvas na tabela: {association_rules_table_name}")
        
        shared_context['association_rules_df'] = rules_df
        logger.info(f"Resultado da {step_name} adicionado ao dicionário shared_context como 'association_rules_df'.")

    except Exception as e:
        logger.error(f"Erro durante a execução da {step_name}: {e}", exc_info=True)
        logger.info(f"==== Etapa: {step_name} FALHOU ====")
        return False
            
    logger.info(f"==== Etapa: {step_name} CONCLUÍDA ====")
    return True