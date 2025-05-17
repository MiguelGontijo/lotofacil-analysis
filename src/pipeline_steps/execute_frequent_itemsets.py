# Lotofacil_Analysis/src/pipeline_steps/execute_frequent_itemsets.py
import logging
import pandas as pd
from typing import Dict, Any

# Não precisamos importar CombinationAnalyzer, Config, etc., diretamente aqui
# pois eles são injetados pelo Orchestrator como argumentos na função do step.

logger = logging.getLogger(__name__)

def run_frequent_itemsets_analysis_step(
    all_data_df: pd.DataFrame, 
    db_manager: Any, # Espera-se DatabaseManager
    combination_analyzer: Any, # Espera-se CombinationAnalyzer
    config: Any, # Espera-se Config
    shared_context: Dict[str, Any], # Dicionário de contexto compartilhado
    **kwargs 
) -> bool: # Padronizado para retornar bool
    step_name = "Frequent Itemsets Analysis"
    logger.info(f"==== Iniciando Etapa: {step_name} ====")

    try:
        # Validações básicas dos objetos injetados e atributos de config
        if not hasattr(config, 'APRIORI_MIN_SUPPORT') or \
           not hasattr(config, 'FREQUENT_ITEMSETS_MIN_LEN') or \
           not hasattr(config, 'FREQUENT_ITEMSETS_MAX_LEN') or \
           not hasattr(config, 'DRAWN_NUMBERS_COLUMN_NAME') or \
           not hasattr(config, 'FREQUENT_ITEMSETS_TABLE_NAME'): # Adicionada verificação para nome da tabela
            logger.error("Atributos de configuração necessários para a análise de itemsets frequentes não encontrados.")
            logger.info(f"==== Etapa: {step_name} FALHOU ====")
            return False

        if not hasattr(combination_analyzer, 'analyze_frequent_itemsets'):
            logger.error("Objeto 'combination_analyzer' não possui o método 'analyze_frequent_itemsets'.")
            logger.info(f"==== Etapa: {step_name} FALHOU ====")
            return False
            
        if not hasattr(db_manager, 'save_dataframe'):
            logger.error("Objeto 'db_manager' não possui o método 'save_dataframe'.")
            logger.info(f"==== Etapa: {step_name} FALHOU ====")
            return False

        min_support = config.APRIORI_MIN_SUPPORT
        min_len = config.FREQUENT_ITEMSETS_MIN_LEN
        max_len = config.FREQUENT_ITEMSETS_MAX_LEN
        drawn_numbers_col = config.DRAWN_NUMBERS_COLUMN_NAME
        frequent_itemsets_table_name = config.FREQUENT_ITEMSETS_TABLE_NAME
        
        logger.info(f"Parâmetros da análise: min_support={min_support}, min_len={min_len}, max_len={max_len}")

        # O método analyze_frequent_itemsets agora retorna dois DataFrames:
        # 1. df_for_db: Formatado para salvar no banco de dados (com 'itemset_str').
        # 2. df_raw_for_rules: Contém 'itemsets' como frozensets, para gerar regras de associação.
        df_for_db, df_raw_for_rules = combination_analyzer.analyze_frequent_itemsets(
            all_draws_df=all_data_df,
            min_support=min_support,
            min_len=min_len,
            max_len=max_len,
            drawn_numbers_col=drawn_numbers_col
        )

        # 1. Salvar o DataFrame formatado para o banco de dados
        if not isinstance(df_for_db, pd.DataFrame): # Checagem de tipo
            logger.error("analyze_frequent_itemsets não retornou um DataFrame válido para 'df_for_db'.")
            logger.info(f"==== Etapa: {step_name} FALHOU ====")
            return False

        if df_for_db.empty:
            logger.info(f"Nenhum itemset frequente (formato DB) encontrado ou retornado pela análise para a etapa {step_name}. Nada a salvar na tabela '{frequent_itemsets_table_name}'.")
        else:
            logger.info(f"Análise de itemsets frequentes (formato DB) concluída. {len(df_for_db)} itemsets encontrados.")
            db_manager.save_dataframe(df_for_db, frequent_itemsets_table_name, if_exists='replace')
            logger.info(f"Resultados da análise de itemsets frequentes (formato DB) salvos na tabela '{frequent_itemsets_table_name}'.")
        
        # Adicionar o DataFrame formatado para DB ao shared_context também, caso seja útil
        shared_context['frequent_itemsets_df_for_db'] = df_for_db

        # 2. Adicionar o DataFrame bruto (para geração de regras) ao shared_context
        if not isinstance(df_raw_for_rules, pd.DataFrame): # Checagem de tipo
            logger.error("analyze_frequent_itemsets não retornou um DataFrame válido para 'df_raw_for_rules'.")
            # Ainda assim, a parte de salvar df_for_db pode ter tido sucesso, então não necessariamente falha a etapa inteira aqui.
            # Mas é um problema para a etapa de regras de associação.
            # Por segurança, vamos considerar uma falha se o contrato não for cumprido.
            logger.info(f"==== Etapa: {step_name} FALHOU (formato inesperado para df_raw_for_rules) ====")
            return False

        if df_raw_for_rules.empty:
            logger.info(f"Nenhum itemset frequente (formato mlxtend bruto para regras) encontrado ou retornado.")
        else:
            logger.info(f"{len(df_raw_for_rules)} itemsets (formato mlxtend bruto para regras) preparados.")
        
        # Chave no shared_context para o DataFrame bruto (com frozensets)
        shared_context['mlxtend_frequent_itemsets_df_for_rules'] = df_raw_for_rules 
        logger.info("DataFrame de itemsets frequentes (formato mlxtend bruto para regras) adicionado ao shared_context.")
        
    except AttributeError as e:
        logger.error(f"Erro na etapa {step_name}: Atributo de configuração ausente. Detalhes: {e}", exc_info=True)
        logger.info(f"==== Etapa: {step_name} FALHOU ====")
        return False
    except ValueError as e:
        logger.error(f"Erro na etapa {step_name}: Problema com os dados ou parâmetros. Detalhes: {e}", exc_info=True)
        logger.info(f"==== Etapa: {step_name} FALHOU ====")
        return False
    except Exception as e:
        logger.error(f"Erro inesperado ao executar a etapa {step_name}: {e}", exc_info=True)
        logger.info(f"==== Etapa: {step_name} FALHOU ====")
        return False

    logger.info(f"==== Etapa: {step_name} CONCLUÍDA ====")
    return True