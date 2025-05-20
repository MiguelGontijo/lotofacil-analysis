# src/pipeline_steps/execute_cycle_closing_propensity.py
import pandas as pd
import logging
from typing import Any, Dict, Optional
from sklearn.preprocessing import MinMaxScaler

# Importa as funções corrigidas do módulo de análise
from src.analysis.cycle_closing_analysis import calculate_closing_number_stats, get_cycles_df_corrected
from src.config import Config # Para type hinting
from src.database_manager import DatabaseManager # Para type hinting

logger = logging.getLogger(__name__)

def run_cycle_closing_propensity_analysis(
    db_manager: DatabaseManager,
    config: Config,
    shared_context: Dict[str, Any],
    # cycles_detail_df é esperado do contexto, produzido por uma etapa anterior
    cycles_detail_df: Optional[pd.DataFrame] = None, 
    **kwargs 
) -> bool:
    step_name = "Cycle Closing Propensity Analysis"
    logger.info(f"==== Iniciando Etapa: {step_name} ====")

    required_attrs = [
        'ANALYSIS_CYCLES_DETAIL_TABLE_NAME', 'ANALYSIS_CYCLE_CLOSING_PROPENSITY_TABLE_NAME',
        'ALL_NUMBERS', 'DEZENA_COLUMN_NAME', 'CYCLE_CLOSING_SCORE_COLUMN_NAME',
        'CICLO_NUM_COLUMN_NAME' # Usado por get_cycles_df_corrected
    ]
    for attr in required_attrs:
        if not hasattr(config, attr):
            logger.error(f"{step_name}: Atributo de configuração '{attr}' não encontrado. Abortando.")
            return False

    try:
        current_cycles_df: Optional[pd.DataFrame] = None
        if cycles_detail_df is not None and not cycles_detail_df.empty:
            logger.info(f"{step_name}: Usando 'cycles_detail_df' fornecido pelo contexto ({len(cycles_detail_df)} linhas).")
            current_cycles_df = cycles_detail_df
        else:
            logger.info(f"{step_name}: 'cycles_detail_df' não fornecido ou vazio no contexto. Carregando da DB.")
            # get_cycles_df_corrected agora usa db_manager e config
            current_cycles_df = get_cycles_df_corrected(db_manager, config) 
        
        if current_cycles_df is None or current_cycles_df.empty:
            logger.warning(f"Nenhum dado de detalhe de ciclos disponível. Não é possível calcular propensão.")
            # Cria e salva uma tabela default para o AnalysisAggregator não quebrar
            propensity_df_default = pd.DataFrame({
                config.DEZENA_COLUMN_NAME: config.ALL_NUMBERS, 
                config.CYCLE_CLOSING_SCORE_COLUMN_NAME: 0.0
            })
            table_name_default = config.ANALYSIS_CYCLE_CLOSING_PROPENSITY_TABLE_NAME
            db_manager.save_dataframe(propensity_df_default, table_name_default, if_exists='replace')
            logger.info(f"Tabela '{table_name_default}' salva com defaults (sem dados de ciclo).")
            return True # Considera sucesso, pois não havia dados para processar

        logger.debug("Chamando calculate_closing_number_stats...")
        # Passa db_manager e config para a função de análise
        closing_stats_df = calculate_closing_number_stats(db_manager, config, current_cycles_df)

        # Nome da coluna de score como será salva no banco (antes do alias do Aggregator)
        score_col_in_db = config.CYCLE_CLOSING_SCORE_COLUMN_NAME # Deve ser "score"

        if closing_stats_df is None or closing_stats_df.empty:
            logger.info("Nenhuma estatística de fechamento de ciclo calculada. Scores serão 0.")
            propensity_df_final = pd.DataFrame({config.DEZENA_COLUMN_NAME: config.ALL_NUMBERS, score_col_in_db: 0.0})
        else:
            propensity_df = closing_stats_df.reset_index() # 'dezena' vira coluna
            
            if config.DEZENA_COLUMN_NAME not in propensity_df.columns:
                logger.error(f"Coluna '{config.DEZENA_COLUMN_NAME}' não encontrada após reset_index. Verifique a saída de calculate_closing_number_stats.")
                return False
            
            if 'closing_freq' not in propensity_df.columns:
                logger.warning("Coluna 'closing_freq' não encontrada nas estatísticas. Scores serão 0.")
                propensity_df[score_col_in_db] = 0.0
            else:
                propensity_df_to_score = propensity_df[[config.DEZENA_COLUMN_NAME, 'closing_freq']].copy()
                propensity_df_to_score['closing_freq'].fillna(0, inplace=True) # Trata NaNs antes de escalar
                
                scaler = MinMaxScaler()
                if propensity_df_to_score.empty or propensity_df_to_score['closing_freq'].sum() == 0: # Se todos os valores são zero ou df vazio
                    propensity_df_to_score[score_col_in_db] = 0.0
                elif propensity_df_to_score['closing_freq'].nunique() > 1:
                    propensity_df_to_score[score_col_in_db] = scaler.fit_transform(propensity_df_to_score[['closing_freq']])
                else: # Todos os valores são iguais e não zero
                    propensity_df_to_score[score_col_in_db] = 0.5 
                
            # Garante que todas as dezenas estejam presentes e seleciona colunas finais
            all_dezenas_df = pd.DataFrame({config.DEZENA_COLUMN_NAME: config.ALL_NUMBERS})
            propensity_df_final = pd.merge(all_dezenas_df, propensity_df_to_score[[config.DEZENA_COLUMN_NAME, score_col_in_db]], 
                                           on=config.DEZENA_COLUMN_NAME, how='left').fillna({score_col_in_db: 0.0})

        table_name_to_save = config.ANALYSIS_CYCLE_CLOSING_PROPENSITY_TABLE_NAME
        db_manager.save_dataframe(propensity_df_final[[config.DEZENA_COLUMN_NAME, score_col_in_db]], table_name_to_save, if_exists='replace')
        logger.info(f"Scores de propensão de fechamento ({len(propensity_df_final)} dezenas) salvos em '{table_name_to_save}'.")

        logger.info(f"==== Etapa: {step_name} CONCLUÍDA ====")
        return True

    except AttributeError as ae: # Para erros de config não encontrado
        logger.error(f"Erro de atributo em {step_name} (verifique constantes em config): {ae}", exc_info=True)
        return False
    except Exception as e:
        logger.error(f"Erro durante a execução da {step_name}: {e}", exc_info=True)
        return False