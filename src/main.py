# Lotofacil_Analysis/src/main.py
import argparse
import logging
import sys
import os
import pandas as pd
from typing import List, Dict, Any, Callable, Optional

# Importar Config e DatabaseManager PRIMEIRO
from src.config import config_obj, Config 
from src.database_manager import DatabaseManager

from src.data_loader import load_and_clean_data, load_cleaned_data # Funções do seu data_loader.py
from src.orchestrator import Orchestrator

# --- Importações das Funções de Etapa do Pipeline ---
# (Suas importações de execute_*.py permanecem aqui)
from src.pipeline_steps.execute_frequency import run_frequency_analysis
from src.pipeline_steps.execute_delay import run_delay_analysis
# ... (todas as suas outras importações de pipeline_steps) ...
from src.pipeline_steps.execute_max_delay import run_max_delay_analysis_step
from src.pipeline_steps.execute_positional_analysis import run_positional_analysis_step
from src.pipeline_steps.execute_recurrence_analysis import run_recurrence_analysis_step
from src.pipeline_steps.execute_grid_analysis import run_grid_analysis_step
from src.pipeline_steps.execute_statistical_tests import run_statistical_tests_step
from src.pipeline_steps.execute_seasonality_analysis import run_seasonality_analysis_step
from src.pipeline_steps.execute_frequent_itemsets import run_frequent_itemsets_analysis_step
from src.pipeline_steps.execute_pairs import run_pair_analysis_step
from src.pipeline_steps.execute_association_rules import run_association_rules_step
from src.pipeline_steps.execute_frequent_itemset_metrics import run_frequent_itemset_metrics_step
from src.pipeline_steps.execute_properties import run_number_properties_analysis
from src.pipeline_steps.execute_sequence_analysis import run_sequence_analysis_step
from src.pipeline_steps.execute_cycles import run_cycle_identification_step
from src.pipeline_steps.execute_cycle_stats import run_cycle_stats_step
from src.pipeline_steps.execute_cycle_progression import run_cycle_progression_analysis_step
from src.pipeline_steps.execute_cycle_closing_propensity import run_cycle_closing_propensity_analysis
from src.pipeline_steps.execute_detailed_cycle_metrics import run_detailed_cycle_metrics_step
from src.pipeline_steps.execute_repetition_analysis import run_repetition_analysis_step
from src.pipeline_steps.execute_temporal_trend_analysis import run_temporal_trend_analysis_step
from src.pipeline_steps.execute_chunk_evolution_analysis import run_chunk_evolution_analysis_step
from src.pipeline_steps.execute_block_aggregation import run_block_aggregation_step
from src.pipeline_steps.execute_rank_trend_analysis import run_rank_trend_analysis_step


# Configuração de Logging (como na sua versão mais recente)
if config_obj:
    logging.basicConfig(
        level=config_obj.LOG_LEVEL,
        format='%(asctime)s - %(name)s - %(levelname)s - [%(module)s.%(funcName)s:%(lineno)d] - %(message)s',
        handlers=[
            logging.FileHandler(config_obj.LOG_FILE, mode='w', encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ],
        force=True
    )
else:
    logging.basicConfig(
        level="INFO", 
        format='%(asctime)s - %(name)s - %(levelname)s - [%(module)s.%(funcName)s:%(lineno)d] - %(message)s',
        handlers=[logging.StreamHandler(sys.stdout)]
    )
    logging.critical("Falha ao carregar config_obj. Usando logging de fallback.")

logger = logging.getLogger(__name__)

pd.set_option('display.max_columns', None)
pd.set_option('display.width', 200)
pd.set_option('display.max_colwidth', 80)


def main(cmd_args: argparse.Namespace):
    if not config_obj:
        logger.critical("Objeto de configuração não está disponível. Encerrando a aplicação.")
        return

    logger.info(f"Aplicação Lotofacil Analysis iniciada com argumentos: {cmd_args}")
    
    all_data_df: Optional[pd.DataFrame] = None

    try:
        # Caminhos para os arquivos de dados, usando config_obj
        raw_file_full_path = config_obj.HISTORICO_CSV_PATH
        cleaned_pickle_full_path = config_obj.CLEANED_DATA_PATH

        if not cmd_args.force_reload:
            logger.info(f"Tentando carregar dados limpos de: {cleaned_pickle_full_path}")
            # A função load_cleaned_data no seu data_loader.py espera data_dir_path
            all_data_df = load_cleaned_data(config_obj.DATA_DIR) 
        
        if all_data_df is None or all_data_df.empty or cmd_args.force_reload:
            action_msg = "--force-reload especificado." if cmd_args.force_reload else "Dados limpos não encontrados ou vazios."
            logger.info(f"{action_msg} Processando dados brutos de: {raw_file_full_path}")
            
            # ***** CORREÇÃO DA CHAMADA load_and_clean_data *****
            # A função espera: load_and_clean_data(raw_file_path: str, cleaned_file_path_to_save: str)
            all_data_df = load_and_clean_data(
                raw_file_path=raw_file_full_path, 
                cleaned_file_path_to_save=cleaned_pickle_full_path
            )
            # O terceiro argumento 'config=config_obj' foi removido pois não é esperado pela função.

        if all_data_df is None or all_data_df.empty:
            logger.error("Nenhum dado carregado. Verifique os arquivos de dados e as configurações. Encerrando.")
            return
        
        logger.info(f"{len(all_data_df)} sorteios carregados para análise.")
        
        # --- Definição do Pipeline de Análise ---
        # (A definição do PIPELINE_CONFIG que você tem aqui permanece a mesma por enquanto)
        # Certifique-se que as assinaturas das funções run_*_step correspondam aos "args"
        default_step_args = ["all_data_df", "db_manager", "config", "shared_context"]
        db_config_shared_args = ["db_manager", "config", "shared_context"]

        main_analysis_pipeline_config: List[Dict[str, Any]] = [
            {"name": "frequency_analysis", "func": run_frequency_analysis, "args": default_step_args},
            {"name": "delay_analysis", "func": run_delay_analysis, "args": default_step_args},
            # {"name": "max_delay_analysis", "func": run_max_delay_analysis_step, "args": default_step_args},
            {"name": "positional_analysis", "func": run_positional_analysis_step, "args": default_step_args},
            {"name": "recurrence_analysis", "func": run_recurrence_analysis_step, "args": default_step_args},
            {"name": "grid_analysis", "func": run_grid_analysis_step, "args": default_step_args},
            {"name": "statistical_tests", "func": run_statistical_tests_step, "args": default_step_args},
            {"name": "seasonality_analysis", "func": run_seasonality_analysis_step, "args": default_step_args},
            
            {"name": "frequent_itemsets_analysis", "func": run_frequent_itemsets_analysis_step, 
             "args": default_step_args, 
             "output_key": "combination_analyzer_instance"},
            
            {"name": "pair_analysis", "func": run_pair_analysis_step, 
             "args": default_step_args + ["combination_analyzer_instance"]},
            
            {"name": "association_rules", "func": run_association_rules_step, 
             "args": db_config_shared_args + ["combination_analyzer_instance"]},

            {"name": "frequent_itemset_metrics_analysis", "func": run_frequent_itemset_metrics_step, "args": default_step_args},
            {"name": "number_properties", "func": run_number_properties_analysis, "args": default_step_args},
            {"name": "sequence_analysis", "func": run_sequence_analysis_step, "args": default_step_args},
            
            {"name": "cycle_identification", "func": run_cycle_identification_step, 
             "args": default_step_args, "output_key": "cycles_detail_df"}, 
            
            {"name": "cycle_stats", "func": run_cycle_stats_step, 
             "args": ["all_data_df", "db_manager", "config", "shared_context"]},
            
            {"name": "cycle_progression", "func": run_cycle_progression_analysis_step, "args": default_step_args},
            
            {"name": "cycle_closing_propensity", "func": run_cycle_closing_propensity_analysis, 
             "args": db_config_shared_args + ["cycles_detail_df"]}, 

            {"name": "detailed_cycle_metrics", "func": run_detailed_cycle_metrics_step, 
             "args": default_step_args + ["cycles_detail_df"]},

            {"name": "repetition_analysis", "func": run_repetition_analysis_step, "args": default_step_args},
            {"name": "temporal_trend_analysis", "func": run_temporal_trend_analysis_step, "args": default_step_args},
            {"name": "chunk_evolution_analysis", "func": run_chunk_evolution_analysis_step, "args": default_step_args},
            {"name": "block_aggregation", "func": run_block_aggregation_step, "args": db_config_shared_args}, 
            {"name": "rank_trend_analysis", "func": run_rank_trend_analysis_step, "args": db_config_shared_args + ["all_data_df"]},
        ]
        
        pipeline_to_run_actual: List[Dict[str, Any]] = []
        if cmd_args.run_steps:
            if "all_analysis" in cmd_args.run_steps:
                pipeline_to_run_actual = main_analysis_pipeline_config
            else:
                for step_name in cmd_args.run_steps:
                    found_step = next((s for s in main_analysis_pipeline_config if s["name"] == step_name), None)
                    if found_step:
                        if found_step not in pipeline_to_run_actual:
                           pipeline_to_run_actual.append(found_step)
                    else:
                        logger.warning(f"Etapa solicitada '{step_name}' não encontrada na configuração principal. Será ignorada.")
        elif cmd_args.force_reload: 
            logger.info("Opção --force-reload ativada. Executando todas as etapas de análise ('all_analysis').")
            pipeline_to_run_actual = main_analysis_pipeline_config
        
        if not pipeline_to_run_actual and not cmd_args.run_strategy_flow :
             logger.info("Nenhuma etapa de pipeline solicitada para execução. Use --run-steps all_analysis ou --force-reload.")
        elif pipeline_to_run_actual:
            with DatabaseManager(db_path=config_obj.DB_PATH) as db_m:
                logger.info(f"Executando pipeline com etapas: {[s['name'] for s in pipeline_to_run_actual]}")
                orchestrator = Orchestrator(pipeline=pipeline_to_run_actual, db_manager=db_m)

                orchestrator.set_shared_context('all_data_df', all_data_df)
                orchestrator.set_shared_context('config', config_obj)
                orchestrator.set_shared_context('shared_context', orchestrator.shared_context) 

                logger.info("Verificando e criando estrutura do banco de dados...")
                db_m._create_all_tables() 

                logger.info(f"Iniciando o Orchestrator. Contexto inicial: {list(orchestrator.shared_context.keys())}")
                orchestrator.run()
        
    except FileNotFoundError as fnf_error:
        logger.critical(f"Erro de arquivo não encontrado: {fnf_error}.", exc_info=True)
    except KeyError as key_error:
        logger.critical(f"Erro de chave não encontrada (KeyError): {key_error}", exc_info=True)
    except Exception as e:
        logger.critical(f"Erro fatal na execução principal: {e}", exc_info=True)
    finally:
        logger.info("Aplicação principal finalizada.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Aplicativo de Análise da Lotofácil.")
    parser.add_argument("--force-reload", action="store_true", help="Força o recarregamento dos dados do arquivo CSV bruto.")
    parser.add_argument("--run-steps", nargs='*', help="Execute etapas específicas (ou 'all_analysis'). Ex: --run-steps frequency_analysis delay_analysis")
    parser.add_argument("--run-strategy-flow", action="store_true", help="Executa o fluxo de agregação e teste de estratégias.")
    
    parsed_args = parser.parse_args()
    
    if parsed_args.run_steps is not None and not parsed_args.run_steps: 
        logger.warning("--run-steps foi especificado sem nenhuma etapa. Nenhuma etapa de análise será executada. Forneça nomes de etapas ou 'all_analysis'.")

    main(cmd_args=parsed_args)