# Lotofacil_Analysis/src/main.py
import argparse
import logging
import sys
import os
import pandas as pd
from typing import List, Dict, Any, Callable

from src.config import config_obj 
from src.data_loader import load_and_clean_data, load_cleaned_data
from src.database_manager import DatabaseManager
from src.orchestrator import Orchestrator

from src.analysis.combination_analysis import CombinationAnalyzer

from src.pipeline_steps.execute_frequency import run_frequency_analysis
from src.pipeline_steps.execute_delay import run_delay_analysis
from src.pipeline_steps.execute_max_delay import run_max_delay_analysis_step
from src.pipeline_steps.execute_pairs import run_pair_analysis_step
from src.pipeline_steps.execute_frequent_itemsets import run_frequent_itemsets_analysis_step
from src.pipeline_steps.execute_frequent_itemset_metrics import run_frequent_itemset_metrics_step
from src.pipeline_steps.execute_cycles import run_cycle_identification_step
from src.pipeline_steps.execute_cycle_stats import run_cycle_stats_step
from src.pipeline_steps.execute_cycle_progression import run_cycle_progression_analysis_step
from src.pipeline_steps.execute_detailed_cycle_metrics import run_detailed_cycle_metrics_step
from src.pipeline_steps.execute_properties import run_number_properties_analysis
from src.pipeline_steps.execute_repetition_analysis import run_repetition_analysis_step
from src.pipeline_steps.execute_chunk_evolution_analysis import run_chunk_evolution_analysis_step
from src.pipeline_steps.execute_block_aggregation import run_block_aggregation_step
from src.pipeline_steps.execute_rank_trend_analysis import run_rank_trend_analysis_step
from src.pipeline_steps.execute_metrics_viz import run_metrics_visualization_step
from src.pipeline_steps.execute_chunk_evolution_visualization import run_chunk_evolution_visualization_step

if config_obj:
    logging.basicConfig(
        level=config_obj.LOG_LEVEL,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(config_obj.LOG_FILE, mode='a'),
            logging.StreamHandler(sys.stdout)
        ]
    )
else:
    # Fallback logging
    logging.basicConfig(level="INFO", format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', handlers=[logging.StreamHandler(sys.stdout)])
    logging.critical("Falha ao carregar config_obj. Usando logging de fallback.")

logger = logging.getLogger(__name__)

def main():
    if not config_obj:
        logger.critical("Objeto de configuração não está disponível. Encerrando a aplicação.")
        return

    parser = argparse.ArgumentParser(description="Aplicativo de Análise da Lotofácil.")
    parser.add_argument(
        "--force-reload", action="store_true",
        help="Força o recarregamento dos dados do arquivo CSV bruto."
    )

    logger.info("Inicializando componentes...")
    main_all_data_df = pd.DataFrame()
    db_manager = None

    try:
        raw_file_path = os.path.join(config_obj.DATA_DIR, config_obj.RAW_DATA_FILE_NAME)
        cleaned_pickle_path = os.path.join(config_obj.DATA_DIR, config_obj.CLEANED_DATA_FILE_NAME)

        args_temp, _ = parser.parse_known_args() # Parse apenas para --force-reload inicialmente
        force_reload_data = args_temp.force_reload
            
        if not force_reload_data:
            logger.info(f"Tentando carregar dados limpos de: {cleaned_pickle_path}")
            main_all_data_df = load_cleaned_data(config_obj.DATA_DIR) 

        if main_all_data_df.empty:
            action_msg = "--force-reload especificado." if force_reload_data else "Dados limpos não encontrados ou vazios."
            logger.info(f"{action_msg} Processando dados brutos de: {raw_file_path}")
            main_all_data_df = load_and_clean_data(raw_file_path, cleaned_pickle_path)

        if main_all_data_df.empty:
            logger.error("Nenhum dado carregado. Verifique os arquivos e configurações.")
            return
        
        logger.info(f"{len(main_all_data_df)} sorteios carregados para análise.")
        
        db_manager = DatabaseManager(db_path=config_obj.DB_PATH)
        combination_analyzer = CombinationAnalyzer(all_numbers=config_obj.ALL_NUMBERS)
        
        shared_context_for_orchestrator: Dict[str, Any] = {
            "config": config_obj,
            "db_manager": db_manager,
            "all_data_df": main_all_data_df, 
            "combination_analyzer": combination_analyzer
        }
        shared_context_for_orchestrator["shared_context"] = shared_context_for_orchestrator

        main_analysis_pipeline_config: List[Dict[str, Any]] = [
            {"name": "frequency_analysis", "func": run_frequency_analysis, "args": ["all_data_df", "db_manager", "config", "shared_context"]},
            {"name": "delay_analysis", "func": run_delay_analysis, "args": ["all_data_df", "db_manager", "config", "shared_context"]},
            {"name": "max_delay_analysis", "func": run_max_delay_analysis_step, "args": ["all_data_df", "db_manager", "config", "shared_context"]},
            {"name": "pair_analysis", "func": run_pair_analysis_step, "args": ["all_data_df", "db_manager", "combination_analyzer", "config", "shared_context"]},
            {"name": "frequent_itemsets_analysis", "func": run_frequent_itemsets_analysis_step, "args": ["all_data_df", "db_manager", "combination_analyzer", "config", "shared_context"]},
            {"name": "frequent_itemset_metrics_analysis", "func": run_frequent_itemset_metrics_step, "args": ["all_data_df", "db_manager", "config", "shared_context"]},
            {"name": "cycle_identification", "func": run_cycle_identification_step, "args": ["all_data_df", "db_manager", "config", "shared_context"]},
            {"name": "cycle_stats", "func": run_cycle_stats_step, "args": ["all_data_df", "db_manager", "config", "shared_context"]},
            {"name": "cycle_progression", "func": run_cycle_progression_analysis_step, "args": ["all_data_df", "db_manager", "config", "shared_context"]},
            {"name": "detailed_cycle_metrics", "func": run_detailed_cycle_metrics_step, "args": ["all_data_df", "db_manager", "config", "shared_context"]},
            {"name": "number_properties", "func": run_number_properties_analysis, "args": ["all_data_df", "db_manager", "config", "shared_context"]},
            {"name": "repetition_analysis", "func": run_repetition_analysis_step, "args": ["all_data_df", "db_manager", "config", "shared_context"]},
            {"name": "chunk_evolution_analysis", "func": run_chunk_evolution_analysis_step, "args": ["all_data_df", "db_manager", "config", "shared_context"]},
            {"name": "block_aggregation", "func": run_block_aggregation_step, "args": ["db_manager", "config", "shared_context"]},
            {"name": "rank_trend_analysis", "func": run_rank_trend_analysis_step, "args": ["db_manager", "config", "shared_context"]},
        ]

        visualization_pipeline_config: List[Dict[str, Any]] = [
             {"name": "metrics_visualization", "func": run_metrics_visualization_step, "args": ["db_manager", "config", "shared_context"]},
             {"name": "chunk_evolution_visualization", "func": run_chunk_evolution_visualization_step, "args": ["db_manager", "config", "shared_context"]}
        ]
        
        all_analysis_step_names = [step_config["name"] for step_config in main_analysis_pipeline_config]
        all_viz_step_names = [step_config["name"] for step_config in visualization_pipeline_config]
        available_steps_for_argparse = ["all_analysis"] + all_analysis_step_names + all_viz_step_names 
        
        parser.add_argument(
            "--run-steps", nargs='+', choices=available_steps_for_argparse,
            help=(f"Execute etapas específicas. Use 'all_analysis' para todas as análises principais. Disponíveis: {', '.join(available_steps_for_argparse)}")
        )
        args = parser.parse_args() # Reparsing para incluir --run-steps

        pipeline_to_run_config: List[Dict[str, Any]] = []
        
        if args.run_steps:
            requested_steps = args.run_steps
            if "all_analysis" in requested_steps:
                pipeline_to_run_config = main_analysis_pipeline_config
            else:
                all_available_step_configs = main_analysis_pipeline_config + visualization_pipeline_config
                for step_name in requested_steps:
                    found_step_config = next((sc for sc in all_available_step_configs if sc["name"] == step_name), None)
                    if found_step_config: pipeline_to_run_config.append(found_step_config)
                    else: logger.warning(f"Step '{step_name}' não reconhecido e será ignorado.")
        elif force_reload_data and not args.run_steps: # Se --force-reload foi a única ação, rodar análises
            logger.info("Opção --force-reload ativada sem --run-steps específico. Executando 'all_analysis'.")
            pipeline_to_run_config = main_analysis_pipeline_config
        else: # Nenhuma ação explícita e não forçou reload
            logger.info("Nenhuma ação especificada (ex: --run-steps ou --force-reload com intenção de recalcular). Use --help para opções.")
            parser.print_help()
            return

        if pipeline_to_run_config:
            logger.info(f"Preparando para executar pipeline com steps: {[s['name'] for s in pipeline_to_run_config]}")
            orchestrator = Orchestrator(pipeline=pipeline_to_run_config, db_manager=db_manager) # Passa db_manager para Orchestrator
            # O shared_context já contém db_manager, config, all_data_df, etc.
            # A lógica do Orchestrator deve ser capaz de injetar os args especificados
            # no pipeline_config a partir do shared_context_for_orchestrator.
            for key, value in shared_context_for_orchestrator.items():
                 orchestrator.set_shared_context(key, value) # Garante que todo o contexto está disponível

            orchestrator.run()
        elif args.run_steps: # Se args.run_steps foi fornecido mas resultou em pipeline vazio (nenhum step válido)
             logger.info("Nenhuma etapa válida selecionada para execução via --run-steps.")
        # Se nem --run-steps nem --force-reload (que implica rodar análises) foi dado, já retornou antes.

    except Exception as e:
        logger.critical(f"Erro fatal na execução principal: {e}", exc_info=True)
    finally:
        if db_manager:
            db_manager.close()
        logger.info("Aplicação finalizada.")

if __name__ == "__main__":
    main()