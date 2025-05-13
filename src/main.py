# src/main.py
import logging
import pandas as pd
from pathlib import Path
import argparse # Importar argparse
from typing import List, Optional # Para type hints

# Importar constantes do config.py com os nomes corretos
from src.config import (
    DATA_DIR,
    LOG_DIR_CONFIG,
    PLOT_DIR_CONFIG,
    RAW_DATA_FILE_NAME,
    CLEANED_DATA_FILE_NAME,
    DB_FILE_NAME
)
from src.data_loader import load_and_clean_data, load_cleaned_data
from src.database_manager import DatabaseManager
from src.orchestrator import Orchestrator
import src.pipeline_steps as ps # Importa o módulo todo

# Configuração básica de logging usando as constantes corrigidas
LOG_DIR_CONFIG.mkdir(parents=True, exist_ok=True)
PLOT_DIR_CONFIG.mkdir(parents=True, exist_ok=True) # Garantir que PLOT_DIR_CONFIG também exista
log_file_path = LOG_DIR_CONFIG / "lotofacil_analysis.log"

current_handlers = logging.root.handlers[:]
for handler in current_handlers:
    logging.root.removeHandler(handler)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(log_file_path, mode='a'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Definição do pipeline de análise com "slugs" para nomes
ANALYSIS_PIPELINE = [
    {"name": "frequency-analysis", "func": ps.run_frequency_analysis, "args": ["all_data_df", "db_manager"], "kwargs": {}},
    {"name": "delay-analysis", "func": ps.run_delay_analysis, "args": ["all_data_df", "db_manager"], "kwargs": {}},
    {"name": "number-properties-analysis", "func": ps.run_number_properties_analysis, "args": ["all_data_df", "db_manager"], "kwargs": {}},
    {"name": "repetition-analysis", "func": ps.run_repetition_analysis, "args": ["all_data_df", "db_manager"], "kwargs": {}},
    {"name": "pair-combination-analysis", "func": ps.run_pair_combination_analysis, "args": ["all_data_df", "db_manager"], "kwargs": {}},
    {"name": "cycle-identification-stats", "func": ps.run_cycle_identification_and_stats, "args": ["all_data_df", "db_manager"], "kwargs": {}},
    {"name": "cycle-closing-analysis", "func": ps.run_cycle_closing_analysis, "args": ["all_data_df", "db_manager"], "kwargs": {}},
    {"name": "group-trend-analysis", "func": ps.run_group_trend_analysis, "args": ["all_data_df", "db_manager"], "kwargs": {}},
    {"name": "rank-trend-analysis", "func": ps.run_rank_trend_analysis, "args": ["all_data_df", "db_manager"], "kwargs": {}},
    {
        "name": "chunk-evol-analysis",
        "func": ps.run_chunk_evolution_analysis,
        "args": ["all_data_df", "db_manager"],
        "kwargs": {}
    },
    {
        "name": "chunk-evol-viz",
        "func": ps.run_chunk_evolution_visualization,
        "args": ["db_manager"],
        "kwargs": {"output_dir_from_pipeline": PLOT_DIR_CONFIG} # Passa o diretório de plotagem
    },
    {
        "name": "core-metrics-viz",
        "func": ps.run_core_metrics_visualization,
        "args": ["db_manager"],
        "kwargs": {"output_dir_from_pipeline": PLOT_DIR_CONFIG} # Passa o diretório de plotagem
    },
]

def run_orchestrator_process(
    force_reload_data: bool = False,
    selected_analyses: Optional[List[str]] = None
):
    logger.info("Iniciando o processo de análise da Lotofácil.")

    raw_data_filepath = DATA_DIR / RAW_DATA_FILE_NAME
    cleaned_data_filepath = DATA_DIR / CLEANED_DATA_FILE_NAME
    db_filepath = DATA_DIR / DB_FILE_NAME

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    
    db_manager = DatabaseManager(db_path=str(db_filepath))
    
    all_data_df = None
    if not force_reload_data and cleaned_data_filepath.exists():
        logger.info(f"Carregando dados limpos de: {cleaned_data_filepath}")
        all_data_df = load_cleaned_data(data_dir_path=str(DATA_DIR))
    else:
        logger.info(f"Arquivo de dados limpos '{CLEANED_DATA_FILE_NAME}' não encontrado em '{DATA_DIR}' ou recarregamento forçado.")
        logger.info(f"Processando dados brutos de: {raw_data_filepath}")
        if raw_data_filepath.exists():
            all_data_df = load_and_clean_data(
                raw_file_path=str(raw_data_filepath), 
                cleaned_file_path_to_save=str(cleaned_data_filepath) 
            )
        else:
            logger.error(f"Arquivo de dados brutos '{RAW_DATA_FILE_NAME}' não encontrado em: {raw_data_filepath}. Encerrando.")
            return

    if all_data_df is None or all_data_df.empty:
        logger.error("Não foi possível carregar ou processar os dados da Lotofácil. Encerrando.")
        return

    pipeline_to_run = ANALYSIS_PIPELINE
    if selected_analyses:
        logger.info(f"Executando análises selecionadas: {selected_analyses}")
        
        temp_pipeline = [step for step in ANALYSIS_PIPELINE if step["name"] in selected_analyses]
        
        if temp_pipeline:
            name_to_step = {step["name"]: step for step in temp_pipeline}
            ordered_pipeline = []
            for name in selected_analyses: # Mantém a ordem da linha de comando
                if name in name_to_step:
                    ordered_pipeline.append(name_to_step[name])
            pipeline_to_run = ordered_pipeline
        else:
             pipeline_to_run = temp_pipeline # Lista vazia

        if not pipeline_to_run:
            logger.warning(f"Nenhuma das análises selecionadas ({selected_analyses}) foi encontrada no pipeline principal. Nenhuma etapa será executada.")
        else:
            logger.info(f"Etapas a serem executadas nesta ordem: {[step['name'] for step in pipeline_to_run]}")

    orchestrator = Orchestrator(pipeline=pipeline_to_run, db_manager=db_manager)
    
    # Adiciona contextos que podem ser usados pelas etapas do pipeline
    orchestrator.set_shared_context("all_data_df", all_data_df)
    orchestrator.set_shared_context("db_manager", db_manager)
    # Adicionando PLOT_DIR_CONFIG ao contexto para que as etapas possam usá-lo se precisarem diretamente,
    # embora seja preferível passar via kwargs da definição do pipeline.
    orchestrator.set_shared_context("plot_dir_context", PLOT_DIR_CONFIG)


    if not pipeline_to_run:
        logger.info("Pipeline vazio devido à seleção de análises. Encerrando processo de orquestração.")
        return

    logger.info("Executando o pipeline de análise...")
    orchestrator.run()
    logger.info("Pipeline de análise concluído.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Executa o pipeline de análise da Lotofácil.")
    parser.add_argument(
        "--analysis",
        nargs='+',
        help="Slug(s) da(s) análise(s) específica(s) a serem executadas."
    )
    parser.add_argument(
        "--force-reload",
        action="store_true",
        help="Força o recarregamento e limpeza dos dados brutos."
    )
    args = parser.parse_args()

    run_orchestrator_process(
        force_reload_data=args.force_reload,
        selected_analyses=args.analysis
    )