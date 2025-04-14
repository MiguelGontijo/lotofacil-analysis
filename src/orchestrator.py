# src/orchestrator.py

import argparse
import logging
import pandas as pd
from typing import Optional

# Importações de configuração e DATA HANDLING
from src.config import logger
from src.data_loader import load_and_clean_data
from src.database_manager import save_to_db, read_data_from_db

# Importa as FUNÇÕES DE EXECUÇÃO dos módulos específicos
from src.pipeline_steps.execute_frequency import execute_frequency_analysis
from src.pipeline_steps.execute_pairs import execute_pair_analysis
from src.pipeline_steps.execute_combinations import execute_combination_analysis
from src.pipeline_steps.execute_cycles import execute_cycle_identification, display_cycle_summary
from src.pipeline_steps.execute_cycle_stats import execute_cycle_stats_analysis
from src.pipeline_steps.execute_delay import execute_delay_analysis
from src.pipeline_steps.execute_max_delay import execute_max_delay_analysis
from src.pipeline_steps.execute_properties import execute_properties_analysis

# Verifica se plotagem está disponível
try:
    from src.visualization.plotter import setup_plotting
    setup_plotting()
    PLOTTING_ENABLED = True
except ImportError as e:
    logger.warning(f"Libs de plotagem não encontradas: {e}. Opção --plot ignorada.")
    PLOTTING_ENABLED = False


class AnalysisOrchestrator:
    """
    Orquestra o pipeline de análise da Lotofácil, lidando com argumentos
    e chamando as funções de execução apropriadas.
    """
    def __init__(self):
        self.logger = logger
        self.args = self._setup_parser().parse_args()
        self.plotting_available = PLOTTING_ENABLED
        self.should_plot = self.args.plot and self.plotting_available
        if self.args.plot and not self.plotting_available:
             self.logger.error("Opção --plot solicitada, mas libs não encontradas.")

    def _setup_parser(self) -> argparse.ArgumentParser:
        """ Configura e retorna o ArgumentParser. """
        parser = argparse.ArgumentParser(
            description="Analisa dados históricos da Lotofácil.",
            formatter_class=argparse.ArgumentDefaultsHelpFormatter
        )
        parser.add_argument('--reload', action='store_true', help="Força recarga do Excel.")
        parser.add_argument('--max-concurso', type=int, default=None, metavar='NUMERO', help="Concurso máximo para análise.")
        parser.add_argument(
            '--analysis', nargs='+',
            choices=['freq', 'pair', 'comb', 'cycle', 'cycle-stats', 'delay', 'max-delay', 'props', 'all'],
            default=['all'], metavar='TIPO', help="Análises a executar."
        )
        parser.add_argument('--top-n', type=int, default=15, metavar='N', help="Top N para pares/combinações.")
        parser.add_argument('--windows', type=str, default='10,25,50', metavar='W1,W2,...', help="Janelas de frequência.")
        parser.add_argument('--plot', action='store_true', help="Gera gráficos.")
        return parser

    def _load_or_check_data(self) -> bool:
        """ Lida com a carga de dados ou verificação do BD. """
        if self.args.reload:
            cleaned_data = load_and_clean_data()
            if cleaned_data is None: return False
            if not save_to_db(df=cleaned_data, if_exists='replace'): return False
            self.logger.info(f"Dados salvos no BD.")
        else:
            test_read = read_data_from_db(columns=['concurso'], concurso_maximo=1)
            if test_read is None or test_read.empty:
                 self.logger.error(f"Não foi possível ler dados do BD. Use --reload?")
                 return False
            self.logger.info("Dados do BD acessíveis.")
        return True

    # --- Método Principal de Execução ---
    def run(self):
        """ Executa o pipeline principal de análises. """
        self.logger.info("Iniciando a aplicação Lotofacil Analysis - Orquestrador v3")
        if not self._load_or_check_data():
            self.logger.error("Pré-requisitos de dados não atendidos. Encerrando.")
            return

        run_all = 'all' in self.args.analysis
        run_freq = run_all or 'freq' in self.args.analysis
        run_pair = run_all or 'pair' in self.args.analysis
        run_comb = run_all or 'comb' in self.args.analysis
        run_cycle = run_all or 'cycle' in self.args.analysis
        run_cycle_stats = run_all or 'cycle-stats' in self.args.analysis
        run_delay = run_all or 'delay' in self.args.analysis
        run_max_delay = run_all or 'max-delay' in self.args.analysis
        run_props = run_all or 'props' in self.args.analysis

        if run_freq: execute_frequency_analysis(self.args, self.should_plot)
        if run_pair: execute_pair_analysis(self.args)
        if run_comb: execute_combination_analysis(self.args)

        cycles_summary = None
        if run_cycle or run_cycle_stats:
            cycles_summary = execute_cycle_identification()

        if run_cycle and cycles_summary is not None:
             display_cycle_summary(cycles_summary, self.args, self.should_plot)

        if run_cycle_stats and cycles_summary is not None:
             execute_cycle_stats_analysis(cycles_summary)
        elif run_cycle_stats and cycles_summary is None:
             self.logger.error("Não foi possível executar 'cycle-stats'.")

        if run_delay: execute_delay_analysis(self.args, self.should_plot)
        if run_max_delay: execute_max_delay_analysis(self.args, self.should_plot)
        if run_props: execute_properties_analysis(self.args, self.should_plot)

        self.logger.info("Aplicação Lotofacil Analysis finalizada.")