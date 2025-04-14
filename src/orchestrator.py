# src/orchestrator.py

import argparse
import logging
import pandas as pd
from typing import Optional

# Importações de configuração e DATA HANDLING
from src.config import logger
from src.data_loader import load_and_clean_data
from src.database_manager import save_to_db, read_data_from_db

# Importa as FUNÇÕES DE EXECUÇÃO dos módulos de pipeline steps
from src.pipeline_steps.execute_frequency import execute_frequency_analysis
from src.pipeline_steps.execute_pairs import execute_pair_analysis
from src.pipeline_steps.execute_combinations import execute_combination_analysis
from src.pipeline_steps.execute_cycles import execute_cycle_identification, display_cycle_summary
from src.pipeline_steps.execute_cycle_stats import execute_cycle_stats_analysis
from src.pipeline_steps.execute_delay import execute_delay_analysis
from src.pipeline_steps.execute_max_delay import execute_max_delay_analysis
from src.pipeline_steps.execute_properties import execute_properties_analysis

# Importa o runner do backtester e as estratégias
from src.backtester.runner import run_backtest
from src.strategies.frequency_strategies import select_most_frequent_overall, select_least_frequent_overall

# Verifica se plotagem está disponível
try:
    from src.visualization.plotter import setup_plotting
    setup_plotting()
    PLOTTING_ENABLED = True
except ImportError as e:
    logger.warning(f"Libs de plotagem não encontradas: {e}. Opção --plot ignorada.")
    PLOTTING_ENABLED = False


class AnalysisOrchestrator:
    """ Orquestra o pipeline de análise ou o backtesting. """
    def __init__(self):
        self.logger = logger
        self.args = self._setup_parser().parse_args()
        self.plotting_available = PLOTTING_ENABLED
        self.should_plot = self.args.plot and self.plotting_available
        if self.args.plot and not self.plotting_available:
             self.logger.warning("Opção --plot solicitada, mas libs não encontradas.") # Mudado para warning

    def _setup_parser(self) -> argparse.ArgumentParser:
        parser = argparse.ArgumentParser(description="Analisa dados históricos ou executa backtest de estratégias da Lotofácil.",
                                         formatter_class=argparse.ArgumentDefaultsHelpFormatter)
        # Grupo para argumentos gerais
        general_group = parser.add_argument_group('Opções Gerais')
        general_group.add_argument('--reload', action='store_true', help="Força recarga do Excel.")

        # Grupo para modo Análise
        analysis_group = parser.add_argument_group('Modo Análise')
        analysis_group.add_argument('--max-concurso', type=int, default=None, metavar='NUM', help="Concurso máximo para análise.")
        analysis_group.add_argument('--analysis', nargs='+', choices=['freq','pair','comb','cycle','cycle-stats','delay','max-delay','props','all'], default=None, metavar='TIPO', help="Análises a executar (se não usar --backtest). Padrão 'all' se nenhuma análise for especificada e não for backtest.")
        analysis_group.add_argument('--top-n', type=int, default=15, metavar='N', help="Top N para pares/combinações.")
        analysis_group.add_argument('--windows', type=str, default='10,25,50', metavar='W1,W2,...', help="Janelas de frequência.")
        analysis_group.add_argument('--plot', action='store_true', help="Gera gráficos para análises.")

        # Grupo para modo Backtest
        backtest_group = parser.add_argument_group('Modo Backtest')
        backtest_group.add_argument('--backtest', action='store_true', help="Ativa o modo de backtesting de estratégia.")
        backtest_group.add_argument('--strategy', type=str, choices=['most_freq', 'least_freq'], default='most_freq', help="Estratégia a ser testada.") # Adicionar novas aqui
        backtest_group.add_argument('--start', type=int, default=2000, metavar='CONC_INICIO', help="Concurso inicial para backtest.")
        backtest_group.add_argument('--end', type=int, default=None, metavar='CONC_FIM', help="Concurso final para backtest (padrão: último disponível).")

        return parser

    def _load_or_check_data(self) -> bool:
        # (Código idêntico ao anterior)
        if self.args.reload:
            # ... lógica de reload ...
            cleaned_data = load_and_clean_data()
            if cleaned_data is None: return False
            if not save_to_db(df=cleaned_data, if_exists='replace'): return False
            self.logger.info(f"Dados salvos no BD.")
        else:
            # ... lógica de verificação ...
            test_read = read_data_from_db(columns=['concurso'], concurso_maximo=1)
            if test_read is None or test_read.empty:
                 self.logger.error(f"Não foi possível ler dados do BD. Use --reload?")
                 return False
            self.logger.info("Dados do BD acessíveis.")
        return True

    def _get_last_available_contest(self) -> Optional[int]:
        """ Busca o último concurso disponível no banco de dados. """
        df = read_data_from_db(columns=['concurso'])
        if df is not None and not df.empty:
             max_c = df['concurso'].max()
             return int(max_c) if not pd.isna(max_c) else None
        return None


    def run(self):
        """ Executa o pipeline principal ou o backtesting. """
        self.logger.info(f"Iniciando Lotofacil Analysis - Orquestrador v3 {'(Modo Backtest)' if self.args.backtest else '(Modo Análise)'}")

        if not self._load_or_check_data():
            self.logger.error("Pré-requisitos de dados não atendidos. Encerrando.")
            return

        # --- MODO BACKTEST ---
        if self.args.backtest:
            self.logger.info("Executando em modo Backtest...")

            # Seleciona a função da estratégia com base no argumento
            strategy_map = {
                'most_freq': select_most_frequent_overall,
                'least_freq': select_least_frequent_overall,
                # Adicionar outras estratégias aqui
            }
            selected_strategy_func = strategy_map.get(self.args.strategy)

            if selected_strategy_func is None:
                self.logger.error(f"Estratégia '{self.args.strategy}' não reconhecida.")
                return

            # Define o concurso final se não especificado
            end_contest = self.args.end
            if end_contest is None:
                end_contest = self._get_last_available_contest()
                if end_contest is None:
                    self.logger.error("Não foi possível determinar o último concurso para o backtest.")
                    return
                self.logger.info(f"Concurso final para backtest definido como o último disponível: {end_contest}")

            # Valida o concurso inicial
            start_contest = self.args.start
            if start_contest >= end_contest or start_contest <= 1:
                 self.logger.error(f"Concurso inicial ({start_contest}) inválido para o período até {end_contest}.")
                 return

            # Executa o backtest
            backtest_summary = run_backtest(
                strategy_func=selected_strategy_func,
                strategy_name=self.args.strategy,
                start_contest=start_contest,
                end_contest=end_contest
            )

            # Exibe o resumo do backtest
            if backtest_summary:
                print(f"\n--- Resumo Backtest: Estratégia '{self.args.strategy}' (Concursos {start_contest}-{end_contest}) ---")
                print(f"Acertos 11: {backtest_summary.get(11, 0)} vezes")
                print(f"Acertos 12: {backtest_summary.get(12, 0)} vezes")
                print(f"Acertos 13: {backtest_summary.get(13, 0)} vezes")
                print(f"Acertos 14: {backtest_summary.get(14, 0)} vezes")
                print(f"Acertos 15: {backtest_summary.get(15, 0)} vezes")
                print(f"Abaixo de 11: {backtest_summary.get('<11', 0)} vezes")
                if backtest_summary.get('errors', 0) > 0:
                    print(f"Erros/Falhas: {backtest_summary['errors']} vezes")
            else:
                self.logger.error("Backtest não retornou resultados.")

        # --- MODO ANÁLISE (Default) ---
        else:
            self.logger.info("Executando em modo Análise...")
            # Define 'all' como padrão se nenhuma análise específica foi dada
            analyses_to_run = self.args.analysis if self.args.analysis is not None else ['all']

            run_all = 'all' in analyses_to_run
            run_freq = run_all or 'freq' in analyses_to_run
            run_pair = run_all or 'pair' in analyses_to_run
            run_comb = run_all or 'comb' in analyses_to_run
            run_cycle = run_all or 'cycle' in analyses_to_run
            run_cycle_stats = run_all or 'cycle-stats' in analyses_to_run
            run_delay = run_all or 'delay' in analyses_to_run
            run_max_delay = run_all or 'max-delay' in analyses_to_run
            run_props = run_all or 'props' in analyses_to_run

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