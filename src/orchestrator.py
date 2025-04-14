# src/orchestrator.py

import argparse
import logging
import pandas as pd
from typing import Optional, Callable, Set # Adicionado Callable, Set

# Importações de config e data handling
from src.config import logger
from src.data_loader import load_and_clean_data
from src.database_manager import save_to_db, read_data_from_db, get_draw_numbers # get_draw_numbers importado aqui também

# Importa as FUNÇÕES DE EXECUÇÃO dos pipeline steps
from src.pipeline_steps.execute_frequency import execute_frequency_analysis
from src.pipeline_steps.execute_pairs import execute_pair_analysis
from src.pipeline_steps.execute_combinations import execute_combination_analysis
from src.pipeline_steps.execute_cycles import execute_cycle_identification, display_cycle_summary
from src.pipeline_steps.execute_cycle_stats import execute_cycle_stats_analysis
from src.pipeline_steps.execute_delay import execute_delay_analysis
from src.pipeline_steps.execute_max_delay import execute_max_delay_analysis
from src.pipeline_steps.execute_properties import execute_properties_analysis

# Importa o runner do backtester
from src.backtester.runner import run_backtest
# Importa TODAS as funções de estratégia disponíveis
from src.strategies.frequency_strategies import (
    select_most_frequent_overall,
    select_least_frequent_overall,
    select_most_frequent_recent # <<< Nova estratégia de frequência
)
from src.strategies.delay_strategies import select_most_delayed # <<< Nova estratégia de atraso

# Verifica plotagem
try:
    from src.visualization.plotter import setup_plotting
    setup_plotting()
    PLOTTING_ENABLED = True
except ImportError as e:
    logger.warning(f"Libs de plotagem não encontradas: {e}. Opção --plot ignorada.")
    PLOTTING_ENABLED = False


# Define o tipo para a função da estratégia para clareza
StrategyFuncType = Callable[[int], Optional[Set[int]]]


class AnalysisOrchestrator:
    """ Orquestra análise ou backtesting. """
    def __init__(self):
        self.logger = logger
        self.args = self._setup_parser().parse_args()
        self.plotting_available = PLOTTING_ENABLED
        self.should_plot = self.args.plot and self.plotting_available
        if self.args.plot and not self.plotting_available:
             self.logger.warning("Opção --plot solicitada, mas libs não encontradas.")

    def _setup_parser(self) -> argparse.ArgumentParser:
        parser = argparse.ArgumentParser(description="Analisa ou faz backtest da Lotofácil.",
                                         formatter_class=argparse.ArgumentDefaultsHelpFormatter)
        general_group = parser.add_argument_group('Opções Gerais')
        general_group.add_argument('--reload', action='store_true', help="Força recarga do Excel.")

        analysis_group = parser.add_argument_group('Modo Análise')
        analysis_group.add_argument('--max-concurso', type=int, default=None, metavar='NUM', help="Concurso máximo para análise.")
        analysis_group.add_argument('--analysis', nargs='+', choices=['freq','pair','comb','cycle','cycle-stats','delay','max-delay','props','all'], default=None, metavar='TIPO', help="Análises a executar (padrão: all se não for backtest).")
        analysis_group.add_argument('--top-n', type=int, default=15, metavar='N', help="Top N para combinações.")
        analysis_group.add_argument('--windows', type=str, default='10,25,50', metavar='W1,W2,...', help="Janelas de frequência.")
        analysis_group.add_argument('--plot', action='store_true', help="Gera gráficos.")

        backtest_group = parser.add_argument_group('Modo Backtest')
        backtest_group.add_argument('--backtest', action='store_true', help="Ativa modo backtesting.")
        # Adiciona novas estratégias às escolhas
        backtest_group.add_argument('--strategy', type=str,
                                    choices=['most_freq', 'least_freq', 'most_freq_recent', 'most_delayed'],
                                    default='most_freq', help="Estratégia a testar.")
        # Adiciona argumento para a janela da estratégia recente
        backtest_group.add_argument('--strategy-window', type=int, default=25, metavar='W', help="Janela (em concursos) para estratégias recentes (ex: most_freq_recent).")
        backtest_group.add_argument('--start', type=int, default=2000, metavar='CONC_INICIO', help="Concurso inicial para backtest.")
        backtest_group.add_argument('--end', type=int, default=None, metavar='CONC_FIM', help="Concurso final para backtest (padrão: último).")

        return parser

    def _load_or_check_data(self) -> bool:
        # (Código idêntico ao anterior)
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

    def _get_last_available_contest(self) -> Optional[int]:
        df = read_data_from_db(columns=['concurso'])
        if df is not None and not df.empty:
             max_c = df['concurso'].max(); return int(max_c) if not pd.isna(max_c) else None
        return None

    def _run_analysis_mode(self):
        """ Executa o pipeline no modo de Análise. """
        self.logger.info("Executando em modo Análise...")
        analyses_to_run = self.args.analysis if self.args.analysis is not None else ['all']
        run_all = 'all' in analyses_to_run
        # ... (definição das flags run_xxx igual antes) ...
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
        if run_cycle or run_cycle_stats: cycles_summary = execute_cycle_identification()
        if run_cycle and cycles_summary is not None: display_cycle_summary(cycles_summary, self.args, self.should_plot)
        if run_cycle_stats and cycles_summary is not None: execute_cycle_stats_analysis(cycles_summary)
        elif run_cycle_stats and cycles_summary is None: self.logger.error("Não executar 'cycle-stats'.")

        if run_delay: execute_delay_analysis(self.args, self.should_plot)
        if run_max_delay: execute_max_delay_analysis(self.args, self.should_plot)
        if run_props: execute_properties_analysis(self.args, self.should_plot)

    def _run_backtest_mode(self):
        """ Executa o pipeline no modo Backtest. """
        self.logger.info("Executando em modo Backtest...")

        # Mapeamento de nomes de estratégia para funções
        # Usamos lambdas para passar argumentos extras como a janela
        strategy_map: Dict[str, StrategyFuncType] = {
            'most_freq': lambda max_c: select_most_frequent_overall(max_c),
            'least_freq': lambda max_c: select_least_frequent_overall(max_c),
            'most_freq_recent': lambda max_c: select_most_frequent_recent(max_c, window=self.args.strategy_window),
            'most_delayed': lambda max_c: select_most_delayed(max_c),
        }
        selected_strategy_func = strategy_map.get(self.args.strategy)

        if selected_strategy_func is None:
            self.logger.error(f"Estratégia '{self.args.strategy}' não reconhecida."); return

        end_contest = self.args.end or self._get_last_available_contest()
        if end_contest is None: self.logger.error("Não foi possível determinar último concurso."); return
        start_contest = self.args.start
        if start_contest >= end_contest or start_contest <= 1:
             self.logger.error(f"Concurso inicial ({start_contest}) inválido para período até {end_contest}."); return

        backtest_summary = run_backtest(selected_strategy_func, self.args.strategy, start_contest, end_contest)

        if backtest_summary:
            print(f"\n--- Resumo Backtest: Estratégia '{self.args.strategy}' (Concursos {start_contest}-{end_contest}) ---")
            # Adiciona um loop para imprimir o resumo formatado
            for hits in range(15, 10, -1): # Imprime de 15 a 11
                print(f"Acertos {hits}: {backtest_summary.get(hits, 0)} vezes")
            print(f"Abaixo de 11: {backtest_summary.get('<11', 0)} vezes")
            if backtest_summary.get('errors', 0) > 0: print(f"Erros/Falhas: {backtest_summary['errors']} vezes")
        else: self.logger.error("Backtest não retornou resultados.")


    def run(self):
        """ Executa o pipeline principal ou o backtesting. """
        self.logger.info(f"Iniciando Lotofacil Analysis - Orquestrador v3 {'(Backtest)' if self.args.backtest else '(Análise)'}")
        if not self._load_or_check_data():
            self.logger.error("Pré-requisitos de dados não atendidos. Encerrando."); return

        if self.args.backtest:
            self._run_backtest_mode()
        else:
            self._run_analysis_mode()

        self.logger.info("Aplicação Lotofacil Analysis finalizada.")