# src/orchestrator.py

import argparse
import logging
import pandas as pd
import sqlite3
from typing import Optional, Callable, Set, Dict, Any, List, Union

# Importações config e data handling
from src.config import (
    logger, TABLE_NAME, DATABASE_PATH, DEFAULT_SNAPSHOT_INTERVALS,
    DEFAULT_CMD_WINDOWS, ALL_CHUNK_INTERVALS
)
from src.data_loader import load_and_clean_data
from src.database_manager import save_to_db, read_data_from_db, get_draw_numbers

# Importa pipeline steps
from src.pipeline_steps.execute_frequency import execute_frequency_analysis
from src.pipeline_steps.execute_pairs import execute_pair_analysis
from src.pipeline_steps.execute_combinations import execute_combination_analysis
from src.pipeline_steps.execute_cycles import execute_cycle_identification, display_cycle_summary
from src.pipeline_steps.execute_cycle_stats import execute_cycle_stats_analysis
from src.pipeline_steps.execute_delay import execute_delay_analysis
from src.pipeline_steps.execute_max_delay import execute_max_delay_analysis
from src.pipeline_steps.execute_properties import execute_properties_analysis
from src.pipeline_steps.execute_metrics_viz import execute_metrics_visualization

# Importa backtester e strategies
from src.backtester.runner import BacktesterRunner
from src.strategies.frequency_strategies import *
from src.strategies.delay_strategies import *
from src.strategies.scoring_strategies import *

# Importa agregador, scorer e updaters
from src.analysis_aggregator import get_consolidated_analysis
from src.scorer import calculate_scores
from src.analysis.cycle_analysis import update_cycles_table
from src.table_updater import update_freq_geral_snap_table, update_chunk_final_stats_table

# Verifica plotagem
try:
    from src.visualization.plotter import setup_plotting
    # Não tenta importar plot_backtest_summary aqui para evitar erro inicial
    setup_plotting(); PLOTTING_ENABLED = True
except ImportError as e: logger.warning(f"Libs plot não encontradas: {e}. Plotagem desativada."); PLOTTING_ENABLED = False
# Importa a função específica onde ela é usada, tratando o erro lá se necessário
# Tenta importar plot_backtest_summary (se existir)
try: from src.visualization.plotter import plot_backtest_summary
except ImportError: plot_backtest_summary = None


StrategyFuncType = Callable[[Dict[str, Any]], Optional[Set[int]]]

class AnalysisOrchestrator:
    def __init__(self):
        self.logger = logger
        self.args = self._setup_parser().parse_args()
        self.plotting_available = PLOTTING_ENABLED
        # Define analysis_args ANTES de usá-lo
        self.analysis_args = self.args.analysis if hasattr(self.args, 'analysis') and self.args.analysis is not None else []
        self.should_plot = (self.args.plot or ('metrics-viz' in self.analysis_args)) and self.plotting_available
        if (self.args.plot or ('metrics-viz' in self.analysis_args)) and not self.plotting_available:
             self.logger.warning("Plot/Viz solicitado, mas matplotlib não disponível.")
        self._last_contest_in_db = None

    def _setup_parser(self) -> argparse.ArgumentParser:
        # (Parser idêntico ao anterior)
        parser = argparse.ArgumentParser(description="Analisa/Backtest Lotofácil.", formatter_class=argparse.ArgumentDefaultsHelpFormatter)
        update_group = parser.add_argument_group('Opções Gerais e de Atualização'); update_group.add_argument('--reload', action='store_true', help="Força recarga e reconstrói tabelas aux."); update_group.add_argument('--update-cycles', action='store_true', help="Atualiza 'ciclos'."); update_group.add_argument('--force-rebuild-cycles', action='store_true', help="Reconstrói 'ciclos'."); update_group.add_argument('--update-freq-snaps', action='store_true', help="Atualiza 'freq_geral_snap'."); update_group.add_argument('--force-rebuild-freq-snaps', action='store_true', help="Reconstrói 'freq_geral_snap'."); update_group.add_argument('--rebuild-chunk-stats', nargs='+', metavar='INT|all', help="Reconstrói tabela(s) stats finais chunk (ex: 10 ou 'all')."); update_group.add_argument('--update-chunk-stats', nargs='+', metavar='INT|all', help="Atualiza tabela(s) stats finais chunk.")
        analysis_group = parser.add_argument_group('Modo Análise'); analysis_group.add_argument('--max-concurso', type=int, default=None, metavar='NUM', help="Concurso máximo."); analysis_group.add_argument('--analysis', nargs='+', choices=['freq','pair','comb','cycle','cycle-stats','delay','max-delay','props','metrics-viz','all'], default=None, metavar='TIPO', help="Análises (inclui 'metrics-viz')."); analysis_group.add_argument('--top-n', type=int, default=15, metavar='N', help="Top N combos."); analysis_group.add_argument('--windows', type=str, default=DEFAULT_CMD_WINDOWS, metavar='W1,...', help=f"Janelas freq. P: {DEFAULT_CMD_WINDOWS}"); analysis_group.add_argument('--plot', action='store_true', help="Gera gráficos (ativado por padrão com metrics-viz).")
        backtest_group = parser.add_argument_group('Modo Backtest'); backtest_group.add_argument('--backtest', action='store_true', help="Ativa backtesting."); backtest_group.add_argument('--strategy', type=str, choices=['most_freq', 'least_freq', 'most_freq_recent', 'most_delayed', 'top_score'], default='most_freq', help="Estratégia."); backtest_group.add_argument('--strategy-window', type=int, default=25, metavar='W', help="Janela p/ est. recente."); backtest_group.add_argument('--start', type=int, default=None, metavar='CONC_INICIO', help="Início backtest."); backtest_group.add_argument('--end', type=int, default=None, metavar='CONC_FIM', help="Fim backtest.")
        predict_group = parser.add_argument_group('Modo Predição'); predict_group.add_argument('--predict-last', action='store_true', help="Analisa N-1, pontua (V8) e compara com N.")
        return parser

    def _ensure_data_loaded(self) -> bool: # (Idêntico)
        if self._last_contest_in_db is None:
             if not self._load_or_check_data(): return False
        return True
    def _load_or_check_data(self) -> bool: # (Idêntico - com try/except corrigido)
        if self.args.reload:
            cleaned_data = load_and_clean_data();
            if cleaned_data is None: return False
            if not save_to_db(df=cleaned_data, table_name=TABLE_NAME, if_exists='replace'): return False
            try: # Bloco Corrigido
                with sqlite3.connect(DATABASE_PATH) as conn:
                    conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_concurso ON {TABLE_NAME} (concurso);")
                    self.logger.info(f"Índice para '{TABLE_NAME}' criado/verificado.")
            except Exception as e:
                self.logger.error(f"Erro ao criar índice para {TABLE_NAME}: {e}")
            self._last_contest_in_db = self._get_last_available_contest(force_read=True);
            if self._last_contest_in_db is None: return False
            self.logger.info("Forçando reconstrução tabelas auxiliares após reload...")
            update_cycles_table(force_rebuild=True); update_freq_geral_snap_table(force_rebuild=True)
            self.logger.info(f"Reconstruindo chunks para: {ALL_CHUNK_INTERVALS}")
            for interval in ALL_CHUNK_INTERVALS: update_chunk_final_stats_table(interval_size=interval, force_rebuild=True)
        else:
            if read_data_from_db(columns=['concurso'], concurso_maximo=1) is None: return False
            if self._last_contest_in_db is None: self._last_contest_in_db = self._get_last_available_contest()
            if self._last_contest_in_db is None: return False
        self.logger.info("Dados do BD acessíveis."); return True
    def _get_last_available_contest(self, force_read: bool = False) -> Optional[int]: # (Idêntico)
        if not force_read and hasattr(self, '_last_contest_in_db') and self._last_contest_in_db is not None: return self._last_contest_in_db
        df = read_data_from_db(columns=['concurso']);
        if df is not None and not df.empty: max_c = df['concurso'].max(); self._last_contest_in_db = int(max_c) if not pd.isna(max_c) else None; return self._last_contest_in_db
        self.logger.error("Não ler dados BD."); return None

    # --- MÉTODO _run_analysis_mode (LÓGICA CORRIGIDA FINALMENTE) ---
    def _run_analysis_mode(self):
        """ Executa o pipeline no modo Análise, incluindo visualização. """
        self.logger.info("Executando em modo Análise...")
        # Usa self.analysis_args definido no __init__
        analyses_to_run = self.analysis_args if self.analysis_args else ['all']
        run_all = 'all' in analyses_to_run

        default_analyses = ['freq','pair','comb','cycle','cycle-stats','delay','max-delay','props']
        run_list = default_analyses if run_all else [a for a in analyses_to_run if a in default_analyses]
        run_metrics_viz = run_all or 'metrics-viz' in analyses_to_run

        # Variáveis para guardar resultados se precisarmos para múltiplos steps
        analysis_results = None
        cycles_summary = None

        # Executa análises padrão se solicitadas
        if run_list:
             self.logger.info(f"Executando análises padrão: {run_list}")
             if 'freq' in run_list: execute_frequency_analysis(self.args, self.should_plot)
             if 'pair' in run_list: execute_pair_analysis(self.args)
             if 'comb' in run_list: execute_combination_analysis(self.args)
             # Identifica ciclos se 'cycle' ou 'cycle-stats' estiverem na lista
             if 'cycle' in run_list or 'cycle-stats' in run_list:
                  if cycles_summary is None: cycles_summary = execute_cycle_identification(self.args)
             if 'cycle' in run_list and cycles_summary is not None: display_cycle_summary(cycles_summary, self.args, self.should_plot)
             if 'cycle-stats' in run_list and cycles_summary is not None: execute_cycle_stats_analysis(cycles_summary, self.args)
             if 'delay' in run_list: execute_delay_analysis(self.args, self.should_plot)
             if 'max-delay' in run_list: execute_max_delay_analysis(self.args, self.should_plot)
             if 'props' in run_list: execute_properties_analysis(self.args, self.should_plot)

        # Executa visualização de métricas se solicitado
        if run_metrics_viz:
             if not self.plotting_available:
                  self.logger.warning("Análise 'metrics-viz' solicitada, mas matplotlib não disponível.")
             else:
                  self.logger.info("Preparando dados para visualização de métricas...")
                  # Garante que temos o último concurso (já deve ter sido pego por _ensure_data_loaded)
                  max_c = self.args.max_concurso if self.args.max_concurso else self._last_contest_in_db
                  if max_c is None:
                       self.logger.error("Não foi possível determinar o último concurso para visualização (self._last_contest_in_db is None).")
                  else:
                       # *** CHAMA O AGREGADOR PARA OBTER OS DADOS NECESSÁRIOS ***
                       self.logger.info(f"Calculando dados agregados até {max_c} para visualização...")
                       analysis_results = get_consolidated_analysis(max_c)

                       if analysis_results: # Verifica se o agregador retornou dados
                            execute_metrics_visualization(analysis_results) # Passa os dados para o step de plotagem
                       else:
                            self.logger.error("Falha ao obter dados consolidados para visualização.")

    def _run_backtest_mode(self):
        # (Código idêntico ao anterior)
        self.logger.info("Executando em modo Backtest (Incremental)...")
        strategy_map: Dict[str, StrategyFuncType] = { 'most_freq': select_most_frequent_overall, 'least_freq': select_least_frequent_overall, 'most_freq_recent': lambda analysis: select_most_frequent_recent(analysis, window=self.args.strategy_window), 'most_delayed': select_most_delayed, 'top_score': select_top_scored }
        selected_strategy_func = strategy_map.get(self.args.strategy);
        if selected_strategy_func is None: self.logger.error(f"Estratégia '{self.args.strategy}' inválida."); return
        last_contest_db = self._last_contest_in_db;
        if last_contest_db is None: self.logger.error("Último concurso BD não determinado."); return
        end_contest = self.args.end if self.args.end is not None and self.args.end <= last_contest_db else last_contest_db
        if self.args.end is not None and self.args.end > last_contest_db: self.logger.warning(f"End {self.args.end} > último ({last_contest_db}). Usando {last_contest_db}.")
        start_contest = self.args.start if self.args.start is not None else 500
        if start_contest >= end_contest or start_contest <= 1: self.logger.error(f"Período backtest inválido: {start_contest}-{end_contest}."); return
        if self.args.strategy == 'top_score' and (end_contest - start_contest > 200): self.logger.warning(f"Backtest 'top_score' LENTO para {end_contest - start_contest + 1} concursos.")
        runner = BacktesterRunner(selected_strategy_func, self.args.strategy, start_contest, end_contest, initial_analysis_needed=(self.args.strategy == 'top_score'))
        backtest_summary = runner.run()
        if backtest_summary:
            title = f"Resumo Backtest Incr: '{self.args.strategy}' ({start_contest}-{end_contest})"; print(f"\n--- {title} ---"); [print(f"Acertos {h}: {backtest_summary.get(h, 0)}x") for h in range(15, 10, -1)]; print(f"<11 Acertos: {backtest_summary.get('<11', 0)}x"); [print(f"Erros: {backtest_summary['errors']}x") for _ in range(1) if backtest_summary.get('errors', 0) > 0]
            if self.should_plot:
                plot_filename = f"backtest_incr_{self.args.strategy}_{start_contest}_{end_contest}"; global PLOTTING_ENABLED
                if PLOTTING_ENABLED:
                    try: from src.visualization.plotter import plot_backtest_summary; plot_backtest_summary(backtest_summary, title, plot_filename)
                    except ImportError: self.logger.warning("Função plot_backtest_summary não encontrada.")
        else: self.logger.error("Backtest não retornou resultados.")

    def _run_predict_last_mode(self):
        # (Código idêntico ao anterior)
        self.logger.info("Executando Predição Último Concurso (Scorer V6/V7)...")
        n = self._last_contest_in_db;
        if n is None or n <= 1: self.logger.error("Concursos insuficientes."); return
        n_minus_1 = n - 1; self.logger.info(f"N={n}, N-1={n_minus_1}")
        analysis_results = get_consolidated_analysis(n_minus_1);
        if analysis_results is None: self.logger.error(f"Falha análises consolidadas."); return
        scores = calculate_scores(analysis_results);
        if scores is None: self.logger.error(f"Falha calcular scores."); return
        selected_numbers: Optional[Set[int]] = None
        if not scores.empty: print("\n--- Pontuação Calculada V8 (Top 5) ---"); print(scores.head(5).to_string()); print("\n--- Pontuação Calculada V8 (Bottom 5) ---"); print(scores.tail(5).to_string()); selected_numbers = set(scores.nlargest(15).index); print(f"\n--- Dezenas Selecionadas (Score V8 até {n_minus_1}) ---"); print(sorted(list(selected_numbers)))
        else: self.logger.error("Scores vazios.")
        actual_numbers = get_draw_numbers(n);
        if actual_numbers is None: self.logger.error(f"Falha resultado real {n}."); return
        print(f"\n--- Resultado Real do Concurso {n} ---"); print(sorted(list(actual_numbers)))
        if selected_numbers is not None: from src.backtester.evaluator import evaluate_hits; hits = evaluate_hits(selected_numbers, actual_numbers); print(f"\n--- Comparação ---\nAcertos: {hits} pontos")
        else: print("\n--- Comparação ---\nSeleção não gerada.")
        
    def run(self): # (Idêntico)
        self.logger.info(f"Iniciando Lotofacil Analysis - Orquestrador v8")
        run_update_action = False
        # ... (Lógica de update/rebuild idêntica e correta) ...
        if getattr(self.args, 'update_cycles', False) or getattr(self.args, 'force_rebuild_cycles', False):
            if not self._ensure_data_loaded(): return; update_cycles_table(force_rebuild=getattr(self.args, 'force_rebuild_cycles', False)); run_update_action = True
        if getattr(self.args, 'update_freq_snaps', False) or getattr(self.args, 'force_rebuild_freq_snaps', False):
             if not self._ensure_data_loaded(): return; update_freq_geral_snap_table(force_rebuild=getattr(self.args, 'force_rebuild_freq_snaps', False)); run_update_action = True
        if getattr(self.args, 'rebuild_chunk_stats', None):
             if not self._ensure_data_loaded(): return; intervals_to_process: List[Union[int, str]] = self.args.rebuild_chunk_stats; final_intervals: List[int] = [];
             if 'all' in intervals_to_process: final_intervals = ALL_CHUNK_INTERVALS
             else:
                 try: final_intervals = [int(i) for i in intervals_to_process]
                 except ValueError: self.logger.error(f"Intervalos inválidos: {intervals_to_process}"); return
             self.logger.info(f"Executando rebuild chunk stats: {final_intervals}")
             for interval in final_intervals: update_chunk_final_stats_table(interval_size=interval, force_rebuild=True)
             run_update_action = True
        elif getattr(self.args, 'update_chunk_stats', None):
             if not self._ensure_data_loaded(): return; intervals_to_process = self.args.update_chunk_stats; final_intervals = []
             if 'all' in intervals_to_process: final_intervals = ALL_CHUNK_INTERVALS
             else:
                 try: final_intervals = [int(i) for i in intervals_to_process]
                 except ValueError: self.logger.error(f"Intervalos inválidos: {intervals_to_process}"); return
             self.logger.info(f"Executando update chunk stats: {final_intervals}")
             for interval in final_intervals: update_chunk_final_stats_table(interval_size=interval, force_rebuild=False)
             run_update_action = True

        if run_update_action: self.logger.info("Ação de update concluída."); return
        # Garante que dados são carregados ANTES de rodar os modos principais
        if not self._ensure_data_loaded(): self.logger.error("Pré-requisitos de dados não atendidos."); return

        if self.args.backtest: self._run_backtest_mode()
        elif self.args.predict_last: self._run_predict_last_mode()
        elif self.args.analysis is not None: self._run_analysis_mode() # Chama método corrigido
        else: self.logger.info("Nenhuma ação principal.");

        self.logger.info("Aplicação Lotofacil Analysis finalizada.")