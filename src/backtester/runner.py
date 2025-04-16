# src/backtester/runner.py

from typing import Callable, Optional, Set, Dict, Any, List
import pandas as pd
from collections import Counter
import time

# Importa do config (INCLUINDO BASE_COLS)
from src.config import logger, ALL_NUMBERS, BASE_COLS, NEW_BALL_COLUMNS

# Importa do database_manager (SEM BASE_COLS)
from src.database_manager import read_data_from_db, get_draw_numbers
from src.backtester.evaluator import evaluate_hits, summarize_results
# Importa o agregador para buscar o estado inicial
from src.analysis_aggregator import get_consolidated_analysis

# Fallbacks (caso config falhe)
if 'ALL_NUMBERS' not in globals(): ALL_NUMBERS = list(range(1, 26))
if 'NEW_BALL_COLUMNS' not in globals(): NEW_BALL_COLUMNS = [f'b{i}' for i in range(1,16)]
if 'BASE_COLS' not in globals(): BASE_COLS = ['concurso'] + NEW_BALL_COLUMNS


# Novo tipo para a função da estratégia
StrategyFuncType = Callable[[Dict[str, Any]], Optional[Set[int]]]

class BacktesterRunner:
    """ Executa backtesting usando atualização incremental (Fase 1: Freq/Delay). """
    def __init__(self,
                 strategy_func: StrategyFuncType,
                 strategy_name: str,
                 start_contest: int,
                 end_contest: int,
                 initial_analysis_needed: bool = True):
        self.strategy_func = strategy_func
        self.strategy_name = strategy_name
        self.start_contest = start_contest
        self.end_contest = end_contest
        self.initial_analysis_needed = initial_analysis_needed
        self.logger = logger
        self.current_contest: int = 0
        self.overall_freq: Optional[pd.Series] = None
        self.current_delay: Optional[pd.Series] = None
        self.last_seen_concurso: Optional[Dict[int, int]] = None
        self.analysis_state: Dict[str, Any] = {}
        self.results_log: Dict[int, int] = {}
        self.draw_data_period: Optional[pd.DataFrame] = None # Armazena dados lidos

    def _initialize_state(self) -> bool:
        """ Calcula o estado inicial das métricas até start_contest - 1. """
        self.current_contest = self.start_contest - 1
        self.logger.info(f"Inicializando estado do backtester até concurso {self.current_contest}...")

        # Se precisar de todas as análises (ex: top_score), chama o agregador UMA VEZ
        if self.initial_analysis_needed:
            self.logger.info("Buscando análises consolidadas iniciais...")
            initial_agg_results = get_consolidated_analysis(self.current_contest)
            if initial_agg_results is None: self.logger.error("Falha ao obter análises iniciais."); return False
            self.analysis_state.update(initial_agg_results)
            self.overall_freq = self.analysis_state.get('overall_freq')
            self.current_delay = self.analysis_state.get('current_delay')
            if self.current_delay is not None:
                 self.last_seen_concurso = { n: self.current_contest - int(d) if pd.notna(d) else 0 for n, d in self.current_delay.items() }
                 self.logger.info("last_seen_concurso inicializado a partir do current_delay agregado.")
            else: self.logger.error("current_delay não encontrado."); return False
            self.logger.info("Análises consolidadas iniciais obtidas.")
        # Senão, calcula apenas as básicas (Freq, Delay)
        else:
            # Usa BASE_COLS importado do config
            initial_data = read_data_from_db(columns=BASE_COLS, concurso_maximo=self.current_contest)
            if initial_data is None: self.logger.error("Falha ao ler dados iniciais."); return False
            # Frequência Geral
            if initial_data.empty: self.overall_freq = pd.Series(0, index=ALL_NUMBERS)
            else: melted = initial_data[NEW_BALL_COLUMNS].melt(value_name='n')['n'].dropna().astype(int); self.overall_freq = melted.value_counts().reindex(ALL_NUMBERS, fill_value=0)
            self.analysis_state['overall_freq'] = self.overall_freq; logger.info("Freq. geral inicial calculada.")
            # Atraso Atual e Last Seen
            self.last_seen_concurso = {n: 0 for n in ALL_NUMBERS}; self.current_delay = pd.Series(self.current_contest + 1, index=ALL_NUMBERS, dtype='Int64')
            if not initial_data.empty:
                for index, row in initial_data.iloc[::-1].iterrows():
                    conc = int(row['concurso']); drawn = {int(n) for n in row[NEW_BALL_COLUMNS].dropna().values}
                    for num in ALL_NUMBERS:
                         if self.last_seen_concurso.get(num, 0) == 0 and num in drawn: self.last_seen_concurso[num] = conc
                for num in ALL_NUMBERS:
                    last_seen = self.last_seen_concurso[num]
                    if last_seen > 0: self.current_delay[num] = self.current_contest - last_seen
            self.analysis_state['current_delay'] = self.current_delay; logger.info("Atraso atual inicial calculado.")
            self.analysis_state['numbers_in_last_draw'] = set(initial_data.iloc[-1][NEW_BALL_COLUMNS].dropna().astype(int)) if not initial_data.empty else set()

        # Pré-lê os dados do período de backtest
        self.logger.info(f"Pré-lendo dados sorteios {self.start_contest} a {self.end_contest}...")
        # Usa BASE_COLS importado do config
        self.draw_data_period = read_data_from_db(columns=BASE_COLS, concurso_minimo=self.start_contest, concurso_maximo=self.end_contest)
        if self.draw_data_period is None or self.draw_data_period.empty: self.logger.error("Não ler dados período backtest."); return False
        self.draw_data_period.set_index('concurso', inplace=True); logger.info("Dados período backtest lidos.")

        if self.overall_freq is None or self.current_delay is None or self.last_seen_concurso is None: self.logger.error("Estado inicial incompleto."); return False
        return True


    def _update_state(self, drawn_numbers_current_draw: Set[int]):
        """ Atualiza incrementalmente Freq Geral e Atraso Atual. """
        self.current_contest += 1; logger.debug(f"Atualizando estado p/ FIM do Concurso {self.current_contest}...")
        # 1. Frequência Geral
        if self.overall_freq is not None:
             self.overall_freq.loc[list(drawn_numbers_current_draw)] += 1 # Mais eficiente
             self.analysis_state['overall_freq'] = self.overall_freq
        # 2. Atraso Atual e Last Seen
        if self.current_delay is not None and self.last_seen_concurso is not None:
            # Incrementa atraso para todos que NÃO saíram
            not_drawn = set(ALL_NUMBERS) - drawn_numbers_current_draw
            self.current_delay.loc[list(not_drawn)] += 1
            # Zera atraso e atualiza last_seen para os que saíram
            self.current_delay.loc[list(drawn_numbers_current_draw)] = 0
            for num in drawn_numbers_current_draw: self.last_seen_concurso[num] = self.current_contest
            self.analysis_state['current_delay'] = self.current_delay
        # 3. Outras métricas (AINDA NÃO INCREMENTAIS - Recalcula p/ top_score)
        if self.strategy_name == 'top_score':
             # logger.warning("Backtester incremental Fase 1: Recalculando métricas p/ 'top_score'.") # Log opcional
             temp_results = get_consolidated_analysis(self.current_contest)
             if temp_results:
                 self.analysis_state.update(temp_results)
                 if self.overall_freq is not None: self.analysis_state['overall_freq'] = self.overall_freq # Garante overwrite
                 if self.current_delay is not None: self.analysis_state['current_delay'] = self.current_delay # Garante overwrite
        # 4. Guarda números deste sorteio para próxima iteração
        self.analysis_state['numbers_in_last_draw'] = drawn_numbers_current_draw


    def run(self) -> Optional[Dict[int, int]]:
        """ Executa o loop de backtest. """
        start_time = time.time()
        if not self._initialize_state(): return None

        total_contests = self.end_contest - self.start_contest + 1; contests_processed = 0
        self.logger.info(f"Iniciando loop backtest ({self.start_contest} a {self.end_contest})...")

        for contest_to_play in range(self.start_contest, self.end_contest + 1):
            # Estado reflete fim do concurso self.current_contest (= contest_to_play - 1)
            current_analysis_for_strategy = self.analysis_state.copy()

            # Chama Estratégia
            self.logger.debug(f"Backtest [Conc. {contest_to_play}]: Aplicando '{self.strategy_name}'...")
            chosen_numbers = self.strategy_func(current_analysis_for_strategy) # Passa dict

            # Processa resultado
            actual_numbers_set = None
            if contest_to_play in self.draw_data_period.index:
                 # Usa NEW_BALL_COLUMNS importado do config
                 actual_numbers_set = set(self.draw_data_period.loc[contest_to_play][NEW_BALL_COLUMNS].dropna().astype(int))
            else: self.logger.error(f"Dados reais não encontrados p/ {contest_to_play}!"); self.results_log[contest_to_play] = -1

            if chosen_numbers is None or len(chosen_numbers) != 15 :
                self.logger.error(f"Estratégia falhou conc. {contest_to_play}."); self.results_log[contest_to_play] = -1
            elif actual_numbers_set is not None:
                hits = evaluate_hits(chosen_numbers, actual_numbers_set)
                self.results_log[contest_to_play] = hits; self.logger.debug(f"Conc. {contest_to_play}: {hits} acertos.")
            # else: erro já logado

            # Atualiza estado com dados REAIS do concurso que acabou de acontecer
            if actual_numbers_set is not None: self._update_state(actual_numbers_set)
            else: self.logger.critical(f"Fim backtest - dados faltantes conc. {contest_to_play}."); break

            contests_processed += 1
            if contests_processed % 100 == 0: self.logger.info(f"Progresso: {contests_processed}/{total_contests} processados.")

        # Fim do Loop
        duration = time.time() - start_time
        self.logger.info(f"Backtest concluído p/ '{self.strategy_name}'. Processados: {contests_processed}/{total_contests}.")
        self.logger.info(f"Tempo total: {duration:.2f} segundos.")
        return summarize_results(self.results_log)