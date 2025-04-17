# src/scorer.py

import pandas as pd
from typing import Dict, Any, Optional, List, Set

# Importa do config
from src.config import logger, ALL_NUMBERS, AGGREGATOR_WINDOWS, DEFAULT_GROUP_WINDOWS # Usa janelas

# Fallbacks
if 'ALL_NUMBERS' not in globals(): ALL_NUMBERS = list(range(1, 26))
if 'AGGREGATOR_WINDOWS' not in globals(): AGGREGATOR_WINDOWS = [10, 25, 50, 100, 200, 300, 400, 500]
if 'DEFAULT_GROUP_WINDOWS' not in globals(): DEFAULT_GROUP_WINDOWS = [25, 100]


# --- CONFIGURAÇÃO DE SCORE V8 ---
# Inclui rank_trend e group_stats
DEFAULT_SCORING_CONFIG_V8: Dict[str, Dict] = {
    # Frequências
    'overall_freq':      {'weight': 0.4, 'rank_higher_is_better': True}, # Peso ainda menor
    **{f'recent_freq_{w}': {'weight': max(0.1, 1.5 - (w/100)*0.4), 'rank_higher_is_better': True} for w in AGGREGATOR_WINDOWS if w >= 100}, # 1.1 a 0.3
    'recent_freq_50':    {'weight': 1.0, 'rank_higher_is_better': True},
    'recent_freq_25':    {'weight': 1.3, 'rank_higher_is_better': True},
    'recent_freq_10':    {'weight': 1.5, 'rank_higher_is_better': True},
    # Tendências
    'freq_trend':        {'weight': 0.8, 'rank_higher_is_better': True},
    'rank_trend':        {'weight': 1.0, 'rank_higher_is_better': True}, # <<< NOVO: Rank subindo = melhor
    # Atrasos
    'current_delay':     {'weight': 1.4, 'rank_higher_is_better': True}, # Peso alto
    'delay_std_dev':     {'weight': 0.7, 'rank_higher_is_better': False},# Menor std dev = melhor
    # Ciclos
    'last_cycle_freq':   {'weight': 0.6, 'rank_higher_is_better': True},
    'current_cycle_freq':{'weight': 1.0, 'rank_higher_is_better': True},
    'current_intra_cycle_delay': {'weight': 1.3, 'rank_higher_is_better': True},
    'closing_freq':      {'weight': 0.7, 'rank_higher_is_better': True},
    # <<< NOVO: Stats de Grupo >>> (Ex: usando janela 25)
    # O nome da métrica deve corresponder à chave criada no agregador
    'group_W25_avg_freq':{'weight': 0.4, 'rank_higher_is_better': True}, # Grupo quente = bom
    # 'group_W100_avg_freq':{'weight': 0.2, 'rank_higher_is_better': True}, # Grupo quente (longo prazo)= bom
    # Stats intra-ciclo histórico e sole_closing ainda não usados
    # 'avg_hist_intra_delay': {'weight': 0.0, 'rank_higher_is_better': False},
    # 'max_hist_intra_delay': {'weight': 0.0, 'rank_higher_is_better': True},
    # 'sole_closing_freq': {'weight': 0.0, 'rank_higher_is_better': True}
}

MISSING_CYCLE_BONUS = 5.0
REPEAT_PENALTY = -15.0 # Mantém penalidade

def calculate_scores(analysis_results: Dict[str, Any],
                     config: Optional[Dict[str, Dict]] = None) -> Optional[pd.Series]:
    """ Calcula pontuação V8: inclui rank trend e group stats. """
    if config is None: config = DEFAULT_SCORING_CONFIG_V8 # <<< USA V8
    if not analysis_results: logger.error("Resultados da análise vazios."); return None

    logger.info("Calculando pontuação das dezenas (v8)...")
    final_scores = pd.Series(0.0, index=ALL_NUMBERS); final_scores.index.name = 'Dezena'

    # Calcula scores baseados nas métricas individuais e pesos
    for metric, params in config.items():
        # Pula métricas de grupo aqui, serão tratadas depois
        if metric.startswith('group_'): continue

        weight = params.get('weight', 1.0); higher_is_better = params.get('rank_higher_is_better', True)
        if weight == 0: continue
        if metric not in analysis_results or analysis_results[metric] is None: logger.warning(f"Métrica '{metric}' Nula/Ausente."); continue

        metric_series = analysis_results[metric]; logger.debug(f"Proc: {metric} (W:{weight}, HighBest:{higher_is_better})")
        if not isinstance(metric_series, pd.Series): logger.warning(f"'{metric}' não é Series."); continue
        try: numeric_series = pd.to_numeric(metric_series, errors='coerce')
        except Exception as e: logger.warning(f"Erro converter '{metric}': {e}."); continue

        numeric_series = numeric_series.reindex(ALL_NUMBERS)
        if numeric_series.isnull().all(): logger.warning(f"'{metric}' só contém nulos."); continue

        ranks = numeric_series.rank(method='min', ascending=(not higher_is_better), na_option='bottom')
        points = 26 - ranks
        weighted_points = points * weight
        final_scores = final_scores.add(weighted_points, fill_value=0)

    # Aplica Bônus de Ciclo
    missing_in_cycle: Optional[Set[int]] = analysis_results.get('missing_current_cycle')
    if missing_in_cycle is not None and len(missing_in_cycle) > 0 and len(missing_in_cycle) < 25:
        logger.info(f"Aplicando bônus {MISSING_CYCLE_BONUS} pts para {len(missing_in_cycle)} dezenas faltantes.")
        # Usando .loc para evitar SettingWithCopyWarning
        final_scores.loc[list(missing_in_cycle.intersection(final_scores.index))] += MISSING_CYCLE_BONUS

    # Aplica Penalidade de Repetição
    numbers_last_draw: Optional[Set[int]] = analysis_results.get('numbers_in_last_draw')
    if numbers_last_draw is not None and REPEAT_PENALTY != 0:
         logger.info(f"Aplicando penalidade {REPEAT_PENALTY} pts para {len(numbers_last_draw)} dezenas repetidas.")
         # Usando .loc para evitar SettingWithCopyWarning
         final_scores.loc[list(numbers_last_draw.intersection(final_scores.index))] += REPEAT_PENALTY

    # *** Aplica Scores de Grupo ***
    # Itera sobre as métricas de grupo definidas na config V8
    for metric, params in config.items():
        if not metric.startswith('group_'): continue # Processa apenas métricas de grupo
        weight = params.get('weight', 1.0); higher_is_better = params.get('rank_higher_is_better', True)
        if weight == 0: continue
        if metric not in analysis_results or analysis_results[metric] is None: logger.warning(f"Métrica de grupo '{metric}' Nula/Ausente."); continue

        group_metric_series = analysis_results[metric] # Esta Series já tem a média do grupo para cada dezena
        logger.debug(f"Proc Grupo: {metric} (W:{weight}, HighBest:{higher_is_better})")
        if not isinstance(group_metric_series, pd.Series): logger.warning(f"'{metric}' não é Series."); continue

        # Como a série já contém o valor do grupo para cada dezena, podemos aplicar diretamente
        # Talvez normalizar ou rankear os valores da *própria série* antes de aplicar?
        # Ou só multiplicar pelo peso? Vamos multiplicar pelo peso por enquanto.
        # Normalizar pode ser melhor: (valor_grupo - media_todos_grupos) / std_dev_todos_grupos
        # Por simplicidade agora, vamos adicionar o valor da métrica * peso
        # (Isso assume que a métrica em si já tem uma escala razoável)
        # Alternativa: Rankear os valores da group_metric_series e dar pontos?
        ranks_group = group_metric_series.rank(method='min', ascending=(not higher_is_better), na_option='bottom')
        points_group = 26 - ranks_group
        weighted_points_group = points_group * weight
        final_scores = final_scores.add(weighted_points_group, fill_value=0)


    final_scores.sort_values(ascending=False, inplace=True)
    final_scores.fillna(0, inplace=True)
    logger.info("Cálculo de pontuação final (v8) concluído.")
    return final_scores