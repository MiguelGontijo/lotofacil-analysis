# src/pipeline_steps/__init__.py
import logging
logger = logging.getLogger(__name__)

# Importar funções dos módulos de etapa existentes
from .execute_frequency import run_frequency_analysis
from .execute_delay import run_delay_analysis
from .execute_properties import run_number_properties_analysis
from .execute_pairs import run_pair_combination_analysis
from .execute_cycle_stats import run_cycle_identification_and_stats
from .execute_cycles import run_cycle_closing_analysis # Mantém o stub se execute_cycles.py não tiver lógica real

# NOVAS importações para análise e visualização de chunks (já devem estar corretas)
from .execute_chunk_evolution_analysis import run_chunk_evolution_analysis
from .execute_chunk_evolution_visualization import run_chunk_evolution_visualization

# Visualizações Core
from .execute_metrics_viz import run_core_metrics_visualization

# --- Etapas que ainda podem ser STUBS ---
# Se você criou os arquivos e funções para estas, descomente os imports e remova os stubs.

# from .execute_repetition import run_repetition_analysis
def run_repetition_analysis(*args, **kwargs):
    logger.error("ETAPA STUB: 'run_repetition_analysis' chamada, mas não totalmente implementada.")
    return False

# from .execute_group_trend import run_group_trend_analysis
def run_group_trend_analysis(*args, **kwargs):
    logger.error("ETAPA STUB: 'run_group_trend_analysis' chamada, mas não totalmente implementada.")
    return False

# IMPORTAÇÃO REAL PARA RANK TREND ANALYSIS
from .execute_rank_trend_analysis import run_rank_trend_analysis_step # <<< MODIFICADO

# --- Fim das Etapas STUBS ---


__all__ = [
    "run_frequency_analysis",
    "run_delay_analysis",
    "run_number_properties_analysis",
    "run_repetition_analysis",
    "run_pair_combination_analysis",
    "run_cycle_identification_and_stats",
    "run_cycle_closing_analysis",
    "run_group_trend_analysis",
    "run_rank_trend_analysis_step", # <<< MODIFICADO (usando o nome da função real)
    "run_core_metrics_visualization",
    "run_chunk_evolution_analysis",
    "run_chunk_evolution_visualization",
]