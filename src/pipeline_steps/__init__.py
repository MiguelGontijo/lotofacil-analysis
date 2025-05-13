# src/pipeline_steps/__init__.py
import logging
logger = logging.getLogger(__name__)

from .execute_frequency import run_frequency_analysis
from .execute_delay import run_delay_analysis
from .execute_properties import run_number_properties_analysis
from .execute_pairs import run_pair_combination_analysis
from .execute_cycle_stats import run_cycle_identification_and_stats 
from .execute_cycles import run_cycle_closing_analysis 
from .execute_cycle_progression import run_cycle_progression_analysis_step # Da iteração anterior

# NOVA ETAPA DE MÉTRICAS DETALHADAS POR CICLO
from .execute_detailed_cycle_metrics import run_detailed_cycle_metrics_step # <<< ADICIONADO

from .execute_chunk_evolution_analysis import run_chunk_evolution_analysis
from .execute_chunk_evolution_visualization import run_chunk_evolution_visualization
from .execute_rank_trend_analysis import run_rank_trend_analysis_step
from .execute_block_aggregation import run_block_aggregation_step
from .execute_metrics_viz import run_core_metrics_visualization

def run_repetition_analysis(*args, **kwargs):
    logger.error("ETAPA STUB: 'run_repetition_analysis' chamada.")
    return False
def run_group_trend_analysis(*args, **kwargs):
    logger.error("ETAPA STUB: 'run_group_trend_analysis' chamada.")
    return False

__all__ = [
    "run_frequency_analysis", "run_delay_analysis", "run_number_properties_analysis",
    "run_repetition_analysis", "run_pair_combination_analysis",
    "run_cycle_identification_and_stats", "run_cycle_closing_analysis",
    "run_group_trend_analysis", "run_rank_trend_analysis_step",
    "run_core_metrics_visualization", "run_chunk_evolution_analysis",
    "run_chunk_evolution_visualization", "run_block_aggregation_step",
    "run_cycle_progression_analysis_step",
    "run_detailed_cycle_metrics_step", # <<< ADICIONADO
]