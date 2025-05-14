# src/pipeline_steps/__init__.py
import logging
logger = logging.getLogger(__name__)

from .execute_frequency import run_frequency_analysis
from .execute_delay import run_delay_analysis
from .execute_properties import run_number_properties_analysis
from .execute_pairs import run_pair_combination_analysis
from .execute_cycle_stats import run_cycle_identification_and_stats 
from .execute_cycles import run_cycle_closing_analysis 
from .execute_cycle_progression import run_cycle_progression_analysis_step
from .execute_detailed_cycle_metrics import run_detailed_cycle_metrics_step

from .execute_chunk_evolution_analysis import run_chunk_evolution_analysis
from .execute_chunk_evolution_visualization import run_chunk_evolution_visualization
from .execute_rank_trend_analysis import run_rank_trend_analysis_step
from .execute_block_aggregation import run_block_aggregation_step
from .execute_metrics_viz import run_core_metrics_visualization

# --- Implementação Real para Repetition Analysis ---
from .execute_repetition_analysis import run_repetition_analysis_step # <<< NOVO IMPORT REAL

# --- Etapas que ainda são STUBS ---
def run_group_trend_analysis(*args, **kwargs):
    logger.error("ETAPA STUB: 'run_group_trend_analysis' chamada.")
    return False
# O run_rank_trend_analysis_step já é real.
# O run_cycle_closing_analysis ainda pode ser um placeholder se a lógica em cycle_analysis.py não for completa.

__all__ = [
    "run_frequency_analysis", "run_delay_analysis", "run_number_properties_analysis",
    "run_repetition_analysis_step", # <<< ATUALIZADO PARA O NOME DA FUNÇÃO REAL
    "run_pair_combination_analysis",
    "run_cycle_identification_and_stats", "run_cycle_closing_analysis",
    "run_group_trend_analysis", "run_rank_trend_analysis_step",
    "run_core_metrics_visualization", "run_chunk_evolution_analysis",
    "run_chunk_evolution_visualization", "run_block_aggregation_step",
    "run_cycle_progression_analysis_step",
    "run_detailed_cycle_metrics_step",
]