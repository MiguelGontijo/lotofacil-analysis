# src/pipeline_steps/__init__.py
import logging
logger = logging.getLogger(__name__)

# Importar funções dos módulos de etapa existentes
from .execute_frequency import run_frequency_analysis
from .execute_delay import run_delay_analysis
from .execute_properties import run_number_properties_analysis
from .execute_pairs import run_pair_combination_analysis
from .execute_cycle_stats import run_cycle_identification_and_stats
from .execute_cycles import run_cycle_closing_analysis

# Novas etapas de Chunk (já devem estar corretas)
from .execute_chunk_evolution_analysis import run_chunk_evolution_analysis
from .execute_chunk_evolution_visualization import run_chunk_evolution_visualization

# Etapa de Rank Trend Analysis (implementada na interação anterior)
from .execute_rank_trend_analysis import run_rank_trend_analysis_step

# NOVA ETAPA DE AGREGAÇÃO DE BLOCO
from .execute_block_aggregation import run_block_aggregation_step # <<< ADICIONADO

# Visualizações Core
from .execute_metrics_viz import run_core_metrics_visualization

# --- Etapas que ainda são STUBS ---
def run_repetition_analysis(*args, **kwargs):
    logger.error("ETAPA STUB: 'run_repetition_analysis' chamada, mas não totalmente implementada.")
    return False

def run_group_trend_analysis(*args, **kwargs):
    logger.error("ETAPA STUB: 'run_group_trend_analysis' chamada, mas não totalmente implementada.")
    return False
# O run_rank_trend_analysis_step já é real, então o stub para run_rank_trend_analysis não é mais necessário se o nome foi atualizado no main.py

__all__ = [
    "run_frequency_analysis",
    "run_delay_analysis",
    "run_number_properties_analysis",
    "run_repetition_analysis",
    "run_pair_combination_analysis",
    "run_cycle_identification_and_stats",
    "run_cycle_closing_analysis",
    "run_group_trend_analysis",
    "run_rank_trend_analysis_step", # Mantém o nome da função real
    "run_core_metrics_visualization",
    "run_chunk_evolution_analysis",
    "run_chunk_evolution_visualization",
    "run_block_aggregation_step", # <<< ADICIONADO
]