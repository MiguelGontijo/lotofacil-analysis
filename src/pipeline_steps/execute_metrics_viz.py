# src/pipeline_steps/execute_metrics_viz.py
import logging # Adicionado
from pathlib import Path # Adicionado
from typing import Optional # Adicionado
from src.database_manager import DatabaseManager
from src.visualization.plotter import plot_frequency, plot_delay # Supondo estas funções em plotter.py
from src.config import PLOT_DIR_CONFIG # Usar como fallback

logger = logging.getLogger(__name__) # Corrigido

def run_core_metrics_visualization(
    db_manager: DatabaseManager,
    output_dir_from_pipeline: Optional[Path] = None, # Recebe do pipeline
    **kwargs
) -> bool:
    """
    Executa a visualização das principais métricas (frequência, atraso).
    """
    try:
        logger.info("Iniciando visualização de métricas principais.")
        
        output_dir_to_use = output_dir_from_pipeline if output_dir_from_pipeline else PLOT_DIR_CONFIG
        Path(output_dir_to_use).mkdir(parents=True, exist_ok=True)

        # Plotar Frequência Absoluta
        df_freq_abs = db_manager.load_dataframe_from_db('frequencia_absoluta')
        if df_freq_abs is not None and not df_freq_abs.empty:
            # Supondo que plot_frequency precisa do DataFrame e do diretório de saída
            # A função plot_frequency em plotter.py já tem output_dir com default PLOT_DIR_CONFIG
            # por isso, podemos chamar diretamente ou passar o output_dir_to_use se quisermos sobrescrever.
            plot_frequency(df_freq_abs, metric_type='Absoluta', output_dir=str(output_dir_to_use)) 
            logger.info("Gráfico de frequência absoluta gerado.")
        else:
            logger.warning("Dados de frequência absoluta não encontrados ou vazios. Gráfico não gerado.")

        # Plotar Atraso Atual
        df_delay_curr = db_manager.load_dataframe_from_db('atraso_atual')
        if df_delay_curr is not None and not df_delay_curr.empty:
            # Supondo que plot_delay precisa do DataFrame, tipo de atraso e diretório de saída
            plot_delay(df_delay_curr, delay_type='Atual', output_dir=str(output_dir_to_use))
            logger.info("Gráfico de atraso atual gerado.")
        else:
            logger.warning("Dados de atraso atual não encontrados ou vazios. Gráfico não gerado.")
            
        # Adicionar mais plots conforme necessário (ex: Atraso Máximo, Frequência Relativa)

        logger.info("Visualização de métricas principais concluída.")
        return True
    except Exception as e:
        logger.error(f"Erro na visualização de métricas principais: {e}", exc_info=True)
        return False