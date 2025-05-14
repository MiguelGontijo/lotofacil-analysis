# src/pipeline_steps/execute_metrics_viz.py
import logging
from pathlib import Path
from typing import Optional, Any, Dict # Adicionado Any, Dict para type hints

# from src.database_manager import DatabaseManager # Para type hint
# from src.config import Config # Para type hint
# Importa as funções de plotagem específicas
from src.visualization.plotter import plot_frequency, plot_delay # Adicione outras se este step as usar
# Importa a configuração do diretório de plotagem
from src.config import PLOT_DIR_CONFIG # Para fallback se não vier do pipeline

logger = logging.getLogger(__name__)

# RENOMEADO PARA CORRESPONDER AO __init__.py e main.py
def run_metrics_visualization_step(
    db_manager: Any, # Tipo real: DatabaseManager
    config: Any, # Tipo real: Config
    shared_context: Dict[str, Any], # Para consistência com o Orchestrator
    # output_dir_from_pipeline: Optional[Path] = None, # Removido, pois o config.PLOT_DIR é o padrão
    **kwargs
) -> bool:
    """
    Executa a visualização das principais métricas (frequência, atraso).
    Os gráficos são salvos no diretório especificado por config.PLOT_DIR.
    """
    step_name = "Core Metrics Visualization"
    logger.info(f"Iniciando etapa do pipeline: {step_name}.")
    
    try:
        # O diretório de saída é agora obtido do objeto config
        # PLOT_DIR_CONFIG é uma constante de nível de módulo, mas config.PLOT_DIR é o atributo da instância
        output_dir_to_use = Path(config.PLOT_DIR) # Usando config.PLOT_DIR da instância config_obj
        output_dir_to_use.mkdir(parents=True, exist_ok=True)
        logger.info(f"Gráficos serão salvos em: {output_dir_to_use}")

        # Plotar Frequência Absoluta
        # Presume que db_manager.load_dataframe (ou um método similar) existe
        df_freq_abs = db_manager.load_dataframe('frequencia_absoluta') # Supondo nome da tabela
        if df_freq_abs is not None and not df_freq_abs.empty:
            plot_frequency(df_freq_abs, metric_type='Absoluta', output_dir=str(output_dir_to_use)) 
            logger.info("Gráfico de frequência absoluta gerado.")
        else:
            logger.warning("Dados de frequência absoluta não encontrados ou vazios. Gráfico não gerado.")

        # Plotar Atraso Atual
        df_delay_curr = db_manager.load_dataframe('atraso_atual') # Supondo nome da tabela
        if df_delay_curr is not None and not df_delay_curr.empty:
            plot_delay(df_delay_curr, delay_type='Atual', output_dir=str(output_dir_to_use))
            logger.info("Gráfico de atraso atual gerado.")
        else:
            logger.warning("Dados de atraso atual não encontrados ou vazios. Gráfico não gerado.")
            
        # Adicionar mais plots conforme necessário (ex: Atraso Máximo, Frequência Relativa)
        # Exemplo:
        # df_freq_rel = db_manager.load_dataframe('frequencia_relativa')
        # if df_freq_rel is not None and not df_freq_rel.empty:
        #     plot_frequency(df_freq_rel, metric_type='Relativa', output_dir=str(output_dir_to_use))
        #     logger.info("Gráfico de frequência relativa gerado.")
        # else:
        #     logger.warning("Dados de frequência relativa não encontrados. Gráfico não gerado.")

        logger.info(f"Etapa do pipeline: {step_name} concluída com sucesso.")
        return True
    except AttributeError as e:
        logger.error(f"Erro na etapa {step_name}: Atributo de configuração ausente (ex: config.PLOT_DIR). Detalhes: {e}", exc_info=True)
        return False
    except Exception as e:
        logger.error(f"Erro ao executar a etapa {step_name}: {e}", exc_info=True)
        return False