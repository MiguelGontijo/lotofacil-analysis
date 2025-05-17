# src/analysis/sequence_analysis.py
import pandas as pd
from typing import List, Dict, Tuple, Any
import logging
from collections import defaultdict

# <<< IMPORTAR AS CONSTANTES DE CONFIGURAÇÃO DIRETAMENTE >>>
try:
    from ..config import Config, CONTEST_ID_COLUMN_NAME, \
                         SEQUENCE_ANALYSIS_CONFIG, SEQUENCE_METRICS_TABLE_NAME
except ImportError:
    from src.config import Config, CONTEST_ID_COLUMN_NAME, \
                         SEQUENCE_ANALYSIS_CONFIG, SEQUENCE_METRICS_TABLE_NAME

logger = logging.getLogger(__name__)

def _find_consecutive_sequences_in_draw(
    draw_numbers_sorted: List[int], 
    min_len: int, 
    max_len: int
) -> Dict[int, List[Tuple[int, ...]]]:
    found_sequences_by_length: Dict[int, List[Tuple[int, ...]]] = {
        length: [] for length in range(min_len, max_len + 1)
    }
    n = len(draw_numbers_sorted)
    if n == 0 or n < min_len:
        return found_sequences_by_length
    for length_to_check in range(min_len, max_len + 1):
        if n < length_to_check:
            continue
        for i in range(n - length_to_check + 1):
            current_subsequence = draw_numbers_sorted[i : i + length_to_check]
            is_consecutive = True
            for k in range(len(current_subsequence) - 1):
                if current_subsequence[k+1] - current_subsequence[k] != 1:
                    is_consecutive = False
                    break
            if is_consecutive:
                found_sequences_by_length[length_to_check].append(tuple(current_subsequence))
    return found_sequences_by_length

def analyze_sequences(
    all_draws_df: pd.DataFrame, 
    config_obj_instance: Config # O objeto config passado pelo orchestrator
) -> pd.DataFrame:
    logger.info("==== INICIANDO ANÁLISE DE SEQUÊNCIAS NUMÉRICAS ====")
    
    # <<< ACESSANDO AS CONSTANTES DE CONFIGURAÇÃO IMPORTADAS DIRETAMENTE >>>
    logger.info(f"Usando SEQUENCE_ANALYSIS_CONFIG do módulo config: {SEQUENCE_ANALYSIS_CONFIG}")
    
    consecutive_config = SEQUENCE_ANALYSIS_CONFIG.get("consecutive") # Usar a constante importada
    
    if not consecutive_config or not isinstance(consecutive_config, dict):
        logger.error("Configuração para 'consecutive' não encontrada ou não é um dicionário em SEQUENCE_ANALYSIS_CONFIG. Verifique config.py.")
        return pd.DataFrame(columns=["sequence_description", "sequence_type", "length", "step", "specific_sequence", "frequency_count", "support"])

    min_len = consecutive_config.get("min_len", 3)
    max_len = consecutive_config.get("max_len", 5)
    is_active = consecutive_config.get("active", False) 

    logger.info(f"Valores lidos para 'consecutive': min_len={min_len}, max_len={max_len}, active={is_active} (Tipo de 'active': {type(is_active)})")
    
    results_list = [] 

    if not is_active:
        logger.warning("Análise de sequências CONSECUTIVAS está DESATIVADA na configuração (active=False). Pulando esta parte.")
    else:
        logger.info(f"Analisando sequências CONSECUTIVAS de comprimento {min_len} a {max_len}.")
        if 'drawn_numbers' not in all_draws_df.columns:
            msg = "DataFrame 'all_draws_df' deve conter a coluna 'drawn_numbers' com listas de dezenas ordenadas."
            logger.error(msg)
            raise ValueError(msg)

        sequence_counts_consecutive: Dict[int, Dict[Tuple[int, ...], int]] = {
            length: defaultdict(int) for length in range(min_len, max_len + 1)
        }
        draws_with_consecutive_sequence_of_length: Dict[int, int] = defaultdict(int)
        total_draws = len(all_draws_df)

        if total_draws > 0:
            for _, row in all_draws_df.iterrows():
                draw_numbers_sorted = row.get('drawn_numbers')
                if not isinstance(draw_numbers_sorted, list) or not all(isinstance(n, int) for n in draw_numbers_sorted):
                    # Usar CONTEST_ID_COLUMN_NAME importado do config
                    contest_id_val = row.get(CONTEST_ID_COLUMN_NAME, "Desconhecido") 
                    logger.warning(f"Sorteio {contest_id_val} tem 'drawn_numbers' inválido. Pulando. Conteúdo: {draw_numbers_sorted}")
                    continue
                draw_numbers_sorted.sort() 
                sequences_found_in_this_draw = _find_consecutive_sequences_in_draw(draw_numbers_sorted, min_len, max_len)
                for length, sequences_list in sequences_found_in_this_draw.items():
                    if sequences_list: 
                        draws_with_consecutive_sequence_of_length[length] += 1
                        for seq_tuple in sequences_list:
                            sequence_counts_consecutive[length][seq_tuple] += 1
            
            for length_iter in range(min_len, max_len + 1):
                total_draws_with_this_len_seq = draws_with_consecutive_sequence_of_length[length_iter]
                support_for_len = (total_draws_with_this_len_seq / total_draws) if total_draws > 0 else 0.0
                results_list.append({
                    "sequence_description": f"Qualquer sequência consecutiva de {length_iter} dezenas",
                    "sequence_type": "consecutive_any", "length": length_iter, "step": 1, 
                    "specific_sequence": "N/A", 
                    "frequency_count": total_draws_with_this_len_seq, "support": round(support_for_len, 6) 
                })
                for seq_tuple, count in sorted(sequence_counts_consecutive[length_iter].items()):
                    seq_str = "-".join(map(str, seq_tuple))
                    support_specific = (count / total_draws) if total_draws > 0 else 0.0
                    results_list.append({
                        "sequence_description": f"Sequência específica: {seq_str}",
                        "sequence_type": "consecutive_specific", "length": length_iter, "step": 1,
                        "specific_sequence": seq_str, "frequency_count": count, "support": round(support_specific, 6)
                    })
        else:
            logger.warning("Nenhum sorteio para analisar sequências consecutivas.")

    # ----- Lógica para "arithmetic_steps" -----
    arithmetic_config = SEQUENCE_ANALYSIS_CONFIG.get("arithmetic_steps", {}) # Usar a constante importada
    is_arithmetic_active = arithmetic_config.get("active", False)
    logger.info(f"Config para 'arithmetic_steps': active={is_arithmetic_active}")

    if is_arithmetic_active:
        logger.info("Análise de sequências aritméticas com steps está ATIVADA. Lógica de implementação pendente.")
        # TODO: Implementar _find_arithmetic_sequences_in_draw e a agregação.
        pass

    logger.info("Análise de sequências numéricas concluída.")
    if not results_list and not is_arithmetic_active: # Modificado para checar se alguma análise ativa produziu resultados
        logger.info("Nenhuma métrica de sequência (ativa e configurada) foi gerada.")
        return pd.DataFrame(columns=["sequence_description", "sequence_type", "length", "step", "specific_sequence", "frequency_count", "support"])

    return pd.DataFrame(results_list)