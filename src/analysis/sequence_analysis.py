# src/analysis/sequence_analysis.py
import pandas as pd
from typing import List, Dict, Tuple, Any
import logging
from collections import defaultdict

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
            is_consecutive_flag = True 
            for k in range(len(current_subsequence) - 1):
                if current_subsequence[k+1] - current_subsequence[k] != 1:
                    is_consecutive_flag = False
                    break
            if is_consecutive_flag:
                found_sequences_by_length[length_to_check].append(tuple(current_subsequence))
    return found_sequences_by_length

# <<< NOVA FUNÇÃO AUXILIAR PARA SEQUÊNCIAS ARITMÉTICAS >>>
def _find_arithmetic_sequences_in_draw(
    draw_numbers_sorted: List[int],
    min_len: int,
    max_len: int,
    step_value: int
) -> Dict[int, List[Tuple[int, ...]]]:
    """
    Encontra todas as subsequências aritméticas com um 'step_value' específico
    de comprimentos entre min_len e max_len em uma lista ordenada de números de um sorteio.
    """
    found_sequences_by_length: Dict[int, List[Tuple[int, ...]]] = {
        length: [] for length in range(min_len, max_len + 1)
    }
    n = len(draw_numbers_sorted)

    if n == 0 or n < min_len or step_value <= 0:
        return found_sequences_by_length

    for length_to_check in range(min_len, max_len + 1):
        if n < length_to_check:
            continue
        
        # Para encontrar sequências aritméticas, precisamos de uma abordagem diferente da janela deslizante simples.
        # Podemos iterar sobre todas as combinações de 'length_to_check' números do sorteio
        # e verificar se formam uma progressão aritmética com o 'step_value'.
        # No entanto, isso pode ser computacionalmente caro (C(15, k)).
        # Uma abordagem mais eficiente é iterar sobre os números e tentar construir sequências.

        # Abordagem: Iterar sobre cada número como um potencial início de sequência
        for i in range(n):
            potential_sequence = [draw_numbers_sorted[i]]
            current_num = draw_numbers_sorted[i]
            
            # Tenta construir uma sequência do tamanho 'length_to_check'
            for _ in range(1, length_to_check):
                next_expected_num = current_num + step_value
                # Otimização: buscar 'next_expected_num' no restante da lista 'draw_numbers_sorted'
                # Isso é mais eficiente do que iterar sobre todas as subsequências.
                # Como draw_numbers_sorted é ordenada, podemos fazer uma busca eficiente.
                found_next = False
                for j in range(i + len(potential_sequence), n): # Busca a partir do próximo índice
                    if draw_numbers_sorted[j] == next_expected_num:
                        potential_sequence.append(next_expected_num)
                        current_num = next_expected_num
                        found_next = True
                        break # Encontrou o próximo número esperado, para este passo da sequência
                    elif draw_numbers_sorted[j] > next_expected_num:
                        break # Passou do ponto onde o número poderia estar
                
                if not found_next: # Se não encontrou o próximo número esperado para a sequência
                    break # Quebra a tentativa de construir esta sequência particular
            
            if len(potential_sequence) == length_to_check:
                found_sequences_by_length[length_to_check].append(tuple(potential_sequence))
                
    return found_sequences_by_length
# <<< FIM DA NOVA FUNÇÃO AUXILIAR >>>

def analyze_sequences(
    all_draws_df: pd.DataFrame, 
    config_obj_instance: Config 
) -> pd.DataFrame:
    logger.info("==== INICIANDO ANÁLISE DE SEQUÊNCIAS NUMÉRICAS ====")
    
    # logger.info(f"Usando SEQUENCE_ANALYSIS_CONFIG do módulo config: {SEQUENCE_ANALYSIS_CONFIG}")
    
    results_list = [] 
    
    # --- Análise de Sequências Consecutivas ---
    consecutive_config = SEQUENCE_ANALYSIS_CONFIG.get("consecutive", {})
    min_len_consecutive = consecutive_config.get("min_len", 3)
    max_len_consecutive = consecutive_config.get("max_len", 5)
    is_active_consecutive = consecutive_config.get("active", False) 

    logger.info(f"Config para 'consecutive': min_len={min_len_consecutive}, max_len={max_len_consecutive}, active={is_active_consecutive} (Tipo: {type(is_active_consecutive)})")

    if 'drawn_numbers' not in all_draws_df.columns:
        msg = "DataFrame 'all_draws_df' deve conter a coluna 'drawn_numbers' com listas de dezenas ordenadas."
        logger.error(msg)
        # Se a coluna crucial faltar e QUALQUER análise de sequência estiver ativa, melhor parar.
        if is_active_consecutive or SEQUENCE_ANALYSIS_CONFIG.get("arithmetic_steps", {}).get("active", False):
             raise ValueError(msg)
        else:
            logger.warning(msg + " Nenhuma análise de sequência ativa que dependa desta coluna.")

    if is_active_consecutive:
        if 'drawn_numbers' in all_draws_df.columns:
            logger.info(f"Analisando sequências CONSECUTIVAS de comprimento {min_len_consecutive} a {max_len_consecutive}.")
            sequence_counts_consecutive: Dict[int, Dict[Tuple[int, ...], int]] = {
                length: defaultdict(int) for length in range(min_len_consecutive, max_len_consecutive + 1)
            }
            draws_with_consecutive_sequence_of_length: Dict[int, int] = defaultdict(int)
            total_draws = len(all_draws_df)

            if total_draws > 0:
                for _, row in all_draws_df.iterrows():
                    draw_numbers_sorted = row.get('drawn_numbers')
                    if not isinstance(draw_numbers_sorted, list) or not all(isinstance(n, int) for n in draw_numbers_sorted):
                        contest_id_val = row.get(CONTEST_ID_COLUMN_NAME, "Desconhecido") 
                        logger.warning(f"Sorteio {contest_id_val} tem 'drawn_numbers' inválido. Pulando para sequências. Conteúdo: {draw_numbers_sorted}")
                        continue
                    draw_numbers_sorted.sort() 
                    sequences_found_in_this_draw = _find_consecutive_sequences_in_draw(draw_numbers_sorted, min_len_consecutive, max_len_consecutive)
                    for length, sequences_list_found in sequences_found_in_this_draw.items(): 
                        if sequences_list_found: 
                            draws_with_consecutive_sequence_of_length[length] += 1
                            for seq_tuple in sequences_list_found:
                                sequence_counts_consecutive[length][seq_tuple] += 1
                
                for length_iter in range(min_len_consecutive, max_len_consecutive + 1):
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
        else: 
            logger.warning("Coluna 'drawn_numbers' não encontrada, pulando análise de sequências consecutivas.")
    else:
        logger.warning("Análise de sequências CONSECUTIVAS está DESATIVADA na configuração (active=False).")

    # <<< INÍCIO DA LÓGICA PARA "arithmetic_steps" >>>
    arithmetic_config = SEQUENCE_ANALYSIS_CONFIG.get("arithmetic_steps", {}) 
    is_arithmetic_active = arithmetic_config.get("active", False)
    logger.info(f"Config para 'arithmetic_steps': active={is_arithmetic_active}")

    if is_arithmetic_active:
        if 'drawn_numbers' not in all_draws_df.columns:
            logger.error("Coluna 'drawn_numbers' necessária para análise aritmética, mas ausente. Pulando análise aritmética.")
        else:
            steps_to_check = arithmetic_config.get("steps_to_check", [])
            min_len_arith = arithmetic_config.get("min_len", 3)
            max_len_arith = arithmetic_config.get("max_len", 4)
            
            logger.info(f"Analisando sequências ARITMÉTICAS com steps {steps_to_check}, comprimentos de {min_len_arith} a {max_len_arith}.")

            total_draws = len(all_draws_df) # Já definido, mas para clareza se este bloco for isolado

            for step_value in steps_to_check:
                if step_value <= 0:
                    logger.warning(f"Step inválido ({step_value}) na configuração. Pulando este step.")
                    continue
                
                logger.info(f"Analisando para step: {step_value}")
                sequence_counts_arith: Dict[int, Dict[Tuple[int, ...], int]] = {
                    length: defaultdict(int) for length in range(min_len_arith, max_len_arith + 1)
                }
                draws_with_arithmetic_sequence_of_length: Dict[int, int] = defaultdict(int)

                if total_draws > 0:
                    for _, row in all_draws_df.iterrows():
                        draw_numbers_sorted = row.get('drawn_numbers')
                        if not isinstance(draw_numbers_sorted, list) or not all(isinstance(n, int) for n in draw_numbers_sorted):
                            continue # Já logado acima
                        draw_numbers_sorted.sort()
                        
                        sequences_found_this_draw_step = _find_arithmetic_sequences_in_draw(
                            draw_numbers_sorted, min_len_arith, max_len_arith, step_value
                        )
                        for length, sequences_list_found in sequences_found_this_draw_step.items():
                            if sequences_list_found:
                                draws_with_arithmetic_sequence_of_length[length] += 1
                                for seq_tuple in sequences_list_found:
                                    sequence_counts_arith[length][seq_tuple] += 1
                    
                    for length_iter in range(min_len_arith, max_len_arith + 1):
                        total_draws_with_this_len_step_seq = draws_with_arithmetic_sequence_of_length[length_iter]
                        support_for_len_step = (total_draws_with_this_len_step_seq / total_draws) if total_draws > 0 else 0.0
                        results_list.append({
                            "sequence_description": f"Qualquer sequência aritmética (step {step_value}) de {length_iter} dezenas",
                            "sequence_type": f"arithmetic_step_{step_value}_any",
                            "length": length_iter,
                            "step": step_value,
                            "specific_sequence": "N/A",
                            "frequency_count": total_draws_with_this_len_step_seq,
                            "support": round(support_for_len_step, 6)
                        })
                        for seq_tuple, count in sorted(sequence_counts_arith[length_iter].items()):
                            seq_str = "-".join(map(str, seq_tuple))
                            support_specific_step = (count / total_draws) if total_draws > 0 else 0.0
                            results_list.append({
                                "sequence_description": f"Sequência aritmética específica (step {step_value}): {seq_str}",
                                "sequence_type": f"arithmetic_step_{step_value}_specific",
                                "length": length_iter,
                                "step": step_value,
                                "specific_sequence": seq_str,
                                "frequency_count": count,
                                "support": round(support_specific_step, 6)
                            })
                else: # total_draws == 0
                    logger.warning(f"Nenhum sorteio para analisar sequências aritméticas com step {step_value}.")
    else: # not is_arithmetic_active
        logger.warning("Análise de sequências ARITMÉTICAS está DESATIVADA na configuração (active=False).")
    # <<< FIM DA LÓGICA PARA "arithmetic_steps" >>>

    logger.info("Análise de sequências numéricas concluída.")
    if not results_list: 
        logger.info("Nenhuma métrica de sequência (ativa e implementada) foi gerada.")
        return pd.DataFrame(columns=["sequence_description", "sequence_type", "length", "step", "specific_sequence", "frequency_count", "support"])

    return pd.DataFrame(results_list)