# src/analysis/cycle_analysis.py
import pandas as pd
from typing import List, Dict, Tuple, Set, Optional, Any
import logging
import numpy as np

# Importar constantes necessárias e existentes de config.py
from src.config import ALL_NUMBERS
# As importações problemáticas de logger, DATABASE_PATH, etc., foram removidas.

logger = logging.getLogger(__name__) # Logger específico para este módulo

ALL_NUMBERS_SET: Set[int] = set(ALL_NUMBERS) # Conjunto de todos os números para checagens eficientes

# --- Funções Auxiliares (ex: get_prime_numbers, se necessário para análises de ciclo futuras) ---
# Se a análise de ciclo precisar de propriedades como primos, elas podem ser definidas ou importadas.
# No momento, a lógica principal de identificação de ciclo não as usa diretamente.
def get_prime_numbers(limit: int) -> List[int]:
    primes = []
    for num in range(2, limit + 1):
        is_p = True
        for i in range(2, int(num**0.5) + 1):
            if num % i == 0:
                is_p = False
                break
        if is_p:
            primes.append(num)
    return primes

PRIMES_UP_TO_25 = get_prime_numbers(max(ALL_NUMBERS) if ALL_NUMBERS else 25)
# logger.debug(f"Números primos (cycle_analysis): {PRIMES_UP_TO_25}")

# ---------------------------------------------------------------------------
# Implementações para Funções de Análise de Ciclo
# ---------------------------------------------------------------------------

def identify_and_process_cycles(all_data_df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """
    Função principal para identificar ciclos, calcular estatísticas e preparar dados de fechamento.

    Args:
        all_data_df: DataFrame com todos os concursos. 
                     Esperadas colunas 'Concurso' e 'bola_1'...'bola_15'.

    Returns:
        Um dicionário onde as chaves são nomes de DataFrames (ex: 'ciclos_detalhe', 'ciclos_sumario_estatisticas')
        e os valores são os DataFrames correspondentes.
        Retorna um dicionário vazio se não for possível processar.
    """
    logger.info("Iniciando análise completa de ciclos (Versão Refatorada).")
    if all_data_df is None or all_data_df.empty:
        logger.warning("DataFrame de entrada para análise de ciclos está vazio.")
        return {}

    required_cols = ['Concurso'] + [f'bola_{i}' for i in range(1, 16)]
    missing_cols = [col for col in required_cols if col not in all_data_df.columns]
    if missing_cols:
        logger.error(f"Colunas essenciais ausentes no DataFrame: {missing_cols}. Não é possível analisar ciclos.")
        return {}

    df_sorted = all_data_df.sort_values(by='Concurso').reset_index(drop=True)
    dezena_cols = [f'bola_{i}' for i in range(1, 16)]

    cycles_data: List[Dict[str, Any]] = []
    current_cycle_numbers_needed = ALL_NUMBERS_SET.copy()
    current_cycle_start_contest = df_sorted.loc[0, 'Concurso'] if not df_sorted.empty else 0
    cycle_count = 0
    last_processed_contest_num_for_open_cycle = df_sorted['Concurso'].max() if not df_sorted.empty else 0

    logger.debug(f"Análise de ciclo: Iniciando loop por {len(df_sorted)} concursos.")
    for index, row in df_sorted.iterrows():
        contest_number = int(row['Concurso'])
        
        try:
            drawn_numbers_in_contest = set()
            for col in dezena_cols:
                if pd.notna(row[col]):
                    drawn_numbers_in_contest.add(int(row[col]))
            
            if not drawn_numbers_in_contest and len(dezena_cols) > 0 :
                # logger.debug(f"Sorteio do concurso {contest_number} não contém dezenas válidas. Pulando.") # Muito verboso
                continue
        except ValueError:
            logger.warning(f"Erro ao converter dezenas para int no concurso {contest_number}. Pulando sorteio.")
            continue
        
        # Ajusta o início do ciclo se necessário (após um ciclo fechar)
        if current_cycle_numbers_needed == ALL_NUMBERS_SET and contest_number != current_cycle_start_contest:
             current_cycle_start_contest = contest_number

        current_cycle_numbers_needed.difference_update(drawn_numbers_in_contest)

        if not current_cycle_numbers_needed: # Ciclo fechou
            cycle_count += 1
            closed_cycle_info = {
                'ciclo_num': cycle_count,
                'concurso_inicio': current_cycle_start_contest,
                'concurso_fim': contest_number,
                'duracao_concursos': contest_number - current_cycle_start_contest + 1,
                'numeros_faltantes': None, 
                'qtd_faltantes': 0
            }
            cycles_data.append(closed_cycle_info)
            logger.debug(f"Ciclo {cycle_count} FECHADO no concurso {contest_number}. "
                         f"Início: {current_cycle_start_contest}, Duração: {closed_cycle_info['duracao_concursos']}. "
                         f"Total de ciclos em cycles_data: {len(cycles_data)}")

            current_cycle_numbers_needed = ALL_NUMBERS_SET.copy()
            if index + 1 < len(df_sorted): # Prepara para o próximo ciclo
                 current_cycle_start_contest = df_sorted.loc[index + 1, 'Concurso']
            # Se for o último concurso, não há próximo ciclo a iniciar aqui.
            
    logger.debug(f"Fim do loop principal. Total de ciclos fechados (cycle_count): {cycle_count}. "
                 f"Tamanho de cycles_data (deve ser igual a cycle_count): {len(cycles_data)}")

    # Após o loop, verifica se há um ciclo em andamento
    if current_cycle_numbers_needed and current_cycle_numbers_needed != ALL_NUMBERS_SET:
        if not df_sorted.empty and current_cycle_start_contest <= last_processed_contest_num_for_open_cycle:
            numeros_faltantes_str = ",".join(map(str, sorted(list(current_cycle_numbers_needed))))
            open_cycle_info = {
                'ciclo_num': cycle_count + 1,
                'concurso_inicio': current_cycle_start_contest,
                'concurso_fim': np.nan, 
                'duracao_concursos': np.nan,
                'numeros_faltantes': numeros_faltantes_str,
                'qtd_faltantes': len(current_cycle_numbers_needed)
            }
            cycles_data.append(open_cycle_info)
            logger.debug(f"Ciclo {cycle_count + 1} EM ABERTO adicionado. "
                         f"Início: {current_cycle_start_contest}, Faltantes: {len(current_cycle_numbers_needed)}. "
                         f"Total de ciclos em cycles_data: {len(cycles_data)}")
        else:
             logger.info("Não há ciclo em andamento para registrar ou início de ciclo inválido.")
    elif cycle_count == 0 and not df_sorted.empty and current_cycle_numbers_needed: # Primeiro ciclo, ainda não fechou
        if current_cycle_numbers_needed != ALL_NUMBERS_SET:
            numeros_faltantes_str = ",".join(map(str, sorted(list(current_cycle_numbers_needed))))
            first_open_cycle_info = {
                'ciclo_num': 1, 
                'concurso_inicio': current_cycle_start_contest,
                'concurso_fim': np.nan,
                'duracao_concursos': np.nan,
                'numeros_faltantes': numeros_faltantes_str,
                'qtd_faltantes': len(current_cycle_numbers_needed)
            }
            cycles_data.append(first_open_cycle_info)
            logger.debug(f"Primeiro ciclo (1) EM ABERTO adicionado. "
                         f"Início: {current_cycle_start_contest}, Faltantes: {len(current_cycle_numbers_needed)}. "
                         f"Total de ciclos em cycles_data: {len(cycles_data)}")
        else: 
            logger.info("Primeiro ciclo apenas iniciado, todos os números ainda faltantes (não será adicionado à lista de ciclos em aberto).")

    results = {}
    logger.debug(f"Antes de criar DataFrame: Tamanho final de cycles_data: {len(cycles_data)}")
    if cycles_data:
        df_cycles_detail = pd.DataFrame(cycles_data)
        logger.debug(f"DataFrame df_cycles_detail criado com {len(df_cycles_detail)} linhas.")
        
        if 'concurso_fim' in df_cycles_detail.columns:
             df_cycles_detail['concurso_fim'] = df_cycles_detail['concurso_fim'].astype('Int64')
        if 'duracao_concursos' in df_cycles_detail.columns:
            df_cycles_detail['duracao_concursos'] = df_cycles_detail['duracao_concursos'].astype('Int64')
        
        if 'numeros_faltantes' in df_cycles_detail.columns:
            df_cycles_detail['numeros_faltantes'] = df_cycles_detail['numeros_faltantes'].apply(
                lambda x: ",".join(map(str, sorted(list(x)))) if isinstance(x, (set, list)) else x
            )
            df_cycles_detail['numeros_faltantes'] = df_cycles_detail['numeros_faltantes'].fillna(value=np.nan).replace([pd.NA], [None])

        results['ciclos_detalhe'] = df_cycles_detail
        logger.info(f"Detalhes de {len(df_cycles_detail)} ciclos/status de ciclo processados (DataFrame final).")

        if 'duracao_concursos' in df_cycles_detail.columns:
            # Filtra para ciclos fechados para calcular estatísticas
            df_closed_cycles = df_cycles_detail[pd.to_numeric(df_cycles_detail['duracao_concursos'], errors='coerce').notna()].copy() # .copy() aqui
            
            if not df_closed_cycles.empty and 'duracao_concursos' in df_closed_cycles.columns:
                df_closed_cycles['duracao_concursos'] = pd.to_numeric(df_closed_cycles['duracao_concursos'])
                
                summary_stats = {
                    'total_ciclos_fechados': int(len(df_closed_cycles)),
                    'duracao_media_ciclo': float(df_closed_cycles['duracao_concursos'].mean()),
                    'duracao_min_ciclo': int(df_closed_cycles['duracao_concursos'].min()),
                    'duracao_max_ciclo': int(df_closed_cycles['duracao_concursos'].max()),
                    'duracao_mediana_ciclo': float(df_closed_cycles['duracao_concursos'].median()),
                }
                df_summary_stats = pd.DataFrame([summary_stats])
                results['ciclos_sumario_estatisticas'] = df_summary_stats
                logger.info("Estatísticas sumárias dos ciclos calculadas.")
            else:
                logger.info("Nenhum ciclo fechado encontrado para calcular estatísticas sumárias.")
        else:
            logger.info("Coluna 'duracao_concursos' não encontrada. Não é possível calcular estatísticas sumárias.")
    else:
        logger.info("Nenhum dado de ciclo gerado (lista cycles_data está vazia).")
        
    logger.info("Análise completa de ciclos concluída (Versão Refatorada).")
    return results

# Funções wrapper (mantidas para compatibilidade com pipeline_steps)
def identify_cycles(all_data_df: pd.DataFrame) -> Optional[pd.DataFrame]:
    logger.info("Chamando identify_cycles (wrapper para identify_and_process_cycles).")
    results = identify_and_process_cycles(all_data_df)
    return results.get('ciclos_detalhe')

def calculate_cycle_stats(df_cycles_detail: pd.DataFrame) -> Optional[pd.DataFrame]:
    logger.info("Chamando calculate_cycle_stats (wrapper para lógica de sumário).")
    if df_cycles_detail is None or df_cycles_detail.empty:
        logger.warning("DataFrame de detalhes de ciclo vazio para calculate_cycle_stats.")
        return None
        
    if 'duracao_concursos' in df_cycles_detail.columns:
        df_closed_cycles = df_cycles_detail[pd.to_numeric(df_cycles_detail['duracao_concursos'], errors='coerce').notna()].copy() # .copy() aqui
        if not df_closed_cycles.empty and 'duracao_concursos' in df_closed_cycles.columns:
            df_closed_cycles['duracao_concursos'] = pd.to_numeric(df_closed_cycles['duracao_concursos'])
            summary_stats = {
                'total_ciclos_fechados': int(len(df_closed_cycles)),
                'duracao_media_ciclo': float(df_closed_cycles['duracao_concursos'].mean()),
                'duracao_min_ciclo': int(df_closed_cycles['duracao_concursos'].min()),
                'duracao_max_ciclo': int(df_closed_cycles['duracao_concursos'].max()),
                'duracao_mediana_ciclo': float(df_closed_cycles['duracao_concursos'].median()),
            }
            logger.info("Sumário de estatísticas de ciclo recalculado por calculate_cycle_stats.")
            return pd.DataFrame([summary_stats])
    logger.info("Nenhum ciclo fechado em df_cycles_detail para calcular estatísticas por calculate_cycle_stats.")
    return None

def analyze_cycle_closing_data(all_data_df: pd.DataFrame) -> Optional[pd.DataFrame]:
    logger.info("Chamando analyze_cycle_closing_data (placeholder).")
    logger.warning("Função 'analyze_cycle_closing_data' é um placeholder e precisa de implementação real.")
    return pd.DataFrame({"analise_fechamento_stub": ["dados de fechamento não implementados"]})