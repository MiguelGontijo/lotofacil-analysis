# tests/test_evaluator.py

import pytest
# Importa a função a ser testada
from src.backtester.evaluator import evaluate_hits

def test_evaluate_hits_basic():
    """ Testa a contagem básica de acertos. """
    escolhidos = {1, 2, 3, 4, 5, 11, 12, 13, 14, 15, 21, 22, 23, 24, 25}
    sorteados = {1, 3, 5, 7, 9, 11, 13, 15, 17, 19, 21, 23, 25, 2, 4} # 11 acertos
    assert evaluate_hits(escolhidos, sorteados) == 11

def test_evaluate_hits_all():
    """ Testa 15 acertos. """
    escolhidos = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
    sorteados = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
    assert evaluate_hits(escolhidos, sorteados) == 15

def test_evaluate_hits_none():
    """ Testa 0 acertos. """
    escolhidos = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
    sorteados = {16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30} # Apenas exemplo
    assert evaluate_hits(escolhidos, sorteados) == 0

def test_evaluate_hits_empty():
    """ Testa com conjuntos vazios. """
    assert evaluate_hits(set(), {1, 2, 3}) == 0
    assert evaluate_hits({1, 2, 3}, set()) == 0
    assert evaluate_hits(set(), set()) == 0

def test_evaluate_hits_invalid_input():
     """ Testa com inputs None (deve retornar -1 ou levantar erro). """
     # Assumindo que retorna -1 para erro, como no código atual
     assert evaluate_hits(None, {1,2}) == -1
     assert evaluate_hits({1,2}, None) == -1
     assert evaluate_hits(None, None) == -1
     # Teste para tipo incorreto
     assert evaluate_hits([1,2], {1,2}) == -1 # Passando lista em vez de set