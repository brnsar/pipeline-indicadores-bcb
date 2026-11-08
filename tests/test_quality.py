from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal

import pytest

from bcb_pipeline.quality import QualidadeError, checar_prata, garantir

COLUNAS = "serie string, data date, valor decimal(18,6)"


def resultado(spark, linhas):
    return {c.nome: c.ok for c in checar_prata(spark.createDataFrame(linhas, COLUNAS))}


def test_dados_corretos_passam_em_todas_as_checagens(spark):
    linhas = [
        ("dolar", date(2024, 1, 2), Decimal("4.89")),
        ("selic_meta", date(2024, 1, 2), Decimal("11.75")),
        ("ipca", date(2024, 1, 1), Decimal("0.42")),
    ]
    assert all(resultado(spark, linhas).values())


def test_detecta_duplicata(spark):
    linhas = [("dolar", date(2024, 1, 2), Decimal("4.89"))] * 2
    assert resultado(spark, linhas)["chave_unica"] is False


def test_detecta_valor_fora_da_faixa(spark):
    # Dólar a R$ 489 é claramente um erro (por exemplo, separador decimal perdido).
    linhas = [("dolar", date(2024, 1, 2), Decimal("489.0"))]
    assert resultado(spark, linhas)["valores_na_faixa"] is False


def test_detecta_serie_desconhecida_e_data_futura(spark):
    linhas = [("euro", date.today() + timedelta(days=5), Decimal("5.0"))]
    r = resultado(spark, linhas)
    assert r["series_conhecidas"] is False
    assert r["sem_datas_futuras"] is False


def test_garantir_interrompe_quando_ha_falha(spark):
    linhas = [("dolar", date(2024, 1, 2), Decimal("489.0"))]
    with pytest.raises(QualidadeError, match="valores_na_faixa"):
        garantir(checar_prata(spark.createDataFrame(linhas, COLUNAS)), "prata")
