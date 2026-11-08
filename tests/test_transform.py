from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal

import pytest
from conftest import IPCA_2023, br, dias_uteis

from bcb_pipeline.config import SERIES
from bcb_pipeline.transform import bronze_para_prata, montar_bronze, prata_para_ouro

T1 = datetime(2024, 1, 10, 8, 0)
T2 = datetime(2024, 1, 11, 8, 0)


def bronze(spark, nome, registros, lote="L1", quando=T1):
    return montar_bronze(spark, SERIES[nome], registros, lote, quando)


def prata(spark, registros_por_serie: dict[str, list[dict]]):
    df = None
    for nome, registros in registros_por_serie.items():
        b = bronze(spark, nome, registros)
        df = b if df is None else df.unionByName(b)
    validos, _ = bronze_para_prata(df)
    return validos


# ----------------------------------------------------------------- prata ---


def test_prata_converte_data_e_valor(spark):
    validos, rejeitados = bronze_para_prata(bronze(spark, "dolar", [{"data": "02/01/2024", "valor": "4.8910"}]))
    linha = validos.first()

    assert linha["data"] == date(2024, 1, 2)
    assert linha["valor"] == Decimal("4.891000")
    assert rejeitados.count() == 0


def test_prata_envia_registros_invalidos_para_quarentena(spark):
    registros = [
        {"data": "02/01/2024", "valor": "4.89"},
        {"data": "31/02/2024", "valor": "4.90"},  # data inexistente
        {"data": "03/01/2024", "valor": "abc"},  # valor não numérico
        {"data": None, "valor": "4.91"},
    ]
    validos, rejeitados = bronze_para_prata(bronze(spark, "dolar", registros))

    assert validos.count() == 1
    assert rejeitados.count() == 3
    assert {r["_valor_original"] for r in rejeitados.collect()} == {"4.90", "abc", "4.91"}


def test_prata_mantem_a_versao_mais_recente_de_cada_data(spark):
    antiga = bronze(spark, "dolar", [{"data": "02/01/2024", "valor": "4.80"}], "L1", T1)
    nova = bronze(spark, "dolar", [{"data": "02/01/2024", "valor": "4.85"}], "L2", T2)
    validos, _ = bronze_para_prata(antiga.unionByName(nova))

    linhas = validos.collect()
    assert len(linhas) == 1
    assert linhas[0]["valor"] == Decimal("4.85")
    assert linhas[0]["_lote_id"] == "L2"


# ------------------------------------------------------------------ ouro ---


def test_ouro_calcula_ipca_12m_igual_ao_acumulado_oficial(spark):
    ipca = [{"data": br(date(2023, m, 1)), "valor": str(v)} for m, v in enumerate(IPCA_2023, start=1)]
    ouro = {r["mes"]: r for r in prata_para_ouro(prata(spark, {"ipca": ipca})).collect()}

    assert ouro[date(2023, 12, 1)]["ipca_12m"] == pytest.approx(4.62, abs=0.005)
    # Antes de completar 12 meses, o acumulado fica vazio em vez de mostrar um número parcial.
    assert ouro[date(2023, 11, 1)]["ipca_12m"] is None


def test_ouro_dolar_medio_fechamento_e_variacao(spark):
    jan = [{"data": "02/01/2024", "valor": "4.80"}, {"data": "31/01/2024", "valor": "5.00"}]
    fev = [{"data": "01/02/2024", "valor": "5.10"}, {"data": "29/02/2024", "valor": "5.25"}]
    ouro = {r["mes"]: r for r in prata_para_ouro(prata(spark, {"dolar": jan + fev})).collect()}

    assert ouro[date(2024, 1, 1)]["dolar_medio"] == pytest.approx(4.90)
    assert ouro[date(2024, 1, 1)]["dolar_fechamento"] == pytest.approx(5.00)
    assert ouro[date(2024, 1, 1)]["dolar_var_mensal_pct"] is None
    assert ouro[date(2024, 2, 1)]["dolar_var_mensal_pct"] == pytest.approx(5.0)


def test_ouro_variacao_do_dolar_nao_compara_meses_nao_consecutivos(spark):
    registros = [{"data": "31/01/2024", "valor": "5.00"}, {"data": "28/03/2024", "valor": "5.50"}]
    ouro = {r["mes"]: r for r in prata_para_ouro(prata(spark, {"dolar": registros})).collect()}
    assert ouro[date(2024, 3, 1)]["dolar_var_mensal_pct"] is None


def test_ouro_selic_do_fim_do_mes_e_juro_real(spark):
    selic = [
        {"data": br(d), "valor": "13.75" if d < date(2023, 12, 14) else "11.75"}
        for d in dias_uteis(date(2023, 12, 1), date(2023, 12, 29))
    ]
    ipca = [{"data": br(date(2023, m, 1)), "valor": str(v)} for m, v in enumerate(IPCA_2023, start=1)]
    dez = {r["mes"]: r for r in prata_para_ouro(prata(spark, {"selic_meta": selic, "ipca": ipca})).collect()}[
        date(2023, 12, 1)
    ]

    assert dez["selic_meta"] == pytest.approx(11.75)
    esperado = ((1 + 0.1175) / (1 + dez["ipca_12m"] / 100) - 1) * 100
    assert dez["juro_real_12m"] == pytest.approx(esperado, abs=1e-3)
    assert dez["juro_real_12m"] == pytest.approx(6.82, abs=0.01)
