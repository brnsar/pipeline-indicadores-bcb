"""Teste de ponta a ponta com Delta Lake: duas execuções seguidas, como em produção."""

from __future__ import annotations

from datetime import date

import pytest
from conftest import FonteFalsa, dados_exemplo

from bcb_pipeline.pipeline import Pipeline


def ler(spark, caminho):
    return spark.read.format("delta").load(caminho)


def test_cargas_inicial_e_incremental(spark, delta_disponivel, tmp_path):
    dados = dados_exemplo()
    fonte = FonteFalsa(dados)
    pipeline = Pipeline(spark, tmp_path / "lake", fonte=fonte)

    # --- 1ª execução: carga completa até 31/12/2023, com um registro inválido
    dados[1].append({"data": "99/99/2023", "valor": "5.00"})
    r1 = pipeline.executar(inicio=date(2023, 1, 1), fim=date(2023, 12, 31))

    assert r1.rejeitados == 1
    assert ler(spark, pipeline.caminho_quarentena).count() == 1

    prata = ler(spark, pipeline.caminho_prata)
    total_2023 = prata.count()
    assert prata.select("serie", "data").distinct().count() == total_2023  # sem duplicatas

    ouro = {r["mes"]: r for r in ler(spark, pipeline.caminho_ouro).collect()}
    assert len(ouro) == 12
    assert ouro[date(2023, 12, 1)]["ipca_12m"] == pytest.approx(4.62, abs=0.005)

    # --- O BCB corrige um valor já carregado
    dezembro = next(r for r in dados[1] if r["data"] == "29/12/2023")
    dezembro["valor"] = "4.8413"

    # --- 2ª execução: incremental (sem --inicio) até março/2024
    fonte.chamadas.clear()
    r2 = pipeline.executar(fim=date(2024, 3, 31))

    # Buscou a partir de alguns dias antes do último dado já carregado, não desde o início.
    inicio_dolar = next(ini for codigo, ini, _ in fonte.chamadas if codigo == 1)
    assert date(2023, 11, 1) < inicio_dolar < date(2023, 12, 29)
    assert r2.rejeitados == 0

    prata = ler(spark, pipeline.caminho_prata)
    assert prata.select("serie", "data").distinct().count() == prata.count()  # reprocessar não duplica
    assert prata.count() > total_2023

    corrigido = prata.filter("serie = 'dolar' AND data = DATE'2023-12-29'").first()
    assert float(corrigido["valor"]) == pytest.approx(4.8413)
    assert corrigido["_lote_id"] == r2.lote_id

    ouro = {r["mes"]: r for r in ler(spark, pipeline.caminho_ouro).collect()}
    assert len(ouro) == 15
    assert ouro[date(2024, 3, 1)]["ipca_12m"] is not None

    # A camada bronze guarda o histórico bruto de cada lote (auditoria).
    lotes = {r["_lote_id"] for r in ler(spark, pipeline.caminho_bronze).select("_lote_id").distinct().collect()}
    assert lotes == {r1.lote_id, r2.lote_id}


def test_execucao_sem_dados_novos_nao_falha(spark, delta_disponivel, tmp_path):
    pipeline = Pipeline(spark, tmp_path / "lake", fonte=FonteFalsa({}))
    resumo = pipeline.executar(inicio=date(2024, 1, 1), fim=date(2024, 1, 31))
    assert sum(resumo.extraidos.values()) == 0
