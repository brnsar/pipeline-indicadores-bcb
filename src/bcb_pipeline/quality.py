"""Checagens de qualidade de dados executadas antes de cada gravação."""

from __future__ import annotations

import logging
from dataclasses import dataclass

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .config import SERIES

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Checagem:
    nome: str
    ok: bool
    detalhe: str


class QualidadeError(RuntimeError):
    """Os dados falharam em pelo menos uma checagem e não foram gravados."""

    def __init__(self, falhas: list[Checagem]) -> None:
        self.falhas = falhas
        super().__init__("; ".join(f"{c.nome}: {c.detalhe}" for c in falhas))


def _checagem(nome: str, quantidade: int, descricao: str) -> Checagem:
    return Checagem(nome, quantidade == 0, f"{quantidade} {descricao}")


def checar_prata(df: DataFrame) -> list[Checagem]:
    fora_da_faixa = F.lit(False)
    for serie in SERIES.values():
        fora_da_faixa = fora_da_faixa | (
            (F.col("serie") == serie.nome) & ((F.col("valor") < serie.valor_min) | (F.col("valor") > serie.valor_max))
        )

    r = df.agg(
        F.count(F.lit(1)).alias("total"),
        F.sum(F.when(F.col("serie").isNull() | F.col("data").isNull() | F.col("valor").isNull(), 1).otherwise(0)).alias(
            "nulos"
        ),
        F.sum(F.when(~F.col("serie").isin(list(SERIES)), 1).otherwise(0)).alias("desconhecidas"),
        F.sum(F.when(fora_da_faixa, 1).otherwise(0)).alias("fora_da_faixa"),
        F.sum(F.when(F.col("data") > F.current_date(), 1).otherwise(0)).alias("futuras"),
    ).first()
    duplicadas = r["total"] - df.select("serie", "data").distinct().count()

    return [
        _checagem("sem_nulos", r["nulos"] or 0, "linhas com serie, data ou valor nulo"),
        _checagem("chave_unica", duplicadas, "linhas duplicadas para (serie, data)"),
        _checagem("series_conhecidas", r["desconhecidas"] or 0, "linhas de séries não configuradas"),
        _checagem("valores_na_faixa", r["fora_da_faixa"] or 0, "valores fora da faixa esperada"),
        _checagem("sem_datas_futuras", r["futuras"] or 0, "linhas com data no futuro"),
    ]


def checar_ouro(df: DataFrame) -> list[Checagem]:
    total = df.count()
    meses_distintos = df.select("mes").distinct().count()
    ipca_fora = df.filter((F.col("ipca_12m") < -10) | (F.col("ipca_12m") > 30)).count()
    return [
        Checagem("nao_vazia", total > 0, f"{total} meses"),
        _checagem("mes_unico", total - meses_distintos, "meses repetidos"),
        _checagem("ipca_12m_plausivel", ipca_fora, "meses com IPCA 12m fora de -10% a 30%"),
    ]


def garantir(checagens: list[Checagem], camada: str) -> None:
    """Registra o resultado das checagens e interrompe o pipeline se alguma falhar."""
    for c in checagens:
        if c.ok:
            logger.info("[qualidade:%s] OK    %s", camada, c.nome)
        else:
            logger.error("[qualidade:%s] FALHA %s (%s)", camada, c.nome, c.detalhe)
    falhas = [c for c in checagens if not c.ok]
    if falhas:
        raise QualidadeError(falhas)
