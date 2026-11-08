"""Orquestração: extração -> bronze -> prata -> ouro, com gravação em Delta Lake."""

from __future__ import annotations

import logging
import uuid
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Protocol

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .config import DATA_INICIO_PADRAO, JANELA_REPROCESSAMENTO_DIAS, SERIES, Serie
from .extract import ClienteBCB
from .quality import checar_ouro, checar_prata, garantir
from .transform import bronze_para_prata, montar_bronze, prata_para_ouro

logger = logging.getLogger(__name__)


class FonteDeDados(Protocol):
    def buscar_serie(self, codigo: int, inicio: date, fim: date) -> list[dict]: ...


@dataclass
class ResumoExecucao:
    lote_id: str
    extraidos: dict[str, int] = field(default_factory=dict)
    rejeitados: int = 0
    linhas_prata: int = 0
    meses_ouro: int = 0

    def __str__(self) -> str:
        extraidos = ", ".join(f"{k}={v}" for k, v in self.extraidos.items())
        return (
            f"lote {self.lote_id} | extraídos: {extraidos} | rejeitados: {self.rejeitados} | "
            f"prata: {self.linhas_prata} linhas | ouro: {self.meses_ouro} meses"
        )


class Pipeline:
    def __init__(
        self,
        spark: SparkSession,
        lake: str | Path,
        fonte: FonteDeDados | None = None,
        series: Iterable[Serie] | None = None,
    ) -> None:
        self.spark = spark
        self.fonte = fonte or ClienteBCB()
        self.series = list(series or SERIES.values())
        lake = Path(lake).resolve()
        self.caminho_bronze = str(lake / "bronze" / "sgs")
        self.caminho_quarentena = str(lake / "bronze" / "quarentena")
        self.caminho_prata = str(lake / "prata" / "indicadores")
        self.caminho_ouro = str(lake / "ouro" / "indicadores_mensais")

    # ------------------------------------------------------------- helpers ---

    def _existe(self, caminho: str) -> bool:
        return DeltaTable.isDeltaTable(self.spark, caminho)

    def _inicio_incremental(self, serie: Serie) -> date:
        """Data inicial da próxima extração, com base no que já está na camada prata."""
        if not self._existe(self.caminho_prata):
            return DATA_INICIO_PADRAO
        ultima = (
            self.spark.read.format("delta")
            .load(self.caminho_prata)
            .filter(F.col("serie") == serie.nome)
            .agg(F.max("data"))
            .first()[0]
        )
        if ultima is None:
            return DATA_INICIO_PADRAO
        return max(DATA_INICIO_PADRAO, ultima - timedelta(days=JANELA_REPROCESSAMENTO_DIAS))

    # -------------------------------------------------------------- etapas ---

    def _gravar_bronze(self, df: DataFrame) -> None:
        df.write.format("delta").mode("append").partitionBy("serie").save(self.caminho_bronze)

    def _gravar_quarentena(self, df: DataFrame) -> None:
        df.write.format("delta").mode("append").save(self.caminho_quarentena)

    def _upsert_prata(self, df: DataFrame) -> None:
        if not self._existe(self.caminho_prata):
            df.write.format("delta").partitionBy("serie").save(self.caminho_prata)
            return
        (
            DeltaTable.forPath(self.spark, self.caminho_prata)
            .alias("destino")
            .merge(df.alias("novo"), "destino.serie = novo.serie AND destino.data = novo.data")
            .whenMatchedUpdate(
                condition="destino.valor <> novo.valor",
                set={
                    "valor": "novo.valor",
                    "_lote_id": "novo._lote_id",
                    "_atualizado_em": "novo._atualizado_em",
                },
            )
            .whenNotMatchedInsertAll()
            .execute()
        )

    def _gravar_ouro(self, df: DataFrame) -> None:
        # A tabela ouro é pequena (uma linha por mês): é recalculada inteira a cada execução.
        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(self.caminho_ouro)

    # ------------------------------------------------------------ execução ---

    def executar(self, inicio: date | None = None, fim: date | None = None) -> ResumoExecucao:
        lote_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S") + "-" + uuid.uuid4().hex[:6]
        ingerido_em = datetime.now(timezone.utc)
        fim = fim or date.today()
        resumo = ResumoExecucao(lote_id=lote_id)
        logger.info("Iniciando lote %s", lote_id)

        # 1. Extração + bronze
        for serie in self.series:
            ini = inicio or self._inicio_incremental(serie)
            if ini > fim:
                resumo.extraidos[serie.nome] = 0
                continue
            registros = self.fonte.buscar_serie(serie.codigo_sgs, ini, fim)
            resumo.extraidos[serie.nome] = len(registros)
            if registros:
                self._gravar_bronze(montar_bronze(self.spark, serie, registros, lote_id, ingerido_em))

        if sum(resumo.extraidos.values()) == 0:
            logger.info("Nenhum dado novo na API. Nada a processar.")
            return resumo

        # 2. Prata: só o que entrou neste lote
        lote = self.spark.read.format("delta").load(self.caminho_bronze).filter(F.col("_lote_id") == lote_id)
        validos, rejeitados = bronze_para_prata(lote)
        validos = validos.cache()

        resumo.rejeitados = rejeitados.count()
        if resumo.rejeitados:
            logger.warning("%d registros inválidos enviados para a quarentena", resumo.rejeitados)
            self._gravar_quarentena(rejeitados)

        garantir(checar_prata(validos), "prata")
        self._upsert_prata(validos)
        validos.unpersist()

        # 3. Ouro: recalculado a partir da prata completa
        prata = self.spark.read.format("delta").load(self.caminho_prata)
        resumo.linhas_prata = prata.count()
        ouro = prata_para_ouro(prata).cache()
        garantir(checar_ouro(ouro), "ouro")
        self._gravar_ouro(ouro)
        resumo.meses_ouro = ouro.count()
        ouro.unpersist()

        logger.info("Concluído: %s", resumo)
        return resumo
