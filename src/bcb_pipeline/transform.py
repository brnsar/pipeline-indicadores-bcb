"""Transformações das camadas bronze, prata e ouro.

As funções aqui recebem e devolvem DataFrames e não leem nem gravam nada,
o que permite testá-las sem Delta Lake. A gravação fica em `storage.py`.
"""

from __future__ import annotations

from datetime import datetime

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType, TimestampType

from .config import Serie

ESQUEMA_BRONZE = StructType(
    [
        StructField("serie", StringType(), False),
        StructField("codigo_sgs", IntegerType(), False),
        StructField("data", StringType(), True),
        StructField("valor", StringType(), True),
        StructField("_lote_id", StringType(), False),
        StructField("_ingerido_em", TimestampType(), False),
    ]
)


# ---------------------------------------------------------------- bronze ---


def montar_bronze(
    spark: SparkSession, serie: Serie, registros: list[dict], lote_id: str, ingerido_em: datetime
) -> DataFrame:
    """Dados exatamente como vieram da API (texto), mais metadados de ingestão."""
    linhas = [(serie.nome, serie.codigo_sgs, r.get("data"), r.get("valor"), lote_id, ingerido_em) for r in registros]
    return spark.createDataFrame(linhas, ESQUEMA_BRONZE)


# ----------------------------------------------------------------- prata ---


def bronze_para_prata(bronze: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Tipa, valida e remove duplicatas.

    Retorna (validos, rejeitados). Linhas com data ou valor que não podem ser
    convertidos vão para `rejeitados` (quarentena) em vez de quebrar o pipeline.
    """
    tipado = bronze.select(
        "serie",
        F.to_date(F.trim("data"), "dd/MM/yyyy").alias("data"),
        F.trim("valor").cast("decimal(18,6)").alias("valor"),
        F.col("data").alias("_data_original"),
        F.col("valor").alias("_valor_original"),
        "_lote_id",
        "_ingerido_em",
    )

    invalido = F.col("data").isNull() | F.col("valor").isNull()
    rejeitados = tipado.filter(invalido).select(
        "serie", "_data_original", "_valor_original", "_lote_id", "_ingerido_em"
    )

    # Se a mesma data aparecer mais de uma vez, fica a versão ingerida por último.
    ordem = Window.partitionBy("serie", "data").orderBy(F.col("_ingerido_em").desc())
    validos = (
        tipado.filter(~invalido)
        .withColumn("_ordem", F.row_number().over(ordem))
        .filter(F.col("_ordem") == 1)
        .select(
            "serie",
            "data",
            "valor",
            "_lote_id",
            F.col("_ingerido_em").alias("_atualizado_em"),
        )
    )
    return validos, rejeitados


# ------------------------------------------------------------------ ouro ---


def prata_para_ouro(prata: DataFrame) -> DataFrame:
    """Tabela mensal com os indicadores prontos para análise.

    Colunas:
      mes                    primeiro dia do mês
      dolar_medio            média do dólar (venda) no mês
      dolar_fechamento       último valor do dólar no mês
      dolar_var_mensal_pct   variação do fechamento em relação ao mês anterior
      selic_meta             meta Selic vigente no fim do mês (% a.a.)
      ipca_mensal            IPCA do mês (%)
      ipca_12m               IPCA acumulado em 12 meses (%), só com os 12 meses completos
      juro_real_12m          juro real ex-post: Selic descontada do IPCA de 12 meses (%)
    """
    base = prata.withColumn("mes", F.trunc("data", "month")).withColumn("valor", F.col("valor").cast("double"))

    ultimo_do_mes = Window.partitionBy("serie", "mes").orderBy(F.col("data").desc())
    fechamento = base.withColumn("_ordem", F.row_number().over(ultimo_do_mes)).filter(F.col("_ordem") == 1)

    dolar_medio = base.filter(F.col("serie") == "dolar").groupBy("mes").agg(F.avg("valor").alias("dolar_medio"))
    dolar_fechamento = fechamento.filter(F.col("serie") == "dolar").select(
        "mes", F.col("valor").alias("dolar_fechamento")
    )
    selic = fechamento.filter(F.col("serie") == "selic_meta").select("mes", F.col("valor").alias("selic_meta"))
    ipca = fechamento.filter(F.col("serie") == "ipca").select("mes", F.col("valor").alias("ipca_mensal"))

    mensal = (
        dolar_medio.join(dolar_fechamento, "mes", "full")
        .join(selic, "mes", "full")
        .join(ipca, "mes", "full")
        .withColumn("_indice_mes", F.year("mes") * 12 + F.month("mes"))
    )

    # Janela por intervalo de meses (e não por linhas), para um mês faltante não distorcer o cálculo.
    ultimos_12_meses = Window.orderBy("_indice_mes").rangeBetween(-11, 0)
    mes_anterior = Window.orderBy("_indice_mes")

    ipca_acumulado = (F.exp(F.sum(F.log1p(F.col("ipca_mensal") / 100)).over(ultimos_12_meses)) - 1) * 100
    meses_com_ipca = F.count("ipca_mensal").over(ultimos_12_meses)

    anterior = F.lag("dolar_fechamento").over(mes_anterior)
    indice_anterior = F.lag("_indice_mes").over(mes_anterior)

    resultado = (
        mensal.withColumn(
            "dolar_var_mensal_pct",
            F.when(
                F.col("_indice_mes") - indice_anterior == 1,
                (F.col("dolar_fechamento") / anterior - 1) * 100,
            ),
        )
        .withColumn("ipca_12m", F.when(meses_com_ipca == 12, ipca_acumulado))
        .withColumn(
            "juro_real_12m",
            ((1 + F.col("selic_meta") / 100) / (1 + F.col("ipca_12m") / 100) - 1) * 100,
        )
    )

    colunas = [
        "dolar_medio",
        "dolar_fechamento",
        "dolar_var_mensal_pct",
        "selic_meta",
        "ipca_mensal",
        "ipca_12m",
        "juro_real_12m",
    ]
    return resultado.select(
        "mes",
        *[F.round(c, 4).alias(c) for c in colunas],
        F.current_timestamp().alias("_atualizado_em"),
    ).orderBy("mes")
