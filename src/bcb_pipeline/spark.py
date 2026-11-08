"""Criação da SparkSession com suporte a Delta Lake."""

from __future__ import annotations

from pyspark.sql import SparkSession


def _configuracao_base(builder: SparkSession.Builder, app: str, master: str) -> SparkSession.Builder:
    return (
        builder.appName(app)
        .master(master)
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "America/Sao_Paulo")
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
        .config("spark.ui.showConsoleProgress", "false")
    )


def criar_spark(app: str = "pipeline-indicadores-bcb", master: str = "local[*]") -> SparkSession:
    """SparkSession local com Delta Lake (os JARs do Delta são baixados na primeira execução)."""
    from delta import configure_spark_with_delta_pip

    builder = (
        _configuracao_base(SparkSession.builder, app, master)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def criar_spark_sem_delta(app: str = "pipeline-indicadores-bcb-testes", master: str = "local[2]") -> SparkSession:
    """SparkSession sem Delta, usada só nos testes das transformações."""
    spark = _configuracao_base(SparkSession.builder, app, master).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
