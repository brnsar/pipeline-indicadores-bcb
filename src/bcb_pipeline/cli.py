"""Linha de comando: `bcb-pipeline --help`."""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import date

from .config import SERIES
from .extract import ExtracaoError
from .quality import QualidadeError


def _data(texto: str) -> date:
    try:
        return date.fromisoformat(texto)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"data inválida '{texto}', use AAAA-MM-DD") from exc


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="bcb-pipeline",
        description="Pipeline de indicadores do Banco Central (dólar, Selic e IPCA) em Delta Lake.",
    )
    parser.add_argument("--lake", default="lake", help="pasta do data lake (padrão: ./lake)")
    parser.add_argument("--inicio", type=_data, help="data inicial AAAA-MM-DD (padrão: carga incremental)")
    parser.add_argument("--fim", type=_data, help="data final AAAA-MM-DD (padrão: hoje)")
    parser.add_argument("--series", nargs="+", choices=sorted(SERIES), help="processa só estas séries (padrão: todas)")
    parser.add_argument("--mostrar", action="store_true", help="exibe os últimos 12 meses da camada ouro")
    parser.add_argument("-v", "--verbose", action="store_true", help="logs detalhados")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )
    logging.getLogger("py4j").setLevel(logging.WARNING)

    # Importações pesadas só depois de validar os argumentos.
    from .pipeline import Pipeline
    from .spark import criar_spark

    spark = criar_spark()
    try:
        series = [SERIES[s] for s in args.series] if args.series else None
        pipeline = Pipeline(spark, args.lake, series=series)
        resumo = pipeline.executar(inicio=args.inicio, fim=args.fim)
        print(resumo)
        if args.mostrar:
            (
                spark.read.format("delta")
                .load(pipeline.caminho_ouro)
                .orderBy("mes", ascending=False)
                .drop("_atualizado_em")
                .show(12, truncate=False)
            )
        return 0
    except (ExtracaoError, QualidadeError) as exc:
        logging.getLogger("bcb_pipeline").error("Pipeline interrompido: %s", exc)
        return 1
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
