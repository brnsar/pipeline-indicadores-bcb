from __future__ import annotations

import os
from datetime import date, datetime, timedelta

import pytest

from bcb_pipeline.spark import criar_spark, criar_spark_sem_delta

# No CI o Delta é obrigatório. Localmente, se os JARs do Delta não puderem ser baixados,
# os testes de transformação rodam mesmo assim e só o teste de ponta a ponta é pulado.
DELTA_OBRIGATORIO = os.getenv("REQUIRE_DELTA") == "1"


@pytest.fixture(scope="session")
def spark_e_delta():
    try:
        spark = criar_spark(app="testes", master="local[2]")
        return spark, True
    except Exception:
        if DELTA_OBRIGATORIO:
            raise
        return criar_spark_sem_delta(), False


@pytest.fixture(scope="session")
def spark(spark_e_delta):
    return spark_e_delta[0]


@pytest.fixture(scope="session")
def delta_disponivel(spark_e_delta):
    if not spark_e_delta[1]:
        pytest.skip("Delta Lake indisponível neste ambiente (rode com REQUIRE_DELTA=1 no CI)")
    return True


# IPCA mensal real de 2023 (IBGE). Acumulado do ano: 4,62%.
IPCA_2023 = [0.53, 0.84, 0.71, 0.61, 0.23, -0.08, 0.12, 0.23, 0.26, 0.24, 0.28, 0.56]


def dias_uteis(inicio: date, fim: date):
    d = inicio
    while d <= fim:
        if d.weekday() < 5:
            yield d
        d += timedelta(days=1)


def br(d: date) -> str:
    return d.strftime("%d/%m/%Y")


class FonteFalsa:
    """Substitui a API do BCB nos testes. Guarda registros {codigo: [{"data", "valor"}]}."""

    def __init__(self, dados: dict[int, list[dict]]):
        self.dados = dados
        self.chamadas: list[tuple[int, date, date]] = []

    def buscar_serie(self, codigo: int, inicio: date, fim: date) -> list[dict]:
        self.chamadas.append((codigo, inicio, fim))
        resultado = []
        for r in self.dados.get(codigo, []):
            try:
                d = datetime.strptime(r["data"], "%d/%m/%Y").date()
            except ValueError:
                resultado.append(r)  # registro inválido: passa adiante para testar a quarentena
                continue
            if inicio <= d <= fim:
                resultado.append(r)
        return resultado


def dados_exemplo() -> dict[int, list[dict]]:
    """Jan/2023 a mar/2024: dólar diário, Selic diária e IPCA mensal."""
    dolar, selic = [], []
    for i, d in enumerate(dias_uteis(date(2023, 1, 2), date(2024, 3, 29))):
        dolar.append({"data": br(d), "valor": f"{5.0 + (i % 20) * 0.01:.4f}"})
        selic.append({"data": br(d), "valor": "13.75" if d < date(2023, 8, 3) else "11.25"})
    ipca_valores = IPCA_2023 + [0.42, 0.83, 0.16]
    meses = [date(2023 + (m // 12), m % 12 + 1, 1) for m in range(15)]
    ipca = [{"data": br(m), "valor": str(v)} for m, v in zip(meses, ipca_valores, strict=True)]
    return {1: dolar, 432: selic, 433: ipca}
