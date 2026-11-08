"""Extração das séries pela API pública SGS do Banco Central.

Documentação: https://dadosabertos.bcb.gov.br/ (API SGS)
Formato de resposta: [{"data": "02/01/2024", "valor": "4.8910"}, ...]
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable, Iterator
from datetime import date, timedelta

import requests

logger = logging.getLogger(__name__)

URL_SGS = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.{codigo}/dados"

# A API limita consultas de séries diárias a um intervalo de 10 anos.
DIAS_POR_CONSULTA = 3650

STATUS_PARA_NOVA_TENTATIVA = {429, 500, 502, 503, 504}


class ExtracaoError(RuntimeError):
    """Falha definitiva ao buscar dados na API."""


def janelas_de_consulta(inicio: date, fim: date, dias: int = DIAS_POR_CONSULTA) -> Iterator[tuple[date, date]]:
    """Divide [inicio, fim] em intervalos contíguos de no máximo `dias` dias."""
    if inicio > fim:
        raise ValueError(f"Data inicial {inicio} é posterior à data final {fim}")
    atual = inicio
    while atual <= fim:
        fim_janela = min(atual + timedelta(days=dias - 1), fim)
        yield atual, fim_janela
        atual = fim_janela + timedelta(days=1)


class ClienteBCB:
    """Cliente HTTP da API SGS com novas tentativas e espera exponencial."""

    def __init__(
        self,
        session: requests.Session | None = None,
        timeout: float = 30.0,
        tentativas: int = 4,
        espera_inicial: float = 2.0,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.session = session or requests.Session()
        self.session.headers.setdefault("User-Agent", "pipeline-indicadores-bcb/1.0")
        self.timeout = timeout
        self.tentativas = tentativas
        self.espera_inicial = espera_inicial
        self.sleep = sleep

    def buscar_serie(self, codigo: int, inicio: date, fim: date) -> list[dict]:
        """Retorna todos os registros da série entre `inicio` e `fim` (inclusive)."""
        registros: list[dict] = []
        for ini, fim_janela in janelas_de_consulta(inicio, fim):
            registros.extend(self._buscar_janela(codigo, ini, fim_janela))
        logger.info("Série %s: %d registros entre %s e %s", codigo, len(registros), inicio, fim)
        return registros

    def _buscar_janela(self, codigo: int, inicio: date, fim: date) -> list[dict]:
        url = URL_SGS.format(codigo=codigo)
        params = {
            "formato": "json",
            "dataInicial": inicio.strftime("%d/%m/%Y"),
            "dataFinal": fim.strftime("%d/%m/%Y"),
        }
        ultimo_erro: Exception | None = None

        for tentativa in range(1, self.tentativas + 1):
            try:
                resposta = self.session.get(url, params=params, timeout=self.timeout)
                if resposta.status_code == 404:
                    # A API responde 404 quando não há valores no intervalo.
                    return []
                if resposta.status_code == 200:
                    dados = resposta.json()
                    if not isinstance(dados, list):
                        raise ExtracaoError(f"Resposta inesperada da série {codigo}: {str(dados)[:200]}")
                    return dados
                if resposta.status_code not in STATUS_PARA_NOVA_TENTATIVA:
                    raise ExtracaoError(f"Série {codigo}: HTTP {resposta.status_code} sem nova tentativa")
                ultimo_erro = ExtracaoError(f"Série {codigo}: HTTP {resposta.status_code}")
            except (requests.RequestException, ValueError) as exc:
                # ValueError cobre corpo que não é JSON (ex.: página de erro em HTML).
                ultimo_erro = exc

            if tentativa < self.tentativas:
                espera = self.espera_inicial * 2 ** (tentativa - 1)
                logger.warning(
                    "Série %s, tentativa %d/%d falhou (%s). Nova tentativa em %.0fs",
                    codigo,
                    tentativa,
                    self.tentativas,
                    ultimo_erro,
                    espera,
                )
                self.sleep(espera)

        raise ExtracaoError(
            f"Série {codigo}: falhou após {self.tentativas} tentativas ({inicio} a {fim})"
        ) from ultimo_erro
