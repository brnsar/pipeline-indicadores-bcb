from __future__ import annotations

from datetime import date, timedelta

import pytest
import requests

from bcb_pipeline.extract import ClienteBCB, ExtracaoError, janelas_de_consulta


class RespostaFalsa:
    def __init__(self, status: int, corpo=None):
        self.status_code = status
        self._corpo = corpo

    def json(self):
        if isinstance(self._corpo, Exception):
            raise self._corpo
        return self._corpo


class SessaoFalsa:
    def __init__(self, respostas):
        self.respostas = list(respostas)
        self.headers: dict[str, str] = {}
        self.chamadas: list[dict] = []

    def get(self, url, params=None, timeout=None):
        self.chamadas.append({"url": url, **(params or {})})
        resposta = self.respostas.pop(0)
        if isinstance(resposta, Exception):
            raise resposta
        return resposta


def cliente(respostas):
    esperas: list[float] = []
    c = ClienteBCB(session=SessaoFalsa(respostas), sleep=esperas.append)
    return c, esperas


def test_janelas_cobrem_o_periodo_sem_buracos_nem_sobreposicao():
    inicio, fim = date(2000, 1, 1), date(2025, 12, 31)
    janelas = list(janelas_de_consulta(inicio, fim))

    assert janelas[0][0] == inicio
    assert janelas[-1][1] == fim
    for (_, fim_a), (ini_b, _) in zip(janelas, janelas[1:], strict=False):
        assert ini_b == fim_a + timedelta(days=1)
    assert all((b - a).days < 3650 for a, b in janelas)


def test_janelas_rejeita_intervalo_invertido():
    with pytest.raises(ValueError):
        list(janelas_de_consulta(date(2024, 1, 2), date(2024, 1, 1)))


def test_busca_monta_parametros_no_formato_da_api():
    dados = [{"data": "02/01/2024", "valor": "4.8910"}]
    c, _ = cliente([RespostaFalsa(200, dados)])

    assert c.buscar_serie(1, date(2024, 1, 1), date(2024, 1, 31)) == dados
    chamada = c.session.chamadas[0]
    assert chamada["url"].endswith("bcdata.sgs.1/dados")
    assert chamada["dataInicial"] == "01/01/2024"
    assert chamada["dataFinal"] == "31/01/2024"
    assert chamada["formato"] == "json"


def test_periodo_longo_e_dividido_em_varias_consultas():
    c, _ = cliente([RespostaFalsa(200, [{"data": "a", "valor": "1"}])] * 3)
    registros = c.buscar_serie(1, date(2000, 1, 1), date(2025, 12, 31))
    assert len(c.session.chamadas) == 3
    assert len(registros) == 3


def test_404_significa_periodo_sem_dados():
    c, _ = cliente([RespostaFalsa(404)])
    assert c.buscar_serie(433, date(2024, 1, 1), date(2024, 1, 31)) == []


def test_erro_temporario_tenta_de_novo_com_espera_crescente():
    dados = [{"data": "02/01/2024", "valor": "4.89"}]
    c, esperas = cliente([RespostaFalsa(503), requests.ConnectionError("caiu"), RespostaFalsa(200, dados)])
    assert c.buscar_serie(1, date(2024, 1, 1), date(2024, 1, 31)) == dados
    assert esperas == [2.0, 4.0]


def test_resposta_que_nao_e_json_conta_como_erro_temporario():
    c, esperas = cliente([RespostaFalsa(200, ValueError("html")), RespostaFalsa(200, [])])
    assert c.buscar_serie(1, date(2024, 1, 1), date(2024, 1, 2)) == []
    assert len(esperas) == 1


def test_desiste_depois_do_limite_de_tentativas():
    c, esperas = cliente([RespostaFalsa(500)] * 4)
    with pytest.raises(ExtracaoError, match="4 tentativas"):
        c.buscar_serie(1, date(2024, 1, 1), date(2024, 1, 31))
    assert len(esperas) == 3


def test_erro_do_cliente_nao_tenta_de_novo():
    c, esperas = cliente([RespostaFalsa(400)])
    with pytest.raises(ExtracaoError, match="HTTP 400"):
        c.buscar_serie(1, date(2024, 1, 1), date(2024, 1, 31))
    assert esperas == []
