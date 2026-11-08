"""Configuração das séries do Banco Central e parâmetros do pipeline."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date


@dataclass(frozen=True)
class Serie:
    """Uma série temporal do SGS (Sistema Gerenciador de Séries Temporais) do BCB."""

    nome: str
    codigo_sgs: int
    descricao: str
    unidade: str
    periodicidade: str  # "diaria" ou "mensal"
    valor_min: float  # limites usados nas checagens de qualidade
    valor_max: float


SERIES: dict[str, Serie] = {
    s.nome: s
    for s in [
        Serie("dolar", 1, "Dólar comercial (venda)", "R$/US$", "diaria", 0.5, 20.0),
        Serie("selic_meta", 432, "Meta da taxa Selic definida pelo Copom", "% a.a.", "diaria", 0.0, 50.0),
        Serie("ipca", 433, "IPCA: variação mensal", "% no mês", "mensal", -5.0, 5.0),
    ]
}

# Primeira data buscada quando a tabela prata ainda não existe.
DATA_INICIO_PADRAO = date(2010, 1, 1)

# Na carga incremental, os últimos dias já carregados são buscados de novo.
# Isso captura correções publicadas pelo BCB; o MERGE na camada prata evita duplicatas.
JANELA_REPROCESSAMENTO_DIAS = 45
