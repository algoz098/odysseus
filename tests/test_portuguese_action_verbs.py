"""Portuguese imperative verbs must reach the product family they name.

Bug history: the action-verb lists in ``src/turn_contract.py`` and
``src/action_intents.py`` were English-only. A user who wrote
``Coloque na agenda amanha as 10:00 para ir no psiquiatra`` got an empty
``requested_capabilities`` set, so the request authority granted only
``ask_user`` and ``update_plan`` -- the model could not call
``manage_calendar``, the dispatch loop returned ``request_authority_denied``
on every attempt, and the operator was never shown the approval card.

These tests pin the fix:

* A Portuguese imperative leading the sentence carries the family scan
  forward exactly like its English equivalent.
* A Portuguese helper verb (``Quero``, ``Vou``, ``Preciso``,
  ``Você pode``) followed by an infinitive does the same.
* A bare auxiliary phrase (``Bote na agenda``, ``Lança no calendario``)
  also routes to the calendar family.

Adding Portuguese verb inflections must not break the English surface
form. The test suite for English imperative forms already covers that;
the addition of Portuguese verbs should also not promote ordinary prose
("Here is my entire essay ...") to action intent via the typo-distance
gate.
"""
from __future__ import annotations

import pytest

from src.action_intents import classify_tool_intent
from src.agent_runtime.authority import ExactOperation, create_request_authority
from src.turn_contract import (
    FAMILY_TOOLS,
    _ACTION,
    _ACTION_VERBS,
    _ACTION_REQUEST,
    requested_capabilities,
)


@pytest.mark.parametrize(("message", "expected_family"), [
    # The exact sentences from the production conversation that triggered
    # the bug. They are pinned here so the family is restored if the
    # routing code regresses.
    ("Coloque na agenda amanha as 10:00 para ir no psiquiatra", "calendar"),
    ("coloque as 16 de amanha um evento para fazer exame de sangue", "calendar"),
    # Variants a Portuguese-speaking operator types throughout the day.
    ("Adicione um evento amanha as 10", "calendar"),
    ("Adiciona um evento amanha as 10", "calendar"),
    ("Crie um evento amanha as 10", "calendar"),
    ("Agende uma reuniao amanha as 10", "calendar"),
    ("Marque amanha as 10 no calendario", "calendar"),
    ("Coloca na agenda amanha as 10", "calendar"),
    ("Lança no calendario amanha as 10", "calendar"),
    ("Insere no calendario amanha as 10", "calendar"),
    ("Registra no calendario amanha as 10", "calendar"),
    ("Ponha na agenda amanha as 10", "calendar"),
    ("Bote na agenda amanha as 10", "calendar"),
    # Notes / email / tasks.
    ("Manda um email para fulano", "email"),
    ("Apaga esse email", "email"),
    ("Cancela o evento de amanha", "calendar"),
    ("Mostra as tarefas de hoje", "tasks"),
    ("Liste os emails nao lidos", "email"),
    # Helper-verb + infinitive.
    ("Quero colocar na agenda amanha as 10", "calendar"),
    ("Vou colocar na agenda amanha as 10", "calendar"),
    ("Preciso colocar na agenda amanha as 10", "calendar"),
    ("Preciso agendar amanha as 10", "calendar"),
    ("Vou agendar amanha as 10", "calendar"),
    ("Quero agendar amanha as 10", "calendar"),
    ("Preciso criar um evento amanha as 10", "calendar"),
    ("Vou criar um evento amanha as 10", "calendar"),
    # Polite "can you" forms.
    ("Você pode colocar na agenda amanha as 10?", "calendar"),
    ("Pode colocar na agenda amanha as 10?", "calendar"),
    ("Você pode agendar amanha as 10?", "calendar"),
])
def test_portuguese_imperative_reaches_the_named_family(message, expected_family):
    assert expected_family in set(requested_capabilities(message))


@pytest.mark.parametrize("tool", ["manage_calendar", "manage_notes", "manage_tasks", "list_emails"])
def test_authority_grants_the_tool_for_a_portuguese_request(tool):
    """The request authority must admit the tool the family word names."""
    message_map = {
        "manage_calendar": "Coloque na agenda amanha as 10 para ir ao medico",
        "manage_notes": "Anote uma nota sobre a reuniao de amanha",
        "manage_tasks": "Crie uma tarefa para revisar o email amanha",
        "list_emails": "Liste os emails nao lidos",
    }
    authority = create_request_authority(
        message_map[tool], owner="alice", session_id="s", workspace="",
    )
    # ``manage_*`` tools take JSON; the normalization is the same path the
    # dispatch loop uses to admit the operation.
    operation = ExactOperation.normalize(tool, "{}")
    assert authority.permits(operation), tool


def test_english_imperative_still_works():
    """The English surface form must keep its calendar grant after the change."""
    assert "calendar" in set(requested_capabilities("put on the agenda tomorrow at 10:00"))
    assert "calendar" in set(requested_capabilities("add an event tomorrow at 4pm"))


def test_action_pattern_matches_the_english_and_portuguese_imperative():
    """The action slot must accept both languages at the start of a request."""
    assert _ACTION.match("Put on the agenda tomorrow at 10:00")
    assert _ACTION.match("Coloque na agenda amanha as 10:00")
    assert _ACTION.match("Adicione um evento amanha as 10")
    assert _ACTION.match("Quero colocar na agenda amanha as 10")
    assert _ACTION.match("Você pode colocar na agenda amanha as 10")


def test_classify_tool_intent_promotes_a_portuguese_calendar_request():
    intent = classify_tool_intent("Coloque na agenda amanha as 10:00 para ir ao medico")
    assert intent.needs_tools
    assert intent.category == "calendar"


def test_classify_tool_intent_promotes_a_portuguese_email_request():
    intent = classify_tool_intent("Mande um email para o fulano")
    assert intent.needs_tools
    assert intent.category == "email"


def test_classify_tool_intent_promotes_a_portuguese_notes_request():
    intent = classify_tool_intent("Anote uma nota sobre a reuniao de amanha")
    assert intent.needs_tools
    assert intent.category == "notes"


def test_english_prose_does_not_become_a_portuguese_action():
    """Portuguese verbs in the regex must not promote ordinary English prose.

    The action-verb regex now also lists the Portuguese inflected forms;
    the bug it could introduce is matching a substring of an English word
    or sentence that happens to contain a Portuguese verb in passing. The
    routing decision still has to anchor on a verb in the first slot, not
    on a substring inside a paragraph.
    """
    long_essay = "Here is my entire essay for review: " + "A paragraph. " * 600
    assert "calendar" not in set(requested_capabilities(long_essay, active_document=True))


def test_typo_distance_does_not_promote_here_to_gere():
    """The Portuguese imperative ``gere`` must not be one edit from ``here``.

    ``gere`` (Portuguese for "generate") sits one transposition away from
    ``here``; the typo-distance gate in ``_has_action_signal`` would
    otherwise promote ordinary English prose to action intent. Keep the
    English-only verb set narrow on purpose.
    """
    assert "gere" not in _ACTION_VERBS


def test_action_request_pattern_includes_portuguese_inflections():
    """Compile the action pattern and spot-check the Portuguese forms."""
    pattern = _ACTION_REQUEST
    for verb in (
        "coloque", "adicione", "crie", "agende", "marque", "remarque",
        "apague", "remova", "cancele", "liste", "mostre", "verifique",
        "procure", "busque", "pesquise", "encontre", "leia", "abra",
        "salve", "atualize", "mude", "edite", "reescreva", "encurte",
        "revise", "substitua", "acrescente", "conserte", "sugira",
        "envie", "mande", "responda", "lembre", "execute", "pare",
        "habilite", "desabilite", "troque", "investigue", "gere",
        "transcreva", "inspecione", "navegue", "reserve", "mova",
        "publique", "use", "programe", "anote", "registre", "insira",
        "lance", "bota",
    ):
        assert f"|{verb}|" in pattern or f"|{verb}\\" in pattern, verb
