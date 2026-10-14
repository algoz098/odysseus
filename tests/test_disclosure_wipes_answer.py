"""The server's effect disclosure must not erase the model's answer.

The completion gate releases the model's events and then appends its own
disclosure for unverified external effects. The render state treats a delta
after a ``final_response`` as a new model synthesis and clears the
accumulator, so a turn that had an unverified external effect persisted only
the disclosure -- the whole answer was dropped at the SSE boundary, with no
error anywhere.
"""
import json

import pytest

from routes.chat_routes import _AgentRenderState


def _final(text):
    return json.dumps({"type": "final_response", "content": text})


def _disclosure(text, tagged=True):
    event = {"delta": text}
    if tagged:
        event["server_disclosure"] = True
    return json.dumps(event)


ANSWER = (
    "Aqui esta o resumo de pendencias e tarefas para hoje.\n\n"
    "## Email\n- Stripe: dispositivo nao reconhecido.\n"
    "## Tarefas\n- Responder a Jusbrasil ate sexta."
)
DISCLOSURE = (
    "External operation mcp__email__list_emails reported success; any "
    "external change it made was not independently verified."
)


def test_a_disclosure_after_a_final_response_keeps_the_answer():
    state = _AgentRenderState()
    state.consume(json.loads(_final(ANSWER)))
    state.consume(json.loads(_disclosure(DISCLOSURE)))

    assert ANSWER in state.content, state.content
    assert DISCLOSURE in state.content, state.content


def test_a_model_synthesis_still_replaces_the_final_response():
    """The reset exists for a *model* answer; it must survive that path."""
    state = _AgentRenderState()
    state.consume(json.loads(_final(ANSWER)))
    state.consume(json.loads(json.dumps({"delta": "Synthesis completely different."})))

    assert state.content == "Synthesis completely different."
    assert ANSWER not in state.content


def test_an_untagged_disclosure_is_the_bug_this_test_pins():
    """Documents the exact regression: untagged, the answer is destroyed."""
    state = _AgentRenderState()
    state.consume(json.loads(_final(ANSWER)))
    state.consume(json.loads(_disclosure(DISCLOSURE, tagged=False)))

    assert state.content == DISCLOSURE, (
        "the pre-fix behaviour: only the disclosure survives, so the turn "
        "persists 120 characters instead of the answer"
    )


def test_deltas_without_a_final_response_still_accumulate():
    state = _AgentRenderState()
    for piece in ("Parte 1. ", "Parte 2. ", "Parte 3."):
        state.consume({"delta": piece})

    assert state.content == "Parte 1. Parte 2. Parte 3."


def test_the_gate_tags_its_disclosure():
    """The tag is what makes the render state able to tell them apart."""
    import inspect

    from src.agent_runtime import completion

    source = inspect.getsource(completion.with_completion_gate)
    assert "'server_disclosure': True" in source or '"server_disclosure": True' in source
