"""Tool results must distinguish a permission denial from a backend failure.

The recurring 'tools work in turn 1, fail in turn 2' surface is rooted in
the model receiving the same ``exit_code=1`` for two completely different
failure modes and concluding that the integration is broken. The dispatch
loop never surfaces ``failure_kind`` to the model: every blocked run is
shaped the same way, so the next batch of calls gets refused as "the
server is denying write operations" when the real cause is a stale
connection, a missing capability, or a malformed payload.

``format_tool_result`` is the function the agent loop feeds back to the
LLM. Including the failure_kind verbatim (with a short human-readable
explanation for the kinds the model routinely misreads) gives the model
enough signal to choose between retrying, asking the operator, and giving
up on this attempt -- instead of treating every failure as the same
"permission denied".
"""
from __future__ import annotations

import pytest

from src.tool_execution import format_tool_result


def test_authority_denial_is_named_so_the_model_does_not_confuse_it_with_a_backend_error():
    """The model needs to know this is an authority gate, not a backend failure."""
    result = {
        "error": "The exact operation is outside server request authority.",
        "exit_code": 1,
        "blocked": True,
        "failure_kind": "request_authority_denied",
    }
    text = format_tool_result("mcp: mcp__whats2api__send_message", result)
    assert "request authority" in text
    assert "operator" in text or "authorised" in text, (
        "the model needs to know this needs an operator decision, not a "
        "backend retry"
    )


def test_backend_failure_keeps_the_actual_error_visible():
    """A MCP backend error must not get rewritten into a 'permission denied'."""
    result = {
        "error": "Connection ID 6ace60a1-87f7-4975-9ac0-3f0ff72ff6f9 not found.",
        "exit_code": 1,
    }
    text = format_tool_result("mcp: mcp__whats2api__send_message", result)
    assert "Connection ID" in text, (
        "the actual error must reach the model verbatim -- the agent loop "
        "strips stdout-derived errors otherwise"
    )


def test_turn_contract_denial_distinguishes_from_backend_error():
    result = {
        "error": "Tool is outside the requested turn capabilities.",
        "exit_code": 1,
        "failure_kind": "turn_contract_denied",
    }
    text = format_tool_result("manage_calendar: create_event", result)
    assert "turn contract" in text or "turn's declared capabilities" in text


def test_successful_tool_result_does_not_show_failure_kind():
    """A success must not introduce a fake failure_kind line."""
    result = {"output": "ok", "exit_code": 0}
    text = format_tool_result("bash: pwd", result)
    assert "failure_kind" not in text


def test_failure_kind_uses_human_readable_label_for_common_kinds():
    """The model gets a plain-English description of why the tool failed."""
    for kind, expected_phrase in [
        ("request_authority_denied", "request authority"),
        ("turn_contract_denied", "turn contract"),
        ("tool_policy_denied", "tool policy"),
        ("exact_target_missing", "exact target"),
        ("resource_identity_denied", "backend identity"),
    ]:
        result = {
            "error": f"some error for {kind}",
            "exit_code": 1,
            "failure_kind": kind,
        }
        text = format_tool_result("tool: anything", result)
        assert expected_phrase in text, (
            f"failure_kind={kind} should surface as a human-readable "
            f"phrase mentioning {expected_phrase!r}; got: {text!r}"
        )