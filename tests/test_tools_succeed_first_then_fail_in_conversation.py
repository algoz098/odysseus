"""Regression test for the 'tool works in turn 1, fails in turn 2' surface.

The user reports: when a tool call succeeds in one conversational turn, a
*similar* tool call in the next turn fails in a different way each time a
fix is applied. The whack-a-mole behaviour is rooted in three independent
gaps, each of which surfaced as a separate bug fix:

1. ``execute_tool_block`` used to return ``request_authority_denied``
   without the ``requires_authority_approval`` flag, so a denied tool
   never surfaced an approval card -- the model silently failed the
   second attempt forever.
2. ``format_tool_result`` did not distinguish a permission denial from a
   backend failure: the model saw the same ``exit_code=1`` for both and
   concluded the integration was broken.
3. The turn-contract denial path inside ``execute_tool_block`` was missing
   the ``requires_authority_approval`` flag too -- callers outside the
   agent loop (skills tester, scheduled task runner) silently lost
   authority for an out-of-turn tool.

This test pins the full chain end-to-end so any of those gaps regressing
catches the recurring surface.
"""
from __future__ import annotations

import asyncio
import json

import pytest

from src.agent_runtime.authority import (
    OperationGrant,
    RequestAuthority,
    bind_request_authority,
)
from src.tool_execution import (
    NO_TOOL_SECURITY_CONTEXT,
    ToolBlock,
    execute_tool_block,
    format_tool_result,
)
from src.turn_contract import TurnContract, bind_turn_contract


def _authority_for(*tools):
    grants = tuple(OperationGrant(name) for name in tools)
    return RequestAuthority(
        request_id="test-request",
        owner="alice",
        session_id="session-1",
        workspace="",
        grants=grants,
    )


def _turn_contract_with(*tools, capabilities=()):
    """Build a turn contract that admits only the named tools/capabilities."""
    schema_json = tuple(
        json.dumps({
            "type": "function",
            "function": {"name": tool, "parameters": {"type": "object", "properties": {}}},
        })
        for tool in tools
    )
    return TurnContract(
        capabilities=frozenset(capabilities),
        required=frozenset(),
        offered=frozenset(tools),
        executable=frozenset(tools),
        unavailable=frozenset(),
        schema_json=schema_json,
    )


def _run(block, *, authority, contract):
    async def _run():
        with bind_request_authority(authority), bind_turn_contract(contract):
            return await execute_tool_block(
                block,
                session_id="session-1",
                owner="alice",
                workspace="",
                tool_policy=None,
                security_context=NO_TOOL_SECURITY_CONTEXT,
            )

    return asyncio.run(_run())


def test_authority_denied_then_backend_failed_are_not_interchangeable():
    """A permission denial and a backend failure must produce different
    model-facing strings. The recurring surface was the model misreading
    ``exit_code=1`` as "permission denied" and giving up on the
    integration. Distinguishing them in ``format_tool_result`` keeps the
    model from doing that.
    """
    permission_denied = {
        "error": "The exact operation is outside server request authority.",
        "exit_code": 1,
        "blocked": True,
        "failure_kind": "request_authority_denied",
    }
    backend_failed = {
        "error": "Connection ID 6ace60a1-87f7-4975-9ac0-3f0ff72ff6f9 not found.",
        "exit_code": 1,
    }
    a = format_tool_result("mcp__whats2api__send_message", permission_denied)
    b = format_tool_result("mcp__whats2api__send_message", backend_failed)

    assert "request authority" in a
    assert "request authority" not in b
    assert "Connection ID" in b
    # ``failure_kind`` is the marker the dispatch loop keys on to surface an
    # approval card. It must only appear for permission-shaped failures,
    # not for backend errors -- otherwise the agent loop turns a real
    # backend failure into a question for the operator.
    assert "failure_kind" in a
    assert "failure_kind" not in b


def test_denied_tool_surfaces_approval_card_on_every_path():
    """Every denial path must reach the operator, not vanish silently.

    Before this fix, three paths returned ``request_authority_denied``
    without the ``requires_authority_approval`` flag -- the dispatch
    loop's only mechanism for turning a denial into an approval card.
    That is the exact surface that caused the second attempt to fail
    silently forever. The paths:
      * explicit authority denial (missing grant)            -- was OK
      * JSON parsing failure                                  -- was NOT
      * turn contract denial (in execute_tool_block, callers
        outside the agent loop)                              -- was NOT
    """
    # 1. explicit authority denial -- existing path kept covered
    block = ToolBlock("manage_calendar", json.dumps({"action": "create_event"}))
    _, result = _run(
        block,
        authority=_authority_for("manage_tasks"),
        contract=_turn_contract_with("manage_calendar"),
    )
    assert result["requires_authority_approval"] is True

    # 2. JSON parsing failure -- new path
    block = ToolBlock("manage_calendar", "{not valid json")
    _, result = _run(
        block,
        authority=_authority_for("manage_calendar"),
        contract=_turn_contract_with("manage_calendar"),
    )
    assert result["requires_authority_approval"] is True
    assert "JSON" in result["authority_reason"]

    # 3. turn contract denial inside execute_tool_block -- new path
    block = ToolBlock("manage_calendar", json.dumps({"action": "list_events"}))
    _, result = _run(
        block,
        authority=_authority_for("manage_calendar"),
        # contract has no capabilities -- no tools offered
        contract=_turn_contract_with(),
    )
    assert result["requires_authority_approval"] is True
    assert "turn" in result["authority_reason"].lower()


def test_tool_policy_block_is_named_as_such_not_as_a_backend_error():
    """The model's prompt must see that an admin blocked the tool.

    A path through the agent loop uses ``"policy": "current_tool_policy"``
    without a ``failure_kind``. The model then conflates it with a backend
    failure and goes looking for an integration problem that doesn't
    exist. The fix adds the kind so the model can recognise this is a
    configured denial.
    """
    blocked = {
        "error": "Tool 'manage_calendar' is disabled by the current request policy.",
        "exit_code": 1,
        "blocked": True,
        "policy": "current_tool_policy",
        "failure_kind": "tool_policy_denied",
    }
    text = format_tool_result("manage_calendar: create_event", blocked)
    assert "tool policy" in text
    assert "Do not retry" in text or "admin" in text or "operator" in text, (
        "the model must be told this is a configured denial, not a transient "
        "backend failure; otherwise it retries the same call indefinitely"
    )