"""Every authority denial in execute_tool_block must surface as an approval card.

The recurring 'tools work, then the next tool silently fails' bug surface is
rooted in `execute_tool_block` returning ``request_authority_denied`` without
the ``requires_authority_approval`` flag for some failure paths, while
returning it for others. The dispatch loop's ``requires_authority_approval``
branch is the only thing that turns a silent block into the same approval
card the untrusted-context gate already uses; missing it for any denial
path means the operator is never asked, and the second attempt at any tool
silently fails forever.

These tests pin every denial path so a future regression that drops the flag
on any branch is caught at import time.
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
from src.tool_execution import NO_TOOL_SECURITY_CONTEXT, ToolBlock, execute_tool_block


def _authority_for(*tools):
    """Build a RequestAuthority that admits only the named tools."""
    grants = tuple(OperationGrant(name) for name in tools)
    return RequestAuthority(
        request_id="test-request",
        owner="alice",
        session_id="session-1",
        workspace="",
        grants=grants,
    )


def _authority_denying():
    return _authority_for()


def _run_authority(block, authority):
    async def _run():
        with bind_request_authority(authority):
            desc, result = await execute_tool_block(
                block,
                session_id="session-1",
                owner="alice",
                workspace="",
                tool_policy=None,
                security_context=NO_TOOL_SECURITY_CONTEXT,
            )
        return desc, result

    return asyncio.run(_run())


def test_authority_denial_surfaces_as_approval_card():
    """The original c3eba5f5 invariant: a missing grant becomes an ask_user."""
    block = ToolBlock("manage_calendar", json.dumps({"action": "create_event"}))
    authority = _authority_for("manage_tasks")

    desc, result = _run_authority(block, authority)

    assert "BLOCKED" in desc
    assert result["blocked"] is True
    assert result["failure_kind"] == "request_authority_denied"
    assert result["requires_authority_approval"] is True
    assert "authority_reason" in result


def test_invalid_json_arguments_surface_as_approval_card():
    """A malformed JSON payload must not silently stop the second attempt.

    Before this fix, the ``(ValueError, TypeError)`` arm in
    ``execute_tool_block`` returned ``request_authority_denied`` *without*
    the approval flag, so a model whose first attempt hit a JSON encoding
    bug surfaced a generic BLOCKED, the operator was never asked, and every
    subsequent call to the same tool silently failed. The flag now bridges
    the same approval card path the untrusted-context gate already uses.
    """
    block = ToolBlock("manage_calendar", "{not valid json")
    authority = _authority_for("manage_calendar")

    desc, result = _run_authority(block, authority)

    assert "invalid arguments" in desc
    assert result["blocked"] is True
    assert result["exit_code"] == 1
    assert result["failure_kind"] == "request_authority_denied"
    assert result["requires_authority_approval"] is True, (
        "a malformed JSON payload must surface as an approval card, otherwise "
        "the model sees a silent block and never recovers"
    )
    assert "authority_reason" in result
    assert "JSON" in result["authority_reason"]


def test_turn_contract_denial_surfaces_as_approval_card():
    """Out-of-turn tools must reach the operator, not vanish silently.

    The agent-loop gate already intercepts this branch and converts it to an
    approval card before ``execute_tool_block`` is called, but every other
    caller (skills tester, scheduled task runner, audit tooling) runs the
    full contract path. Without the flag, those callers surface a silent
    BLOCKED and the operator never sees the proposed action.
    """
    from src.turn_contract import TurnContract, bind_turn_contract

    block = ToolBlock("manage_calendar", json.dumps({"action": "list_events"}))
    authority = _authority_for("manage_calendar")
    empty_contract = TurnContract(
        capabilities=frozenset(), required=frozenset(), offered=frozenset(),
        executable=frozenset(), unavailable=frozenset(), schema_json=(),
    )

    async def _run():
        with bind_request_authority(authority), bind_turn_contract(empty_contract):
            return await execute_tool_block(
                block,
                session_id="session-1",
                owner="alice",
                workspace="",
                tool_policy=None,
                security_context=NO_TOOL_SECURITY_CONTEXT,
            )

    desc, result = asyncio.run(_run())

    assert "BLOCKED" in desc
    assert result["exit_code"] == 1
    assert result["failure_kind"] == "turn_contract_denied"
    assert result["requires_authority_approval"] is True, (
        "an out-of-turn tool called from outside the agent loop must reach "
        "the operator; otherwise the second attempt at the same tool fails "
        "silently for skills tests and scheduled task runners"
    )
    assert "authority_reason" in result


def test_authority_denial_messages_distinguish_failure_kinds():
    """The error message must name the cause so the model can act on it.

    A model that sees ``"Tool is outside the requested turn capabilities."``
    needs to know whether to retry with a different payload, fix a syntax
    error, or stop. The reason field is the only signal that distinguishes
    those three failure kinds.
    """
    block = ToolBlock("manage_calendar", "{not valid json")
    authority = _authority_for("manage_calendar")

    _, result = _run_authority(block, authority)

    # The JSON failure kind is named explicitly so a model can recognise
    # "I produced bad arguments" and fix the payload rather than give up
    # on the whole integration.
    assert "JSON" in result["authority_reason"]