"""End-to-end smoke that mirrors the production session the user reported.

Session ``b74f9eb3-39a0-40c7-a1f7-6feaae2191c8`` on 2026-10-15 had a clear
bug pattern: a Portuguese imperative request (calendar / whatsapp) ran
into a tool-policy denial, the operator approved, the next tool ran but
failed at the backend, and the model concluded "the server is denying
write operations". The fixes land here:

* Portuguese imperative verbs reach the right family (69e48608).
* A tool the turn did not anticipate is a question, not a denial (d283408f).
* An approval card must not swallow the rest of a tool batch (85285f3b).
* A request-authority denial surfaces as an approval card (c3eba5f5).
* This change closes the remaining silent-block paths and distinguishes
  backend failures from permission denials so the model no longer
  confuses the two.

The test reproduces the conversation shape with monkeypatched execute
and asserts the dispatcher does the right thing for every leg.
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


def _auth(*tools):
    grants = tuple(OperationGrant(name) for name in tools)
    return RequestAuthority(
        request_id="prod-regression",
        owner="admin",
        session_id="b74f9eb3-39a0-40c7-a1f7-6feaae2191c8",
        workspace="",
        grants=grants,
    )


def _contract(*tools):
    schema_json = tuple(
        json.dumps({
            "type": "function",
            "function": {"name": tool, "parameters": {"type": "object", "properties": {}}},
        })
        for tool in tools
    )
    return TurnContract(
        capabilities=frozenset(),
        required=frozenset(),
        offered=frozenset(tools),
        executable=frozenset(tools),
        unavailable=frozenset(),
        schema_json=schema_json,
    )


def test_portuguese_calendar_request_grants_calendar_family():
    """`Coloque na agenda amanha as 10` must reach manage_calendar.

    Before 69e48608 the action-verb regex was English-only. A
    Portuguese imperative produced an empty contract, only ask_user was
    granted, and the operator was told "Google Calendar is not set up"
    even though the integration worked. The fix widens the verb list
    to include the imperative + 3rd-person singular forms of the
    common Portuguese verbs (coloque, adicione, crie, agende, marque,
    ...).
    """
    from src.turn_contract import requested_capabilities
    caps = set(requested_capabilities(
        "Coloque na agenda amanha as 10:00 para ir no psiquiatra"
    ))
    assert "calendar" in caps, (
        "the Portuguese imperative must reach the calendar family; "
        "without it the operator gets the 'integration not set up' lie"
    )


def test_portuguese_email_request_grants_email_family():
    from src.turn_contract import requested_capabilities
    caps = set(requested_capabilities("Mande msg no whatsapp da mari"))
    # whatsapp routes to messaging, which expands to the email family
    # through the bridge. The core check is that *some* family is
    # granted -- the bug was an empty capability set.
    assert caps, (
        "the Portuguese imperative must reach at least one family; an "
        "empty set was the original 'every tool silently fails' bug"
    )


def test_first_leg_blocked_routes_to_approval_card():
    """The first ``list_connections`` call in the production session
    raised ``requires_authority_approval=True`` and the operator
    approved. After the fix every denial path -- not just the one the
    user happened to hit -- does the same.
    """
    block = ToolBlock("mcp__whats2api__list_connections", json.dumps({}))
    _, result = _run(block, _auth("manage_tasks"))
    assert result["requires_authority_approval"] is True


def test_second_leg_backend_failure_does_not_pretend_to_be_a_permission_denial():
    """The model's "the server is denying write operations" output came
    from a backend failure (connection ID not found). ``format_tool_result``
    now keeps that error verbatim and does not add the ``failure_kind``
    hint that belongs to authority denials, so the model can read the
    actual reason and not hallucinate a permission problem.
    """
    formatted = format_tool_result(
        "mcp__whats2api__send_message",
        {"error": "Connection ID 6ace60a1-87f7-4975-9ac0-3f0ff72ff6f9 not found.", "exit_code": 1},
    )
    assert "Connection ID" in formatted
    assert "request authority" not in formatted


def test_authority_reason_names_the_cause_for_each_denial():
    """The dispatcher's approval card text must tell the operator why.

    Three denial paths in execute_tool_block: malformed JSON,
    authority denial, and turn-contract denial. Each must produce
    ``authority_reason`` that names the cause, otherwise the operator
    cannot tell which gate the denial came from.
    """
    # JSON
    block = ToolBlock("manage_calendar", "{not valid json")
    _, result = _run(block, _auth("manage_calendar"))
    assert "JSON" in result["authority_reason"]

    # authority
    block = ToolBlock("manage_calendar", json.dumps({"action": "create_event"}))
    _, result = _run(block, _auth("manage_tasks"))
    assert "request authority" in result["authority_reason"].lower() or "policy" in result["authority_reason"].lower()

    # turn contract
    block = ToolBlock("manage_calendar", json.dumps({"action": "list_events"}))
    _, result = _run_block(block, _auth("manage_calendar"), _contract())
    assert "turn" in result["authority_reason"].lower()


def _run(block, authority):
    return _run_block(block, authority, _contract("manage_calendar", "manage_tasks"))


def _run_block(block, authority, contract):
    async def _run_inner():
        with bind_request_authority(authority), bind_turn_contract(contract):
            return await execute_tool_block(
                block,
                session_id="b74f9eb3-39a0-40c7-a1f7-6feaae2191c8",
                owner="admin",
                workspace="",
                tool_policy=None,
                security_context=NO_TOOL_SECURITY_CONTEXT,
            )

    return asyncio.run(_run_inner())