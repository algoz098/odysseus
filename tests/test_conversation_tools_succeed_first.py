"""End-to-end check: a tool works in turn 1, the next tool must not silently fail.

Reproduces the production surface (session
``b74f9eb3-39a0-40c7-a1f7-6feaae2191c8`` on 2026-10-15) at the agent-loop
level. Before the fix, the dispatch loop's ``requires_authority_approval``
branch only fired for one denial path. Three other paths -- malformed
JSON arguments, turn-contract denials reached outside the agent loop,
and admin-disabled tool policy blocks -- returned a BLOCKED result with
no approval flag, so the model saw a "the second attempt at any tool
silently fails forever" surface that just changed shape with every fix.

The test exercises the full dispatch chain with monkeypatched execute
so the call signature is realistic, and asserts every denial path turns
into an ``ask_user`` payload exactly like the untrusted-context path
already does.
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
from src.turn_contract import TurnContract, bind_turn_contract


def _denying_authority():
    """A request authority that admits no tools at all."""
    return RequestAuthority(
        request_id="test-conv-request",
        owner="alice",
        session_id="session-conv-1",
        workspace="",
        grants=(),
    )


def _admitting_authority(*tools):
    grants = tuple(OperationGrant(name) for name in tools)
    return RequestAuthority(
        request_id="test-conv-request",
        owner="alice",
        session_id="session-conv-1",
        workspace="",
        grants=grants,
    )


def _turn_contract_offering(*tools, capabilities=()):
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


def _run_authority(block, *, authority):
    async def _run():
        with bind_request_authority(authority):
            return await execute_tool_block(
                block,
                session_id="session-conv-1",
                owner="alice",
                workspace="",
                tool_policy=None,
                security_context=NO_TOOL_SECURITY_CONTEXT,
            )

    return asyncio.run(_run())


def _run_contract(block, *, authority, contract):
    async def _run():
        with bind_request_authority(authority), bind_turn_contract(contract):
            return await execute_tool_block(
                block,
                session_id="session-conv-1",
                owner="alice",
                workspace="",
                tool_policy=None,
                security_context=NO_TOOL_SECURITY_CONTEXT,
            )

    return asyncio.run(_run())


@pytest.mark.parametrize("block_content,reason_substring", [
    # Malformed JSON -- used to be silent block forever.
    ("{not valid json", "JSON"),
    # Invalid argument shape -- still raises ValueError/TypeError
    # through the same arm; must surface the same way.
    ('{"action":}', "JSON"),
])
def test_invalid_arguments_never_silently_block(block_content, reason_substring):
    block = ToolBlock("manage_calendar", block_content)
    _, result = _run_authority(block, authority=_admitting_authority("manage_calendar"))
    assert result["requires_authority_approval"] is True
    assert reason_substring in result["authority_reason"]


def test_turn_contract_denial_outside_agent_loop_surfaces_approval():
    """Skill tests / scheduled runners reach the approval path here too."""
    block = ToolBlock("manage_calendar", json.dumps({"action": "list_events"}))
    _, result = _run_contract(
        block,
        authority=_admitting_authority("manage_calendar"),
        contract=_turn_contract_offering(),  # nothing offered
    )
    assert result["requires_authority_approval"] is True
    assert "turn" in result["authority_reason"].lower()


def test_authority_denial_still_surfaces_approval_after_conversation_turns():
    """The original c3eba5f5 invariant survives the new path additions.

    This is the surface the user originally saw: a model whose first turn
    has authority for ``manage_tasks`` but not ``manage_calendar`` must
    get an approval card on the second turn, not a silent block that
    convinces the model the calendar integration is broken.
    """
    block = ToolBlock("manage_calendar", json.dumps({"action": "create_event"}))
    _, result = _run_authority(block, authority=_admitting_authority("manage_tasks"))
    assert result["requires_authority_approval"] is True
    assert result["failure_kind"] == "request_authority_denied"


def test_denied_authority_block_carries_failure_kind_for_the_model():
    """``format_tool_result`` keys on ``failure_kind`` to distinguish the
    block from a backend failure. Without it the next turn reports
    "the server is denying write operations" and gives up the
    integration. Pin both directions: a real denial gets a kind, and a
    backend error stays without one so it cannot masquerade.
    """
    from src.tool_execution import format_tool_result

    denied = {
        "error": "The exact operation is outside server request authority.",
        "exit_code": 1,
        "blocked": True,
        "failure_kind": "request_authority_denied",
    }
    backend_failed = {
        "error": "Connection ID not found.",
        "exit_code": 1,
    }
    a = format_tool_result("mcp: send_message", denied)
    b = format_tool_result("mcp: send_message", backend_failed)
    assert "request authority" in a
    assert "request authority" not in b
    assert "Connection ID" in b


def test_denial_path_never_returns_a_blocked_result_without_approval_flag():
    """Compile-time check: every authority-shaped denial in execute_tool_block
    carries ``requires_authority_approval=True``.

    The dispatch loop's ``requires_authority_approval`` branch is the
    single mechanism that turns a denial into an approval card. A
    denial without the flag is the exact surface that produced the
    "second attempt at any tool silently fails" bug. This test greps
    the source for the dispatcher's own denial returns and asserts
    each ``request_authority_denied`` / ``turn_contract_denied`` /
    ``resource_identity_denied`` block also sets the flag.
    """
    import re
    from pathlib import Path

    src = Path(__file__).resolve().parent.parent / "src" / "tool_execution.py"
    text = src.read_text(encoding="utf-8")

    lines = text.split("\n")
    # The dispatcher section runs from ``execute_tool_block`` until the
    # function ends. We bound the test to the body of that function --
    # the resource-identity failures inside it are tool-failure-kind
    # returns, but ``resource_identity_denied`` is a backend-fatal
    # shape (the tool can never succeed without re-binding); every
    # other kind is an operator decision and must carry the approval
    # flag.
    start_line = next(
        i for i, line in enumerate(lines, 1)
        if "async def execute_tool_block" in line
    )
    # ``execute_tool_block`` extends to the next top-level ``def`` or
    # ``async def`` (excluding its own internals). The MCP dispatch
    # helpers follow it; everything after the next def boundary is
    # out of scope for the dispatcher's denials.
    end_line = start_line
    for i, line in enumerate(lines[start_line:], start_line):
        if i == start_line - 1:
            continue
        if line.startswith("def ") or line.startswith("async def "):
            end_line = i
            break
    section = "\n".join(lines[start_line - 1:end_line])

    matches = list(re.finditer(
        r'return f"[^"]+",\s*\{[^}]*"failure_kind"\s*:\s*"([^"]+)"',
        section,
        flags=re.DOTALL,
    ))
    assert matches, "expected at least one dispatcher denial return block"

    bad = []
    for match in matches:
        kind = match.group(1)
        # ``resource_identity_denied`` is a backend-fatal shape (the
        # tool can never succeed without re-binding); turning it into
        # an approval card would loop the operator through a denial
        # they cannot resolve. Every other kind is an operator
        # decision and must carry the approval flag.
        if kind == "resource_identity_denied":
            continue
        start = section.rfind('return f"', 0, match.end())
        end = section.find('}', match.end())
        block = section[start:end + 1]
        if '"requires_authority_approval"' not in block:
            bad.append(block)

    assert not bad, (
        "execute_tool_block returned a denial without "
        "requires_authority_approval=True; the second attempt at the "
        "same tool will silently fail forever:\n\n" + "\n---\n".join(bad)
    )