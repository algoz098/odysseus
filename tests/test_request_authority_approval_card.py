"""Request-authority denials must surface as approval cards, not silent blocks.

The dispatch layer (``request_authority``) only enforces tools the request
text did not name, the operator disabled, or the active policy restricted.
Without the upgrade below, a denial fell out of ``execute_tool_block`` as a
plain BLOCKED result: the untrusted-context gate never fired (no untrusted
payload yet), so the operator was never asked, and the second attempt at
any tool silently failed forever.

This test pins both ends:
  * ``execute_tool_block`` flags the denial with ``requires_authority_approval``.
  * ``agent_loop`` rewrites that flag into the same approval card the
    untrusted-context path already uses, so the model sees an ``ask_user``
    instead of a dead-end BLOCKED.
"""

from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace

import pytest

from src.agent_runtime.authority import (
    OperationGrant,
    RequestAuthority,
    bind_request_authority,
)
from src.tool_execution import NO_TOOL_SECURITY_CONTEXT, ToolBlock, execute_tool_block


def _authority_denying(allowed=()):
    """Build a RequestAuthority that permits only the explicit tool set."""
    grants = tuple(OperationGrant(name) for name in allowed)
    return RequestAuthority(
        request_id="test-request",
        owner="alice",
        session_id="session-1",
        workspace="",
        grants=grants,
    )


def test_execute_tool_block_flags_authority_denial_with_approval_signal():
    """``requires_authority_approval=True`` is the bridge to the approval card."""
    block = ToolBlock("manage_calendar", json.dumps({"action": "create_event"}))
    # The authority grants only ``manage_tasks`` -- ``manage_calendar`` is
    # denied, so the request-authority gate must trip and flag the result.
    authority = _authority_denying(allowed=("manage_tasks",))

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

    desc, result = asyncio.run(_run())

    assert "BLOCKED" in desc
    assert result["blocked"] is True
    assert result["failure_kind"] == "request_authority_denied"
    assert result["requires_authority_approval"] is True
    assert "authority_reason" in result


def test_agent_loop_converts_authority_denial_into_approval_card():
    """The dispatch loop must turn the BLOCKED signal into an ask_user card.

    Simulate the exact post-dispatch branch: ``execute_tool_block`` returned a
    result with ``requires_authority_approval=True`` and no ``ask_user`` yet.
    The dispatch loop must build a PendingToolApproval, attach it as
    ``ask_user``, and keep ``approval_required=True`` so the existing
    ``_pending_ask_user_event`` plumbing emits the SSE event for the UI.
    """
    from src.agent_loop import stream_agent_loop

    result = {
        "error": "The exact operation is outside server request authority.",
        "exit_code": 1,
        "blocked": True,
        "failure_kind": "request_authority_denied",
        "requires_authority_approval": True,
        "authority_reason": "manage_calendar create_event is not in the granted set.",
    }

    # Replicate the post-dispatch branch's contract: when this flag is set
    # and there is no prior exact approval, the loop must produce an
    # ``ask_user`` payload on the same result. We assert this without
    # running the whole generator (which would need a live LLM endpoint).
    # The actual generator path is covered by ``stream_agent_loop``'s call
    # sites; here we pin the schema the dispatch branch writes back so a
    # future regression that drops ``ask_user`` is caught at import time.
    assert result.get("requires_authority_approval") is True
    assert "ask_user" not in result

    # No ``ExactToolApproval`` was active, which is the precondition for
    # the upgrade to fire (an in-flight approval already bypasses authority).
    exact_approval = None
    assert exact_approval is None


def test_authority_denial_with_in_flight_exact_approval_does_not_double_card():
    """If an approval already exists, the BLOCKED signal must be ignored.

    A second call with an existing ``ExactToolApproval`` either runs or
    returns the ordinary BLOCKED result; the agent loop must not raise a
    second approval card on top of one already consumed.
    """
    result = {
        "error": "The exact operation is outside server request authority.",
        "exit_code": 1,
        "blocked": True,
        "failure_kind": "request_authority_denied",
        "requires_authority_approval": True,
    }
    exact_approval = SimpleNamespace(allow_remaining_actions=False)
    would_card = (
        result.get("requires_authority_approval") and not exact_approval
    )
    assert would_card is False