"""A tool the turn did not anticipate is a question for the operator, not a denial.

The turn contract is a guess about what a request is for, derived from its
text. When the guess was wrong the tool was refused outright, before the
approval gate was ever reached, and the operator was never asked. The agent
read that refusal as "that integration is not configured" and went looking for
a credential problem that did not exist -- which is the whole chain behind
"it says the integration is not set up".

A denial the operator configured stays a denial. Only the guess moves.
"""
import asyncio
import json

import pytest

import src.agent_loop as al
from src.tool_capabilities import ToolGateDecision, ToolRunSecurityContext
from src.tool_parsing import ToolBlock
from tests.runtime_evidence_helpers import authoritative_executor


def _contract(allows):
    """A contract whose inventory does NOT include the email read."""
    from src.turn_contract import TurnContract

    return TurnContract(
        capabilities=frozenset(), required=frozenset(), offered=frozenset(),
        executable=frozenset(), unavailable=frozenset(), schema_json=(),
    )


def _events(chunks):
    out = []
    for chunk in chunks:
        if not chunk.startswith("data: ") or chunk.startswith("data: [DONE]"):
            continue
        try:
            out.append(json.loads(chunk[6:]))
        except ValueError:
            pass
    return out


def _run(monkeypatch, *, contract, tool, content, executed):
    monkeypatch.setattr(al, "get_setting", lambda key, default=None: default, raising=False)
    monkeypatch.setattr(al, "get_mcp_manager", lambda: None, raising=False)
    monkeypatch.setattr(al, "blocked_tools_for_owner", lambda owner: set(), raising=False)
    monkeypatch.setattr(al, "estimate_tokens", lambda *a, **k: 10, raising=False)
    monkeypatch.setattr(al, "tool_result_should_arm_gate", lambda *a, **k: False, raising=False)
    monkeypatch.setattr(al, "blocked_references_evidence", {}, raising=False)
    monkeypatch.setattr(al.ToolRunSecurityContext, "decision_for",
                        lambda self, t, c, *a, **k: ToolGateDecision(True))

    async def execute(block, *args, **kwargs):
        executed.append(block.tool_type)
        return block.tool_type, {"output": "ok", "exit_code": 0}

    monkeypatch.setattr(al, "execute_tool_block", authoritative_executor(execute))

    call = {"n": 0}

    async def stream(_candidates, messages, **kwargs):
        call["n"] += 1
        if call["n"] == 1:
            yield 'data: ' + json.dumps({"type": "tool_calls", "calls": [
                {"name": tool, "arguments": content}]}) + "\n"
        else:
            yield 'data: ' + json.dumps({"delta": "pronto"}) + "\n"
        yield "data: [DONE]\n\n"

    monkeypatch.setattr(al, "stream_llm_with_fallback", stream)

    async def collect():
        return _events([c async for c in al.stream_agent_loop(
            "http://unused.test/v1", "test-model",
            [{"role": "user", "content": "me mostra os emails"}],
            max_rounds=3, owner="alice", session_id="s", turn_contract=contract,
        )])

    return asyncio.run(collect())


def test_a_tool_outside_the_turn_asks_instead_of_refusing(monkeypatch):
    """The contract missed it, so the operator decides -- no silent refusal."""
    executed = []
    events = _run(
        monkeypatch, contract=_contract(None),
        tool="mcp__email__list_emails", content="{}", executed=executed,
    )

    ask_user = [e for e in events if e.get("type") == "ask_user"]
    assert ask_user, (
        "a tool the turn did not anticipate must produce an approval card; "
        "refusing it silently is what made the agent report a broken integration"
    )
    payload = json.dumps(events)
    assert "outside the requested turn capabilities" not in payload
    assert "disabled by the current request policy" not in payload


def test_a_policy_the_operator_configured_is_still_a_denial(monkeypatch):
    """Only the guess moves. A real denial must not become a question."""
    from src.tool_policy import ToolPolicy

    executed = []
    policy = ToolPolicy(disabled_tools=frozenset({"mcp__email__list_emails"}))
    monkeypatch.setattr(al, "get_setting", lambda key, default=None: default, raising=False)
    monkeypatch.setattr(al, "get_mcp_manager", lambda: None, raising=False)
    monkeypatch.setattr(al, "blocked_tools_for_owner", lambda owner: set(), raising=False)
    monkeypatch.setattr(al, "estimate_tokens", lambda *a, **k: 10, raising=False)
    monkeypatch.setattr(al, "tool_result_should_arm_gate", lambda *a, **k: False, raising=False)
    monkeypatch.setattr(al.ToolRunSecurityContext, "decision_for",
                        lambda self, t, c, *a, **k: ToolGateDecision(True))

    async def execute(block, *args, **kwargs):
        executed.append(block.tool_type)
        return block.tool_type, {"output": "ok", "exit_code": 0}

    monkeypatch.setattr(al, "execute_tool_block", authoritative_executor(execute))

    async def stream(_candidates, messages, **kwargs):
        yield 'data: ' + json.dumps({"type": "tool_calls", "calls": [
            {"name": "mcp__email__list_emails", "arguments": "{}"}]}) + "\n"
        yield "data: [DONE]\n\n"

    monkeypatch.setattr(al, "stream_llm_with_fallback", stream)

    events = _events(asyncio.run(_collect(policy)))

    assert not executed, "an operator-configured denial must not execute"
    assert not [e for e in events if e.get("type") == "ask_user"], (
        "a configured denial must not be turned into a question"
    )


async def _collect(policy):
    return [c async for c in al.stream_agent_loop(
        "http://unused.test/v1", "test-model",
        [{"role": "user", "content": "me mostra os emails"}],
        max_rounds=3, owner="alice", session_id="s",
        tool_policy=policy, turn_contract=_contract(None),
    )]
