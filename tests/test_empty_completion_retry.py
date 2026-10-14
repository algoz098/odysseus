"""An empty completion is a provider blip, not a verdict.

A 200 response carrying no text and no tool call used to end the turn with
"All model candidates returned no substantive output" (502). The only recovery
was a *different* candidate, so a single-endpoint deployment had none and the
whole request failed on a blip -- with the tool result already in hand and
nothing to show for it.

Retrying the same candidate is safe precisely because the completion was
empty: no token reached the client, so re-issuing cannot duplicate output.
"""
import asyncio
import json

import pytest

from src import llm_core

DONE = "data: [DONE]\n\n"


def _stream(*events):
    """The app's own normalized SSE, which is what the fallback loop reads."""
    return "".join(f"data: {json.dumps(e)}\n\n" for e in events) + DONE


def _text(value):
    return {"delta": value}


def _tool_call(name="mcp__email__list_emails", args="{}"):
    return {"type": "tool_calls", "calls": [
        {"id": "call_1_0", "type": "function",
         "function": {"name": name, "arguments": args}}]}


def _error(status=502, message="All model candidates returned no substantive output"):
    return f'event: error\ndata: {json.dumps({"error": message, "status": status})}\n\n'


async def _run(monkeypatch, bodies, candidates=None, retry_empty=True):
    """Replay each body as the candidate's stream, in call order."""
    calls = []

    async def fake_candidate(*args, **kwargs):
        body = bodies[min(len(calls), len(bodies) - 1)]
        calls.append(body)
        for chunk in body.splitlines(keepends=True):
            yield chunk

    monkeypatch.setattr(llm_core, "_stream_candidate_with_context_recovery", fake_candidate)
    cands = candidates or [("https://api.example/v1", "m", {"Authorization": "k"})]
    out = []
    async for chunk in llm_core.stream_llm_with_fallback(
            cands, [{"role": "user", "content": "oi"}],
            retry_empty_completion=retry_empty):
        out.append(chunk)
    return out, calls


@pytest.mark.asyncio
async def test_an_empty_completion_is_retried_on_the_same_candidate(monkeypatch):
    out, calls = await _run(monkeypatch, [_stream(), _stream(_text("pronto"))])

    assert len(calls) == 2, "an empty completion must be retried, not failed"
    assert any('"delta"' in c for c in out), out
    assert not any("no substantive output" in c for c in out), out


@pytest.mark.asyncio
async def test_a_persistently_empty_completion_still_fails(monkeypatch):
    out, calls = await _run(monkeypatch, [_stream()])

    assert len(calls) == llm_core._EMPTY_COMPLETION_RETRIES + 1
    assert any("no substantive output" in c for c in out), out


@pytest.mark.asyncio
async def test_the_retry_is_bounded(monkeypatch):
    _, calls = await _run(monkeypatch, [_stream()])

    assert len(calls) <= llm_core._EMPTY_COMPLETION_RETRIES + 1


@pytest.mark.asyncio
async def test_a_successful_first_completion_is_not_retried(monkeypatch):
    out, calls = await _run(monkeypatch, [_stream(_text("oi"))])

    assert len(calls) == 1, "a completion with output must not be re-issued"
    assert not any("no substantive output" in c for c in out), out


@pytest.mark.asyncio
async def test_a_tool_call_completion_is_substantive(monkeypatch):
    out, calls = await _run(monkeypatch, [_stream(_tool_call())])

    assert len(calls) == 1, "a tool call is output; it must not be re-issued"
    assert any('"tool_calls"' in c for c in out), out


@pytest.mark.asyncio
async def test_metadata_alone_does_not_commit_a_candidate(monkeypatch):
    """A usage-only frame is not an answer and must not stop the retry."""
    usage_only = {"type": "usage", "data": {"input_tokens": 10, "output_tokens": 0}}

    out, calls = await _run(monkeypatch, [_stream(usage_only), _stream(_text("oi"))])

    assert len(calls) == 2, "usage metadata is not substantive output"
    assert any('"delta"' in c for c in out), out


@pytest.mark.asyncio
async def test_without_the_opt_in_an_empty_completion_is_terminal(monkeypatch):
    """The chain's contract is unchanged unless a caller opts in."""
    out, calls = await _run(monkeypatch, [_stream()], retry_empty=False)

    assert len(calls) == 1
    assert any("no substantive output" in c for c in out), out


@pytest.mark.asyncio
async def test_the_empty_retry_still_falls_through_to_the_next_candidate(monkeypatch):
    cands = [
        ("https://primary.example/v1", "primary", {}),
        ("https://backup.example/v1", "backup", {}),
    ]
    out, calls = await _run(
        monkeypatch, [_stream(), _stream(), _stream(), _stream(_text("do backup"))],
        candidates=cands, retry_empty=True,
    )

    assert any("do backup" in c for c in out), out
    assert not any("All model candidates" in c for c in out), out


def test_the_agent_loop_opts_into_the_empty_retry():
    """The chat turn must survive a blip when there is no second endpoint."""
    import inspect

    import src.agent_loop as agent_loop

    source = inspect.getsource(agent_loop)
    assert "retry_empty_completion=True" in source, (
        "stream_agent_loop must request the empty-completion retry; without it "
        "a single-endpoint deployment has no recovery from a 200-with-no-output"
    )


def test_the_retry_is_opt_in_not_default():
    """Other callers keep the terminal-on-empty contract unless they opt in."""
    import inspect

    from src import llm_core

    # ``stream_llm_with_fallback`` pops the flag out of **kwargs, so the
    # opt-in is visible in the source default rather than a named parameter.
    source = inspect.getsource(llm_core.stream_llm_with_fallback)
    assert 'kwargs.pop("retry_empty_completion", False)' in source, (
        "the empty-completion retry must default to off so callers that did "
        "not ask for it keep the terminal-on-empty contract"
    )


# --- The regression this file exists for ------------------------------------
#
# The first version of this retry was gated on ``fallback_on_empty`` as well
# as its own opt-in. The foreground chat route sets that flag to False on
# purpose (see ``ForegroundModelPolicy.fallback_on_empty``), so the retry was
# dead in precisely the path that needed it: a chat turn against a single
# endpoint still lost the whole request to an empty completion.

@pytest.mark.asyncio
async def test_retry_still_runs_when_fallback_on_empty_is_false(monkeypatch):
    """The chat path: fallback_on_empty=False + retry opt-in must retry."""
    calls = []

    async def fake_candidate(*args, **kwargs):
        body = _stream() if len(calls) < 1 else _stream(_text("pronto"))
        calls.append(body)
        for chunk in body.splitlines(keepends=True):
            yield chunk

    monkeypatch.setattr(llm_core, "_stream_candidate_with_context_recovery", fake_candidate)

    out = []
    async for chunk in llm_core.stream_llm_with_fallback(
            [("https://api.example/v1", "m", {"Authorization": "k"})],
            [{"role": "user", "content": "oi"}],
            fallback_on_empty=False,
            retry_empty_completion=True,
    ):
        out.append(chunk)

    assert len(calls) == 2, "the retry must not be gated on fallback_on_empty"
    assert any('"delta"' in c for c in out), out


@pytest.mark.asyncio
async def test_fallback_on_empty_false_alone_still_means_no_retry(monkeypatch):
    """Without the opt-in the foreground policy is untouched."""
    calls = []

    async def fake_candidate(*args, **kwargs):
        body = _stream()
        calls.append(body)
        for chunk in body.splitlines(keepends=True):
            yield chunk

    monkeypatch.setattr(llm_core, "_stream_candidate_with_context_recovery", fake_candidate)

    out = []
    async for chunk in llm_core.stream_llm_with_fallback(
            [("https://api.example/v1", "m", {"Authorization": "k"})],
            [{"role": "user", "content": "oi"}],
            fallback_on_empty=False,
    ):
        out.append(chunk)

    assert len(calls) == 1
    assert any("no substantive output" in c for c in out), out


def test_the_agent_loop_opts_in():
    """stream_agent_loop must ask for the retry; without it nothing retries."""
    import inspect

    import src.agent_loop as agent_loop

    assert "retry_empty_completion=True" in inspect.getsource(agent_loop)


def test_foreground_policy_leaves_fallback_on_empty_false():
    """Documents why the retry must not be gated on that flag."""
    from src.foreground_model_routing import ForegroundModelPolicy

    assert ForegroundModelPolicy().fallback_on_empty is False


# --- The retry must not reproduce the same empty stream ---------------------
#
# Empty completions arrive in bursts and instant retries failed identically,
# three in a row, before the turn was lost. A reused keepalive connection that
# a peer half-closed is the classic source of "HTTP 200 with no body": every
# standalone request from a fresh client succeeds while the long-lived pool
# intermittently returns an empty stream. So the retry waits, and asks for a
# connection of its own without disturbing the shared client.

def test_the_retry_waits_before_reissuing():
    import inspect

    from src import llm_core

    source = inspect.getsource(llm_core.stream_llm_with_fallback)
    assert "await asyncio.sleep(backoff)" in source, (
        "an instant retry lands in the same window and fails the same way"
    )
    assert len(llm_core._EMPTY_COMPLETION_BACKOFF_SECONDS) >= 1
    assert all(d > 0 for d in llm_core._EMPTY_COMPLETION_BACKOFF_SECONDS)


def test_a_retry_gets_its_own_connection_once():
    """The one-shot client is consumed by the next request, not sticky."""
    import httpx

    from src import llm_core

    shared = llm_core._get_http_client()          # prime the process-wide client
    llm_core._one_shot_client.set(True)
    one_shot = llm_core._get_http_client()
    after = llm_core._get_http_client()
    assert isinstance(one_shot, httpx.AsyncClient)
    assert one_shot is not shared, "the retry must not reuse the pooled client"
    assert after is shared, "the one-shot must be consumed by the first request"
    # Leave no state behind: the flag is process-wide and the next test reads it.
    llm_core._one_shot_client.set(False)


def test_the_shared_client_is_untouched_by_a_retry():
    from src import llm_core

    assert llm_core._one_shot_client.get() is False
    assert llm_core._get_http_client() is llm_core._get_http_client()


# --- A configured fallback must be usable on an empty completion -----------
#
# MiniMax intermittently answers HTTP 200 with no body. A same-candidate
# retry covers one blip; three in a row ended the turn outright because the
# foreground policy refused to move to another candidate on empty. Configuring
# a fallback and then not using it on the failure that motivated it is not a
# configuration. It is opt-in: with no fallback configured there is nothing to
# move to, so the flag stays false and the retry remains the whole recovery.

def test_no_configured_fallback_keeps_empty_completion_strict():
    from src.foreground_model_routing import ForegroundModelPolicy

    assert ForegroundModelPolicy().fallback_on_empty is False


def test_empty_completion_fallback_is_opt_in_per_owner(monkeypatch):
    """A configured fallback does not by itself authorise switching on empty.

    Some providers answer HTTP 200 with no body. Whether the answer should then
    come from a different model is the owner's call, so it is a preference and
    not implied by configuring a fallback.
    """
    from src.foreground_model_routing import (
        FOREGROUND_FALLBACK_ON_EMPTY_KEY, resolve_foreground_model_policy,
    )

    entries = [{"endpoint_id": "e1", "model": "backup"}]
    candidates = [("https://backup.example/v1", "backup", {})]

    def fake(entries_, owner=None, require_exact_model=False):
        return candidates

    monkeypatch.setattr(
        "src.foreground_model_routing.resolve_fallback_entries", fake)
    monkeypatch.setattr(
        "src.foreground_model_routing._load_policy_preferences",
        lambda owner: {
            "foreground_fallback_enabled": True,
            "foreground_model_fallbacks": entries,
        })

    assert resolve_foreground_model_policy("alice").fallback_on_empty is False

    monkeypatch.setattr(
        "src.foreground_model_routing._load_policy_preferences",
        lambda owner: {
            "foreground_fallback_enabled": True,
            "foreground_model_fallbacks": entries,
            FOREGROUND_FALLBACK_ON_EMPTY_KEY: True,
        })

    assert resolve_foreground_model_policy("alice").fallback_on_empty is True


# --- Enough retries for a provider that empties about half the time -------
#
# MiniMax-M3 answers HTTP 200 with a one-character body on roughly half its
# requests for a given payload, with no error and no moderation flag: the same
# captured request replays successfully on the next attempt. At two retries a
# turn still fell through to a different model a eighth of the time, which is
# not a failure but it is not M3 either. Four retries take that to ~3% while
# the worst case stays bounded by the backoff.

def test_the_retry_budget_outlasts_a_half_rate_provider():
    rate = 0.5
    attempts = llm_core._EMPTY_COMPLETION_RETRIES + 1
    assert 0.5 ** attempts < 0.05, (
        f"{attempts} attempts at a {rate:.0%} empty rate still falls through "
        f"{rate ** attempts:.1%} of the time"
    )


def test_the_backoff_covers_every_retry():
    assert len(llm_core._EMPTY_COMPLETION_BACKOFF_SECONDS) >= llm_core._EMPTY_COMPLETION_RETRIES
    assert all(d > 0 for d in llm_core._EMPTY_COMPLETION_BACKOFF_SECONDS)
    assert list(llm_core._EMPTY_COMPLETION_BACKOFF_SECONDS) == sorted(
        llm_core._EMPTY_COMPLETION_BACKOFF_SECONDS), "backoff must not shrink"


def test_the_worst_case_stays_bounded():
    total = sum(llm_core._EMPTY_COMPLETION_BACKOFF_SECONDS[:llm_core._EMPTY_COMPLETION_RETRIES])
    assert total < 60, f"a turn would stall {total:.0f}s before failing"
