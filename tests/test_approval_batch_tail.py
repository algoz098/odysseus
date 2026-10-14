"""An approval card must not silently swallow the rest of a tool batch.

The batch loop breaks at a pending approval on purpose: the user has to
decide before anything else in that batch runs. Breaking is correct. Breaking
*silently* is not -- the assistant message still declares every call in the
batch, so a call that never ran used to be answered with an empty tool result.
The model read that as "done", never re-issued the call, and the rest of the
request quietly never ran.
"""
import pytest

from src.agent_loop import _append_tool_results, answer_calls_dropped_by_approval


def _batch(paused_after=0):
    """A batch of two parallel calls, `paused_after` of which have run."""
    converted_calls = [
        {"id": "call_1_0", "name": "mcp__email__list_emails", "arguments": "{}"},
        {"id": "call_1_1", "name": "manage_calendar", "arguments": "{}"},
    ]
    ran = [f"### {c['name']}\n```\nok\n```" for c in converted_calls[:paused_after]]
    return converted_calls, list(ran)


def test_a_call_dropped_by_an_approval_is_answered_not_silently_empty():
    converted_calls, texts = _batch(paused_after=1)

    added = answer_calls_dropped_by_approval(texts, list(texts), converted_calls)

    assert added == 1
    assert len(texts) == len(converted_calls)
    assert texts[1].strip()
    assert "not executed" in texts[1].lower()
    assert "approval" in texts[1].lower()


def test_every_declared_call_ends_up_with_a_tool_message():
    """An unanswered tool_call_id makes the next request malformed."""
    converted_calls, texts = _batch(paused_after=1)
    results = list(texts)
    answer_calls_dropped_by_approval(texts, results, converted_calls)
    messages = []

    _append_tool_results(
        messages, "checking both in parallel", converted_calls,
        results, list(texts), True, 1,
    )

    tool_messages = [m for m in messages if m.get("role") == "tool"]
    assert [m["tool_call_id"] for m in tool_messages] == ["call_1_0", "call_1_1"]
    assert all(m["content"].strip() for m in tool_messages), (
        "an empty tool result reads as a completed no-op, not as 'never ran'"
    )


def test_a_fully_executed_batch_is_left_alone():
    converted_calls, texts = _batch(paused_after=2)

    added = answer_calls_dropped_by_approval(texts, list(texts), converted_calls)

    assert added == 0
    assert texts == ["### mcp__email__list_emails\n```\nok\n```",
                     "### manage_calendar\n```\nok\n```"]


@pytest.mark.parametrize("declared,ran", [(2, 0), (3, 1), (3, 0), (1, 0)])
def test_the_tail_is_always_filled_to_the_declared_length(declared, ran):
    converted_calls = [
        {"id": f"call_1_{i}", "name": f"tool_{i}", "arguments": "{}"}
        for i in range(declared)
    ]
    texts = [f"result {i}" for i in range(ran)]

    added = answer_calls_dropped_by_approval(texts, list(texts), converted_calls)

    assert added == declared - ran
    assert len(texts) == declared
