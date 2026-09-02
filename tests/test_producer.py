from datetime import UTC, datetime

from app.contracts import validate_event
from app.producer import (
    generate_events,
    prepare_delivery_sequence,
)


def test_generate_events_creates_valid_events():
    start_time = datetime(
        2026,
        9,
        1,
        20,
        0,
        tzinfo=UTC,
    )

    events = generate_events(
        count=6,
        seed=42,
        start_time=start_time,
    )

    assert len(events) == 6

    for event in events:
        validate_event(event)


def test_generate_events_rotates_nodes():
    events = generate_events(
        count=6,
        seed=42,
    )

    assert [event["key"] for event in events] == [
        "NODE-01",
        "NODE-02",
        "NODE-03",
        "NODE-01",
        "NODE-02",
        "NODE-03",
    ]


def test_duplicate_rate_one_duplicates_every_event():
    events = generate_events(
        count=3,
        seed=42,
    )

    delivery = prepare_delivery_sequence(
        events,
        duplicate_rate=1.0,
        out_of_order_rate=0.0,
        seed=43,
    )

    assert len(delivery) == 6

    assert delivery[0]["event_id"] == delivery[1]["event_id"]
    assert delivery[2]["event_id"] == delivery[3]["event_id"]
    assert delivery[4]["event_id"] == delivery[5]["event_id"]


def test_out_of_order_changes_delivery_order():
    events = generate_events(
        count=4,
        seed=42,
    )

    delivery = prepare_delivery_sequence(
        events,
        duplicate_rate=0.0,
        out_of_order_rate=1.0,
        seed=43,
    )

    assert delivery[0]["event_time"] > delivery[1]["event_time"]