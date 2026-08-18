"""Tests for SingleFlight: identical concurrent work runs once and is shared."""

import threading

import pytest

from data_access_service.utils.cancellation import Cancellation, ClientGoneError
from data_access_service.utils.single_flight import SingleFlight

# Short poll so a follower notices its own client quickly in tests.
POLL = 0.01


def _run_in_threads(target, count, first_index=0):
    """Run target(index) in `count` threads and return them, already started."""
    threads = [
        threading.Thread(target=target, args=(first_index + i,)) for i in range(count)
    ]
    for thread in threads:
        thread.start()
    return threads


def test_concurrent_callers_with_the_same_key_run_the_work_once():
    single_flight = SingleFlight(poll_interval=POLL)
    calls = []
    started = threading.Event()
    release = threading.Event()
    results = {}

    def work(_group):
        calls.append(1)
        started.set()
        release.wait(5)
        return "estimate"

    def caller(index):
        results[index] = single_flight.run("same-key", work, Cancellation())

    first = _run_in_threads(caller, 1)[0]
    assert started.wait(5)  # the leader is inside work()

    followers = _run_in_threads(caller, 3, first_index=1)
    release.set()
    for thread in [first, *followers]:
        thread.join(5)

    assert len(calls) == 1
    assert results == {0: "estimate", 1: "estimate", 2: "estimate", 3: "estimate"}


def test_different_keys_run_separately():
    single_flight = SingleFlight(poll_interval=POLL)
    calls = []

    def work(_group):
        calls.append(1)
        return "estimate"

    single_flight.run("key-a", work, Cancellation())
    single_flight.run("key-b", work, Cancellation())

    assert len(calls) == 2


def test_key_is_reusable_after_the_job_finishes():
    single_flight = SingleFlight(poll_interval=POLL)
    calls = []

    def work(_group):
        calls.append(1)
        return len(calls)

    assert single_flight.run("key", work) == 1
    assert single_flight.run("key", work) == 2


def test_shared_job_keeps_running_until_every_client_has_gone():
    single_flight = SingleFlight(poll_interval=POLL)
    started = threading.Event()
    saw_cancelled = {}
    leader_cancellation = Cancellation()
    follower_cancellation = Cancellation()

    def work(group):
        started.set()
        # Poll the SHARED cancellation, the way the estimate checkpoints do.
        for _ in range(500):
            if group.is_cancelled:
                saw_cancelled["value"] = True
                raise ClientGoneError("client disconnected")
            threading.Event().wait(POLL)
        saw_cancelled["value"] = False
        return "finished"

    def lead():
        with pytest.raises(ClientGoneError):
            single_flight.run("key", work, leader_cancellation)

    def follow():
        with pytest.raises(ClientGoneError):
            single_flight.run("key", work, follower_cancellation)

    leader = threading.Thread(target=lead)
    leader.start()
    assert started.wait(5)

    follower = threading.Thread(target=follow)
    follower.start()
    threading.Event().wait(POLL * 5)

    # The leader's client leaves; the follower is still waiting for the answer.
    leader_cancellation.cancel()
    threading.Event().wait(POLL * 5)
    assert leader.is_alive(), "work stopped while a client was still attached"

    # Now the last client leaves too.
    follower_cancellation.cancel()
    leader.join(5)
    follower.join(5)

    assert saw_cancelled["value"] is True
    assert not leader.is_alive()
    assert not follower.is_alive()


def test_follower_stops_waiting_when_its_own_client_goes():
    single_flight = SingleFlight(poll_interval=POLL)
    started = threading.Event()
    release = threading.Event()

    def work(_group):
        started.set()
        release.wait(5)
        return "estimate"

    def lead():
        single_flight.run("key", work, Cancellation())

    leader = threading.Thread(target=lead)
    leader.start()
    assert started.wait(5)

    follower_cancellation = Cancellation()
    follower_cancellation.cancel()
    with pytest.raises(ClientGoneError):
        single_flight.run("key", work, follower_cancellation)

    release.set()
    leader.join(5)


def test_failure_is_shared_with_followers():
    single_flight = SingleFlight(poll_interval=POLL)
    started = threading.Event()
    release = threading.Event()
    errors = {}

    def work(_group):
        started.set()
        release.wait(5)
        raise ValueError("no such key")

    def caller(index):
        try:
            single_flight.run("key", work, Cancellation())
        except ValueError as e:
            errors[index] = str(e)

    leader = threading.Thread(target=caller, args=(0,))
    leader.start()
    assert started.wait(5)

    follower = threading.Thread(target=caller, args=(1,))
    follower.start()
    release.set()
    leader.join(5)
    follower.join(5)

    assert errors == {0: "no such key", 1: "no such key"}


def test_new_caller_does_not_attach_to_a_cancelled_job():
    single_flight = SingleFlight(poll_interval=POLL)
    started = threading.Event()
    calls = []
    leader_cancellation = Cancellation()

    def work(group):
        calls.append(1)
        started.set()
        for _ in range(500):
            if group.is_cancelled:
                raise ClientGoneError("client disconnected")
            threading.Event().wait(POLL)
        return "estimate"

    def lead():
        with pytest.raises(ClientGoneError):
            single_flight.run("key", work, leader_cancellation)

    leader = threading.Thread(target=lead)
    leader.start()
    assert started.wait(5)
    leader_cancellation.cancel()

    # A fresh client must get its own job, not the one that is winding down.
    def quick_work(_group):
        calls.append(1)
        return "estimate"

    assert single_flight.run("key", quick_work, Cancellation()) == "estimate"
    leader.join(5)
    assert len(calls) == 2
