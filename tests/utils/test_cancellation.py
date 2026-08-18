"""Tests for the cooperative cancellation primitives."""

import pytest

from data_access_service.utils.cancellation import (
    Cancellation,
    ClientGoneError,
    GroupCancellation,
    raise_if_client_gone,
)


def test_cancellation_starts_active_and_raises_once_cancelled():
    cancellation = Cancellation()

    assert cancellation.is_cancelled is False
    cancellation.raise_if_client_gone()  # no raise

    cancellation.cancel()

    assert cancellation.is_cancelled is True
    with pytest.raises(ClientGoneError):
        cancellation.raise_if_client_gone()


def test_client_gone_error_is_not_a_value_error():
    # estimate_datasets_size catches ValueError per key to skip unsupported
    # formats. If ClientGoneError were a ValueError it would be swallowed there
    # and the loop would carry on to the next key.
    assert not issubclass(ClientGoneError, ValueError)


def test_raise_if_client_gone_helper_ignores_none():
    raise_if_client_gone(None)  # no raise

    cancelled = Cancellation()
    cancelled.cancel()
    with pytest.raises(ClientGoneError):
        raise_if_client_gone(cancelled)


def test_group_is_not_cancelled_while_one_client_remains():
    group = GroupCancellation()
    first, second = Cancellation(), Cancellation()
    group.attach(first)
    group.attach(second)

    assert group.is_cancelled is False

    first.cancel()
    assert group.is_cancelled is False  # second is still waiting

    second.cancel()
    assert group.is_cancelled is True


def test_group_with_no_clients_is_not_cancelled():
    assert GroupCancellation().is_cancelled is False


def test_group_client_without_cancellation_never_goes_away():
    # A batch job or test attaches None: it has no client to disconnect.
    group = GroupCancellation()
    client = Cancellation()
    group.attach(client)
    group.attach(None)

    client.cancel()

    assert group.is_cancelled is False


def test_group_can_be_cancelled_directly():
    group = GroupCancellation()
    group.attach(Cancellation())

    group.cancel()

    assert group.is_cancelled is True
