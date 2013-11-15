from mock import Mock

from nameko.legacy import responses


def test_iter_rpcresponses():
    response_list = [
        Mock(payload={'id': 1, 'failure': False, 'ending': False}),
        Mock(payload={'id': 2, 'failure': False, 'ending': False}),
        Mock(payload={'id': 3, 'failure': False, 'ending': True}),
    ]

    iter_ = responses.iter_rpcresponses(response_list)
    ret = responses.last(iter_)

    # should be the message preceeding the `ending`
    assert ret.payload['id'] == 2


def test_iter_rpcresponses_ending_only():
    response_list = [
        Mock(payload={'id': 3, 'failure': False, 'ending': True}),
    ]

    iter_ = responses.iter_rpcresponses(response_list)

    # should not include the ending message
    assert list(iter_) == []
