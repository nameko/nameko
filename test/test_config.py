import pytest

from nameko import config, setup_config, update_config


@pytest.mark.usefixtures("empty_config")
def test_setup_config():

    assert config == {}

    setup_config({"HEARTBEAT": 10})

    assert config == {"HEARTBEAT": 10}

    setup_config({})

    assert config == {}


@pytest.mark.usefixtures("empty_config")
def test_setup_config_context_manager():

    with setup_config({"HEARTBEAT": 10}):
        assert config == {"HEARTBEAT": 10}

    assert config == {}

    with setup_config({"HEARTBEAT": 40}):
        assert config == {"HEARTBEAT": 40}
        with setup_config({"HEARTBEAT": 10}):
            assert config == {"HEARTBEAT": 10}
        assert config == {"HEARTBEAT": 40}

    assert config == {}


@pytest.mark.usefixtures("empty_config")
def test_setup_config_replaces_whole_config():

    setup_config({"HEARTBEAT": 10, "ACCEPT": "YAML"})

    with setup_config({"HEARTBEAT": 40}):
        assert config == {"HEARTBEAT": 40}

    assert config == {"HEARTBEAT": 10, "ACCEPT": "YAML"}


@pytest.mark.usefixtures("empty_config")
def test_update_config():

    setup_config({"HEARTBEAT": 10, "ACCEPT": "YAML"})

    with update_config({"HEARTBEAT": 40}):
        assert config == {"HEARTBEAT": 40, "ACCEPT": "YAML"}

    assert config == {"HEARTBEAT": 10, "ACCEPT": "YAML"}
