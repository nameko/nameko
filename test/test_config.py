import pytest

from nameko import config, config_setup, config_update


@pytest.mark.usefixtures("empty_config")
def test_config_setup():

    assert config == {}

    config_setup({"HEARTBEAT": 10})

    assert config == {"HEARTBEAT": 10}

    config_setup({})

    assert config == {}


@pytest.mark.usefixtures("empty_config")
def test_config_setup_context_manager():

    with config_setup({"HEARTBEAT": 10}):
        assert config == {"HEARTBEAT": 10}

    assert config == {}

    with config_setup({"HEARTBEAT": 40}):
        assert config == {"HEARTBEAT": 40}
        with config_setup({"HEARTBEAT": 10}):
            assert config == {"HEARTBEAT": 10}
        assert config == {"HEARTBEAT": 40}

    assert config == {}


@pytest.mark.usefixtures("empty_config")
def test_config_setup_replaces_whole_config():

    config_setup({"HEARTBEAT": 10, "ACCEPT": "YAML"})

    with config_setup({"HEARTBEAT": 40}):
        assert config == {"HEARTBEAT": 40}

    assert config == {"HEARTBEAT": 10, "ACCEPT": "YAML"}


@pytest.mark.usefixtures("empty_config")
def test_config_update():

    config_setup({"HEARTBEAT": 10, "ACCEPT": "YAML"})

    with config_update({"HEARTBEAT": 40}):
        assert config == {"HEARTBEAT": 40, "ACCEPT": "YAML"}

    assert config == {"HEARTBEAT": 10, "ACCEPT": "YAML"}
