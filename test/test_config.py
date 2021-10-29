import pytest

from nameko import config


@pytest.mark.usefixtures("empty_config")
def test_setup():

    assert config == {}

    config.setup({"HEARTBEAT": 10})

    assert config == {"HEARTBEAT": 10}

    config.setup({})

    assert config == {}


@pytest.mark.usefixtures("empty_config")
def test_patch_full_context_manager():

    config.setup({"HEARTBEAT": 10, "ACCEPT": "YAML"})

    with config.patch({"HEARTBEAT": 40}, clear=True):
        assert config == {"HEARTBEAT": 40}

    assert config == {"HEARTBEAT": 10, "ACCEPT": "YAML"}


@pytest.mark.usefixtures("empty_config")
def test_patch_full_nesting_context_managers():

    with config.patch({"HEARTBEAT": 10}):
        assert config == {"HEARTBEAT": 10}

    assert config == {}

    with config.patch({"HEARTBEAT": 40}, clear=True):
        assert config == {"HEARTBEAT": 40}
        with config.patch({"HEARTBEAT": 10}, clear=True):
            assert config == {"HEARTBEAT": 10}
        assert config == {"HEARTBEAT": 40}

    assert config == {}


@pytest.mark.usefixtures("empty_config")
def test_patch_full_decorator():

    config.setup({"HEARTBEAT": 10, "ACCEPT": "YAML"})

    @config.patch({"HEARTBEAT": 40}, clear=True)
    def test():
        assert config == {"HEARTBEAT": 40}

    assert config == {"HEARTBEAT": 10, "ACCEPT": "YAML"}

    test()

    assert config == {"HEARTBEAT": 10, "ACCEPT": "YAML"}


@pytest.mark.usefixtures("empty_config")
def test_patch_context_manager():

    config.setup({"HEARTBEAT": 10, "ACCEPT": "YAML"})

    with config.patch({"HEARTBEAT": 40}):
        assert config == {"HEARTBEAT": 40, "ACCEPT": "YAML"}

    assert config == {"HEARTBEAT": 10, "ACCEPT": "YAML"}


@pytest.mark.usefixtures("empty_config")
def test_patch_decorator():

    config.setup({"HEARTBEAT": 10, "ACCEPT": "YAML"})

    @config.patch({"HEARTBEAT": 40})
    def test():
        assert config == {"HEARTBEAT": 40, "ACCEPT": "YAML"}

    assert config == {"HEARTBEAT": 10, "ACCEPT": "YAML"}

    test()

    assert config == {"HEARTBEAT": 10, "ACCEPT": "YAML"}


@pytest.mark.usefixtures("empty_config")
def test_patch_decorator_deals_with_failing_function_execution():

    config.setup({"HEARTBEAT": 10, "ACCEPT": "YAML"})

    exception = Exception('Boom!')

    @config.patch({"HEARTBEAT": 40})
    def test():
        assert config == {"HEARTBEAT": 40, "ACCEPT": "YAML"}
        raise exception

    assert config == {"HEARTBEAT": 10, "ACCEPT": "YAML"}

    with pytest.raises(Exception) as exc_info:
        test()

    assert exc_info.value == exception

    assert config == {"HEARTBEAT": 10, "ACCEPT": "YAML"}
