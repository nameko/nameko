import pytest

from nameko import config, config_setup, config_update


@pytest.mark.usefixtures("empty_config")
def test_config_setup():

    assert config == {}

    config_setup({"spam": "ham"})

    assert config == {"spam": "ham"}

    config_setup({})

    assert config == {}


@pytest.mark.usefixtures("empty_config")
def test_config_setup_context_manager():

    with config_setup({"spam": "ham"}):
        assert config == {"spam": "ham"}

    assert config == {}

    with config_setup({"spam": "egg"}):
        assert config == {"spam": "egg"}
        with config_setup({"spam": "ham"}):
            assert config == {"spam": "ham"}
        assert config == {"spam": "egg"}

    assert config == {}


@pytest.mark.usefixtures("empty_config")
def test_config_setup_replaces_whole_config():

    config_setup({"spam": "ham", "egg": "spam"})

    with config_setup({"spam": "egg"}):
        assert config == {"spam": "egg"}

    assert config == {"spam": "ham", "egg": "spam"}


@pytest.mark.usefixtures("empty_config")
def test_config_update():

    config_setup({"spam": "ham", "egg": "spam"})

    with config_update({"spam": "egg"}):
        assert config == {"spam": "egg", "egg": "spam"}

    assert config == {"spam": "ham", "egg": "spam"}
