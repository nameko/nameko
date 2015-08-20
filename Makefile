# shortcuts for local dev
#
.PHONY: test flake8 pylint pytest docs spelling test_docs

test: flake8 pylint pytest

flake8:
	flake8 nameko test

pylint:
	pylint nameko -E

pytest:
	py.test test --cov

docs:
	tox -e docs

spelling:
	tox -e spelling

test_docs:
	spelling
	docs
