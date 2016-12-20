.PHONY: test docs

ENABLE_BRANCH_COVERAGE ?= 0

test: flake8 pylint test_lib test_examples

flake8:
	flake8 nameko test

pylint:
	pylint --rcfile=pylintrc nameko -E

test_lib:
	BRANCH=$(ENABLE_BRANCH_COVERAGE) py.test test --strict --timeout 30 --cov --cov-config=$(CURDIR)/.coveragerc

test_examples:
	BRANCH=$(ENABLE_BRANCH_COVERAGE) py.test docs/examples/test --strict --timeout 30 --cov=docs/examples --cov-config=$(CURDIR)/.coveragerc
	py.test docs/examples/testing

test_docs: docs spelling #linkcheck

docs:
	sphinx-build -n -b html -d docs/build/doctrees docs docs/build/html

spelling:
	sphinx-build -b spelling -d docs/build/doctrees docs docs/build/spelling

linkcheck:
	sphinx-build -W -b linkcheck -d docs/build/doctrees docs docs/build/linkcheck
