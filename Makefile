.PHONY: test docs

export ENABLE_BRANCH_COVERAGE ?= 0
export AUTO_FIX_IMPORTS ?= 0
export CONCURRENCY_BACKEND ?= eventlet
export COVERAGE_FAIL_UNDER ?= 100

ifneq ($(AUTO_FIX_IMPORTS), 1)
  autofix = --check-only
endif

static: imports flake8 pylint
test: static test_lib test_examples

flake8:
	flake8 nameko test

pylint:
	pylint --rcfile=pylintrc nameko -E

imports:
	isort -rc $(autofix) nameko test

test_lib:
	coverage run -m pytest test --strict --timeout 30
	coverage report

test_examples:
	py.test docs/examples/test --strict --timeout 30 --cov=docs/examples --cov-config=$(CURDIR)/.coveragerc
	py.test docs/examples/testing

test_docs: docs spelling #linkcheck

docs:
	sphinx-build -n -b html -d docs/build/doctrees docs docs/build/html

spelling:
	sphinx-build -b spelling -d docs/build/doctrees docs docs/build/spelling

linkcheck:
	sphinx-build -W -b linkcheck -d docs/build/doctrees docs docs/build/linkcheck
