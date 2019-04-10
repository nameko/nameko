.PHONY: test docs

ENABLE_BRANCH_COVERAGE ?= 0
AUTO_FIX_IMPORTS ?= 0

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
	BRANCH=$(ENABLE_BRANCH_COVERAGE) coverage run -m pytest test --strict --timeout 30
	BRANCH=$(ENABLE_BRANCH_COVERAGE) coverage report

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

rabbitmq-container:
	docker run -d --rm -p 15672:15672 -p 5672:5672 -p 5671:5671 --name nameko-rabbitmq nameko/nameko-rabbitmq:3.6.6
	docker cp nameko-rabbitmq:/srv/ssl certs
