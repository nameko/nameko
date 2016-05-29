.PHONY: test docs

test: flake8 pylint test_lib test_examples

flake8:
	flake8 nameko test

pylint:
	pylint --rcfile=pylintrc nameko -E

test_lib:
	py.test test --cov=$(CURDIR)/nameko --cov-config=$(CURDIR)/.coveragerc

test_examples:
	py.test docs/examples/test --cov=docs/examples
	py.test docs/examples/testing

test_docs: docs spelling #linkcheck

docs:
	sphinx-build -n -b html -d docs/build/doctrees docs docs/build/html

spelling:
	sphinx-build -b spelling -d docs/build/doctrees docs docs/build/spelling

linkcheck:
	sphinx-build -W -b linkcheck -d docs/build/doctrees docs docs/build/linkcheck
