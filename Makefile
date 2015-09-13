.PHONY: test docs

test: flake8 pylint test_codebase

flake8:
	flake8 nameko test

pylint:
	pylint --rcfile=pylintrc nameko -E

test_codebase:
	py.test test --cov=$(CURDIR)/nameko --cov-config=$(CURDIR)/.coveragerc

test_examples:
	py.test docs/examples/test --cov=docs/examples
	py.test docs/examples/testing

test_docs:
	docs spelling

docs:
	sphinx-build -n -b html -d docs/build/doctrees docs docs/build/html

spelling:
	sphinx-build -b spelling -d docs/build/doctrees docs docs/build/spelling
	# sphinx-build -W -b linkcheck -d docs/build/doctrees docs docs/build/linkcheck
