.PHONY: test flake8 pylint pytest test_docs docs spelling

test: flake8 pylint pytest

flake8:
	flake8 nameko test

pylint:
	pylint --rcfile=pylintrc nameko -E

pytest:
	py.test test --cov --cov-config=$(CURDIR)/.coveragerc

test_docs: docs spelling

docs:
	sphinx-build -n -b html -d docs/build/doctrees docs docs/build/html

spelling:
	sphinx-build -b spelling -d docs/build/doctrees docs docs/build/spelling
	# sphinx-build -W -b linkcheck -d docs/build/doctrees docs docs/build/linkcheck
