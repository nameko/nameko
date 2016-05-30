.PHONY: test docs

test: flake8 pylint test_lib test_examples

flake8:
	flake8 nameko test

pylint:
	pylint --rcfile=pylintrc nameko -E

coverage_combine:
	coverage combine $(COVERAGE_OPTS)

coverage_report: coverage_combine
	coverage report $(COVERAGE_OPTS)

coverage_erase:
	coverage erase $(COVERAGE_OPTS)

coverage_lib:
	coverage run $(COVERAGE_OPTS) --source $(CURDIR)/nameko -m pytest test

test_lib: coverage_erase coverage_lib coverage_report

coverage_examples:
	coverage run --rcfile $(CURDIR)/.coveragerc --source docs/examples -m pytest docs/examples/test

test_examples: coverage_erase coverage_examples coverage_report
	py.test docs/examples/testing

test_docs: docs spelling #linkcheck

docs:
	sphinx-build -n -b html -d docs/build/doctrees docs docs/build/html

spelling:
	sphinx-build -b spelling -d docs/build/doctrees docs docs/build/spelling

linkcheck:
	sphinx-build -W -b linkcheck -d docs/build/doctrees docs docs/build/linkcheck
