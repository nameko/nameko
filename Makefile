# See https://bugs.launchpad.net/pyflakes/+bug/1223150
export PYFLAKES_NODOCTEST=1

noop:
	@true

.PHONY: noop

requirements:
	pip install -r dev_requirements.txt

develop: requirements
	python setup.py develop

pytest:
	coverage run --source nameko -m pytest test
	coverage report

flake8:
	flake8 nameko test

pylint:
	pylint nameko -E

test: pytest pylint flake8 coverage-check

full-test: requirements test

coverage-check:
	coverage report | grep "TOTAL.*100%" > /dev/null

sphinx:
	sphinx-build -b html -d ./docs/build/doctrees  ./docs ./docs/build/html

docs/api/modules.rst: $(wildcard nameko/**/*.py)
	sphinx-apidoc -e -f -o docs/api nameko

autodoc: docs/api/modules.rst

docs: autodoc sphinx
