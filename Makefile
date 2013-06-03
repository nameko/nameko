noop:
	@true

.PHONY: noop

requirements:
	pip install -r requirements.txt
	pip install -r test_requirements.txt


develop: requirements
	python setup.py develop

pytest:
	py.test --cov nameko test --cov-report term-missing

flake8:
	flake8 --ignore=E128 nameko test

pyflakes:
	pyflakes nameko

pylint:
	pylint nameko -E

test: pytest pylint pyflakes flake8

full-test: requirements test
