noop:
	@true

.PHONY: noop

dev_extras:
	pip install -e .[dev] -q

docs_extras:
	pip install -e .[docs] -q

pytest: dev_extras
	coverage run --concurrency=eventlet --source nameko -m pytest test

flake8: dev_extras
	flake8 nameko test

pylint: dev_extras
	pylint nameko -E

test: flake8 pylint pytest coverage-check

coverage-check: dev_extras
	coverage report --fail-under=100

sphinx: docs_extras
	sphinx-build -n -b html -d docs/build/doctrees docs docs/build/html

spelling: docs_extras
	sphinx-build -b spelling -d docs/build/doctrees docs docs/build/spelling

docs: sphinx spelling
