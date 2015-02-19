noop:
	@true

.PHONY: noop

pytest:
	coverage run --concurrency=eventlet --source nameko -m pytest test

flake8:
	flake8 nameko test

pylint:
	pylint nameko -E

test: flake8 pylint pytest coverage-check

coverage-check:
	coverage report --fail-under=100

sphinx:
	sphinx-build -n -b html -d docs/build/doctrees docs docs/build/html

spelling:
	sphinx-build -b spelling -d docs/build/doctrees docs docs/build/spelling

docs: spelling sphinx
