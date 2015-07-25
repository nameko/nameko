.PHONY: test flake8 pylint pytest test_docs docs spelling

test: flake8 pylint pytest

flake8:
	flake8 nameko test

pylint:
	pylint --rcfile=pylintrc nameko -E

pytest:
	coverage run --concurrency=eventlet --source nameko -m pytest test
	coverage report --show-missing --fail-under=100

test_docs: docs spelling

docs:
	sphinx-build -n -b html -d docs/build/doctrees docs docs/build/html

spelling:
	sphinx-build -b spelling -d docs/build/doctrees docs docs/build/spelling
	# sphinx-build -W -b linkcheck -d docs/build/doctrees docs docs/build/linkcheck
