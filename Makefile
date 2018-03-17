.PHONY: test docs

ENABLE_BRANCH_COVERAGE ?= 0
AUTO_FIX_IMPORTS ?= 0
USE_SYSTEM_RABBITMQ ?= 0
RABBITMQ_VERSION ?= 3.6.6

ifneq ($(AUTO_FIX_IMPORTS), 1)
  autofix = --check-only
endif

ifneq ($(USE_SYSTEM_RABBITMQ), 1)
  find_port = $(shell docker port nameko-rabbitmq | grep $(1) | awk -F":" '{print $$2}')
  rabbitmq_options = --amqp-uri "pyamqp://guest:guest@localhost:$(call find_port, ^5672)" --rabbit-api-uri "http://guest:guest@localhost:$(call find_port, ^15672)"
rabbitmq-container:
	docker stop nameko-rabbitmq || true
	docker run -d --rm -v nameko-rabbitmq-certs:/mnt/certs -P --name nameko-rabbitmq nameko/nameko-rabbitmq:$(RABBITMQ_VERSION)

rabbitmq: rabbitmq-container
	@printf "Waiting for container to be ready"
	@booting=1; \
	while [ $${booting} -ne 0 ] ; do \
		curl -s http://localhost:$(call find_port, ^15672)/api/overview > /dev/null; \
		booting=$$?; \
		sleep 2; \
		printf "."; \
	done; \
	printf "\n"
else
  system_version = $(shell curl -s http://guest:guest@localhost:15672/api/overview | sed -n 's/.*"management_version":"\([0-9]*\.[0-9]*\.[0-9]*\).*/\1/p')
rabbitmq:
	@echo "Using system-installed RabbitMQ"
	@if [ "$(call system_version)" != "$(RABBITMQ_VERSION)" ]; then\
		echo "System RabbitMQ does not match requested version ($(RABBITMQ_VERSION))"; \
		false; \
	fi
endif

static: imports flake8 pylint
test: static test_lib test_examples

flake8:
	flake8 nameko test

pylint:
	pylint --rcfile=pylintrc nameko -E

imports:
	isort -rc $(autofix) nameko test

test_lib: rabbitmq
	BRANCH=$(ENABLE_BRANCH_COVERAGE) py.test test --strict --timeout 30 --cov --cov-config=$(CURDIR)/.coveragerc $(call rabbitmq_options)

test_examples: rabbitmq
	BRANCH=$(ENABLE_BRANCH_COVERAGE) py.test docs/examples/test --strict --timeout 30 --cov=docs/examples --cov-config=$(CURDIR)/.coveragerc $(call rabbitmq_options)
	py.test docs/examples/testing $(call rabbitmq_options)

test_docs: docs spelling #linkcheck

docs:
	sphinx-build -n -b html -d docs/build/doctrees docs docs/build/html

spelling:
	sphinx-build -b spelling -d docs/build/doctrees docs docs/build/spelling

linkcheck:
	sphinx-build -W -b linkcheck -d docs/build/doctrees docs docs/build/linkcheck
