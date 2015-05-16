MOCHA_OPTS = --check-leaks
REPORTER = spec

test: test-unit

test-lint:
	@NODE_ENV=test ./node_modules/.bin/jshint \
		lib/*.js

test-unit:
	@NODE_ENV=test ./node_modules/.bin/mocha \
		--reporter $(REPORTER) \
		$(MOCHA_OPTS) \
		test/unit/*.js

test-integration:
	@NODE_ENV=test node test/integration/runner.js

.PHONY: test test-lint test-unit test-integration
