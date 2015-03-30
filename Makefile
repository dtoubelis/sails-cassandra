MOCHA_OPTS = --check-leaks
REPORTER = spec

test: test-unit

test-integration:
	@NODE_ENV=test node test/integration/runner.js

test-unit:
	@NODE_ENV=test ./node_modules/.bin/mocha \
		--reporter $(REPORTER) \
		$(MOCHA_OPTS) \
		test/unit/*.js

test-load:
	@NODE_ENV=test ./node_modules/.bin/mocha \
		--reporter $(REPORTER) \
		$(MOCHA_OPTS) \
		test/load/*.js
