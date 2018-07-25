.PHONY: clean
clean:
	rm -rf dist
	rm -rf .tox
	rm -rf nosetests.xml
	rm -rf pylint.out

.PHONY: test
test:
	tox -e nose || true

.PHONY: lint
lint:
	tox -e pylint || true
