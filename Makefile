.PHONY: update-docs

update-docs:
	@echo "Updating docs..."
	@pydoc-markdown -I src/cdf >docs/api_reference.md
	@echo "Done."

