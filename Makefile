.PHONY update-docs

update-docs:
	@echo "Updating docs..."
	@typer src/cdf/cli.py utils docs --name=cdf >docs/cli_reference.md
	@pydoc-markdown -I src/cdf >docs/api_reference.md
	@echo "Done."

