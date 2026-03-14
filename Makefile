.PHONY: openapi rm-openapi

openapi:
	openapi-python-client generate \
		--overwrite \
		--path ../openapi.json \
		--output-path src/data_pebbles/client \
		--meta none

rm-openapi:
	rm -rf src/data_pebbles/client