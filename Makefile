UMLS_SSSOM_TSV="umls.sssom.tsv"
RESOURCE="umls"

mappings:
	umls get-mappings --resource $(RESOURCE) --output $(UMLS_SSSOM_TSV)