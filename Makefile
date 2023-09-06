UMLS_SSSOM_TSV="umls.sssom.tsv"
RESOURCE="umls"

all: umls_hp.sssom.tsv umls_mesh.sssom.tsv umls_ncit.sssom.tsv 


umls_%.sssom.tsv:
	umls get-mappings --subject-prefixes umls --object-prefixes $*

umls_mesh_ncit_hp.sssom.tsv:
	umls get-mappings --subject-prefixes umls --object-prefixes mesh --object-prefixes ncit --object-prefixes hp