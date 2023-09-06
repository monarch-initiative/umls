UMLS_SSSOM_TSV="umls.sssom.tsv"
RESOURCE="umls"

all: all-umls all-x
all-umls: umls_hp.sssom.tsv umls_mesh.sssom.tsv umls_ncit.sssom.tsv
all-x: hp_mesh.sssom.tsv hp_ncit.sssom.tsv ncit_mesh.sssom.tsv hp_ncit_mesh.sssom.tsv


umls_%.sssom.tsv:
	umls get-mappings --subject-prefixes umls --object-prefixes $*

umls_mesh_ncit_hp.sssom.tsv:
	umls get-mappings --subject-prefixes umls --object-prefixes mesh --object-prefixes ncit --object-prefixes hp

hp_mesh.sssom.tsv:
	umls get-x-mappings --object-prefixes hp --object-prefixes mesh

hp_ncit.sssom.tsv:
	umls get-x-mappings --object-prefixes hp --object-prefixes ncit

ncit_mesh.sssom.tsv:
	umls get-x-mappings --object-prefixes ncit --object-prefixes mesh

hp_ncit_mesh.sssom.tsv:
	umls get-x-mappings --object-prefixes hp --object-prefixes ncit --object-prefixes mesh