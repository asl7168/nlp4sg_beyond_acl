# NOTE: while some functions provide (or previously provided) the option to provide custom 
# filepaths, we recommend only modifying the paths in this file (namely corpora_path, since 
# all others paths rely on it). Otherwise, custom filepaths are untested

# main directory, in which datasets are downloaded and subcorpora are built
corpora_path = "/projects/b1170/corpora/comp_ling_meta"

datasets_path = f"{corpora_path}/datasets"  # where datasets are stored
authors_path = f"{corpora_path}/authors"  # ---

# https://api.semanticscholar.org/api-docs/datasets
s2orc_path = f"{datasets_path}/s2orc"  # full text, abstracts, etc.
s2_papers_db_path = f"{datasets_path}/s2_papers"  # metadata
csvs_path = f"{datasets_path}/csvs"  # results/data CSVs

# subcorpora created by create_subcorpora.py
sub_a = f"{corpora_path}/subcorpus_a"  # where ACL files are stored
sub_b = f"{corpora_path}/subcorpus_b"  # ---
sub_c = f"{corpora_path}/subcorpus_c"  # ---