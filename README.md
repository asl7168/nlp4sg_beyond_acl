# Understanding the Dataset

## subcorpus_a 
All works published at an ACL(-adjacent) venue.

## subcorpus_b 
DEPRECATED?

## subcorpus_c 
All works published outside of ACL. 

# [create_subcorpora.py](create_subcorpora.py)
This file contains the functions that are responsible for downloading SemanticScholar datasets (S2ORC and Ppaers); extracting individual works from those JSONL files; and pairing those works with their equivalents in OpenAlex (as well as extracting the authors from works, as identified via OpenAlex).

## download_s2orc()
Downloads and gunzips s2orc JSONL files from Semantic Scholar.

## download_s2_papers()
Downloads and gunzips the Semantic Scholar 'Papers' JSONL files.

## extract_from_s2orc()
Extracts individual works from the downloaded S2ORC JSONL files, organizing them based on whether or not that paper was published in an ACL(-adjacent) venue. All files are stored in individual directories named for their SemanticScholar CorpusID; and those directories will additionally contain metadata extracted from the Papers dataset, and from OpenAlex. 

CorpusID directories are stored within a directory that is named for the first four digits of the CorpusID; this provides a simple way to group multiple papers together for later iteration/data searching.

This function *additionally* creates two text files, stored in the datasets directory, that store all ACL and non-ACL CorpusIDs for later use.

## get_s2_info() 
Extracts individual works' metadata from the downloaded Papers JSONL files, placing them in their appropriate CorpusID directory (as specified above).

## get_openalex_info()
Loops through all extracted works from SemanticScholar, matching them to their equivalent in OpenAlex. Then, creates an OpenAlex JSON file for each, containing the OpenAlex metadata; this file is used to supplement any metadata that may be missing in SemanticScholar, and helps to improve author disambiguation. 

## extract_authors() 
Loops through all extracted ACL works' OpenAlex files and extracts every author associated with that paper. Appends the ACL and non-ACL works of each author to the author file that is created for future use.


# [csv_builder.py](csv_builder.py)
This file contains the functions that collate paper and author metadata into CSVs for data analysis and further augmentation (e.g. with Google Scholar h5 index info). 

## make_author_csv() 
Collates OpenAlex author information from all papers into a single CSV. 

## csv_builder() 
Navigates through each OpenAlex metadata JSON file, extracts key information, and appends to a master works CSV. Utilizes authors.csv (as above) to determine which contributing author has the most ACL works of them all.

## merge_csvs()
Combines any sub-CSVs created by csv_builder() into a single works CSV. 