# Overview 
1. **corpus creation** 
    - ``create_subcorpora.py``:
    - ``csv_builder.py``:
    - ``paths.py``:
    - ``testing.py``:
2. **NLP4SG classification** 
    - ```nlp4sg_task1.py```: implements classifier from Adauto et al. (2023) to classify NLP papers as social-good focused or not
    - ```nlp4sg_task2.py```: predicts UN Social Development Category for NLP papers classified as NLP4SG

3. **data**
    - ``main_dataset.csv``: our primary results file, including the following information for each paper
        * Semantic Scholar Corpus ID
        * OpenAlex Work ID
        * Title
        * Venue Name
        * Publication Year
        * Venue Type (ACL, ACL-ADJACENT, or EXTERNAL)
        * Author Type (ACL or non-ACL)
        * NLP4SG Label (NLP4SG or Not NLP4SG)
    - ``GoogleScholar_venue_info.csv``: Google Scholar categories and subcategories for venues in ``main_dataset.csv``

# The Corpus Creation Process
1. S2ORC and Papers datasets are downloaded from Semantic Scholar
2. Files are sorted based on whether they are "ACL" (i.e. published at an ACL or associated venue) or "non-ACL"
3. Matching metadata for *all* extracted works (where possible) are found in OpenAlex
4. Author files are created based on the downloaded OpenAlex data

All work-related files (S2ORC, Papers, and OpenAlex) are stored together within sub_a (for ACL files) or sub_c (non-ACL). Works are grouped by the first four digits of their Semantic Scholar CorpusID, which are used to create a directory that itself contains all works whose CorpusIDs begin with those four digits. Each CorpusID gets its own directory within the appropriate grouping directory; and it is this directory that contains the associated S2ORC, Papers, and OpenAlex files for a given work. 

An example target ACL work with CorpusID 123499 would be stored as follows:

```
.
└── corpora_path/                      # see paths.py
    ├── authors/
    ├── datasets/
    ├── subcorpus_a/                   # ACL files
    │   └── 1234/                      # grouped CorpusIDs
    │       ├── 1234/
    │       ├── 123456/
    │       ├── 123412/
    │       └── 123499/                # target work's CorpusID
    │           ├── s2orc-123499.json  # the S2ORC file
    │           ├── 123499.json        # the Papers metadata file
    │           └── W999999.json       # the matched OpenAlex metadata file 
    └── subcorpus_c/
```

Author profiles are stored separately from their works, instead containing sets of their ACL and non-ACL works. 

# Recreating the Corpus
A user attempting to recreate the dataset from scratch will need access to:
-  ~TODO GB of storage space (for the smallest possible version); 
- A [Semantic Scholar API Key](https://www.semanticscholar.org/product/api)
- An [OpenAlex API Key](https://docs.openalex.org/how-to-use-the-api/rate-limits-and-authentication), for efficient API calls (Premium or Educator access required for the current version of the code)

We recommend the defaults provided for each function; the dataset is quite large, so users attempting to recreate the dataset should ensure that their computer (or allocation on a cluster) has adequate space to store files -- especially when non-destructive function parameters are set (i.e. when the JSONL files are not deleted post-extraction). 

A user with adequate space for JSONL and extracted S2ORC files will be advantaged by running multiple function calls simultaneously, either in multiple terminals or through their cluster's job system. Example Slurm scripts can be found in the [examples](examples/) folder of this repository.


An example recipe for a space-limited user might look as follows:  # TODO: maybe move this to the examples folder, too?

```python
# TODO: finish this example 

from create_subcorpora import * 

if __name__ == "__main__":
    download_s2orc(True, False, True)  # calls extract_from_s2orc; does not save full S2ORC work bodies (i.e. full paper texts, etc.); deletes JSONL files after extraction complete
    download_s2_papers(True, True)  # as above, but does make Papers metadata files contentful (as this is a critical step in building the corpus)
    get_openalex_info(get_ids_from_s2orc=False)  # matches works to their OpenAlex metadata, but does not attempt to utilize CorpusIDs only present in S2ORC files (since they were not saved)
    extract_authors()  # creates author profiles for all authors in OpenAlex metadata files
    csv_builder()  # TODO: decide if the typical user should make multiple and then merge_csv or not
```