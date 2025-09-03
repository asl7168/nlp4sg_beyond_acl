import requests
from credentials import headers
from tqdm import tqdm
import urllib.request
import glob
from gzip import open as gunzip
from shutil import copyfileobj
import json
import jsonlines

s2_path = "/projects/b1170/corpora/semantic_scholar"


def s2_db_download(dataset: str, dest: str):
    """Download a full Semantic Scholar database (of .jsonl.gz files)
    """
    dataset_root = "https://api.semanticscholar.org/datasets/v1/release/latest/dataset"
    db_files = requests.get(f"{dataset_root}/{dataset}", headers=headers).json()["files"]
    for i in tqdm(range(len(db_files))):
        urllib.request.urlretrieve(db_files[i], f"{dest}/{dataset}-{i}.jsonl.gz")


def s2_db_gunzip(dest: str):
    """Gunzip all .jsonl.gz files to .jsonl
    """
    for f in tqdm(glob.glob(f"{dest}/*.gz")):           
        with gunzip(f, "rb") as f_in:
            with open(f[:-3], "wb") as f_out:
                copyfileobj(f_in, f_out)


def s2orc_download(dest: str = "/projects/b1170/corpora/semantic_scholar/s2orc"):
    """Download the full s2orc database
    """
    s2_db_download("s2orc", dest)


def papers_download(dest: str = "/projects/b1170/corpora/semantic_scholar/papers"):
    """Download the full Semantic Scholar papers databse
    """
    s2_db_download("papers", dest)


def s2orc_gunzip(dest: str = "/projects/b1170/corpora/semantic_scholar/s2orc"):
    """Gunzip all .jsonl.gz files to .jsonl
    """
    s2_db_gunzip(dest)


def papers_gunzip(dest: str = "/projects/b1170/corpora/semantic_scholar/papers"):
    """Gunzip all .jsonl.gz files to .jsonl
    """
    s2_db_gunzip(dest)


def papers_decompose():
    """Break all of papers .jsonl files into individual .json files
    DO NOT USE THIS JUST LIMIT SEMANTIC SCHOLAR QUERY FIELDS
    """
    for i in tqdm(range(30)):
        jsonl_file = f"{s2_path}/papers/papers-{i}.jsonl"

        with jsonlines.open(jsonl_file) as reader:
            for j in tqdm(reader):
                with open(f"{s2_path}/papers/{j['corpusid']}.json", "w") as f:
                    json.dump(j, f, indent=4)


if __name__ == "__main__":
    # s2orc_download()
    # s2orc_gunzip()
    # papers_download()
    # papers_gunzip()
    # papers_decompose()
    print()
