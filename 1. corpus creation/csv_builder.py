from paths import *

from os.path import exists
from os import makedirs, listdir
import pandas as pd
from tqdm import tqdm 
import glob
import ijson
import json
from multiprocessing import Pool
from ast import literal_eval

csv_columns = ["title", "corpus_id", "openalex_id", "author_ids", "venue", "is_acl", "is_nlp", 
               "max_acl_contribs", "openalex_path", "s2orc_path"]

# OpenAlex concepts that we consider indicative of "NLP" content
concepts = {'C204321447', 'C41895202', 'C23123220', 'C203005215', 'C119857082', 
            'C186644900', 'C28490314', 'C2777530160', 'C137293760'}

def process_author_file(path):
    """Given the path to an author's file, extract and return its data as a dictionary.

    Parameters
    ----------
        path (str): path to an OpenAlex author file (see: extract_authors()) 
    
    Returns
    ----------
        dict: JSONKey:JSONvalue
    """
    path = path.replace("\\", "/")
    
    with open(path) as f:
        j = json.load(f)
    
    id_string = path.split('/')[-1].split('.')[0]
    return {'AuthorID': int(id_string[1:]),
            'acl_papers': [int(id) for id in j['acl_papers']],
            'non_acl_papers': [int(id) for id in j['non_acl_papers']]}


def make_author_csv():
    """Collate OpenAlex author info into a single CSV.

    Parameters 
    ----------
        None 

    Returns 
    ----------
        None
    """
    author_columns = ['AuthorID', 'acl_papers', 'non_acl_papers']

    author_files = glob.glob(f"{authors_path}/*/*.json")
    tqdm.write('Collected author paths')

    with Pool() as pool:
        results = pool.map(process_author_file, author_files)
    
    df = pd.DataFrame(results, columns=author_columns)
    df.to_csv(f"{csvs_path}/authors.csv")


def csv_builder(threshold: float = 0.0, start: int = 0, end: int = 10000, batch_size: int = 1000):
    """Navigate through each OpenAlex metadata JSON file, extracting key information and appending to a 
    master data CSV. Utilize authors.csv to determine which author has the most ACL contributions, adding
    this information to the CSV as well.
        
    Parameters
    ----------
        threshold (float): OpenAlex concept similarity threshold, to be considered an "NLP" paper
        start (int): the subdirectory to begin with (first four digits of CorpusID; for job segmentation)
        end (int): the subdirectory to end with
        batch_size (int): the number of works to add to the dataframe at once (rather than one at a time)
    
    Returns
    ----------
        None
    """
    if threshold < 0 or threshold > 1: raise ValueError(f"threshold (= {threshold}) must be between 0.0 and 1.0")
    if not exists(csvs_path): makedirs(csvs_path)

    df = pd.DataFrame(columns=csv_columns)

    tqdm.write('Loading authors CSV...')
    author_df = pd.read_csv(f"{csvs_path}/authors.csv", usecols=['AuthorID', 'acl_papers', 'non_acl_papers'],      
                            index_col='AuthorID')
    author_df.sort_index(inplace=True)
    tqdm.write('Loaded authors CSV!')

    subdirs = tqdm([str(x) for x in range(start, end)], leave=False)
    
    with open(f"{datasets_path}/openalex_paths.txt") as f:
        openalex_paths = {l.strip() for l in tqdm(f, desc='loading openalex paths')}
        openalex_works_dict = {}

        for path in tqdm(openalex_paths, desc='sorting openalex paths'):
            subdir = path.split('/')[-3]  # four-digit CorpusID substring
            
            if subdir in openalex_works_dict:
                openalex_works_dict[subdir].append(path)
            else:
                openalex_works_dict[subdir] = [path]
        
        del openalex_paths  # remove file from memory

    for subdir in subdirs:
        subdirs.set_description(f"Looping through {subdir}/ in all subcorpora")
        batch = []
        
        num_works = 0  # display total number of works to be processed for the subdir

        if exists(f"{sub_a}/{subdir}"): num_works += len(listdir(f"{sub_a}/{subdir}/"))
        elif exists(f"{sub_c}/{subdir}"): num_works += len(listdir(f"{sub_c}/{subdir}/"))
        else: continue  # if subdir doesn't exist, skip it

        if subdir not in openalex_works_dict: continue  # if subdir doesn't exist, skip it
        
        works = openalex_works_dict[subdir]  # OpenAlex files present for subdir in either sub_a or sub_c
        pbar = tqdm(total=num_works, leave=False)

        for work in works:
            pbar.set_description(f"Extracting from {work}")
            work_row = {"openalex_path": work, "openalex_id": work.split("/")[-1].split(".")[0],
                        "author_ids": [], "max_acl_contribs": 0, "is_nlp": False}
            
            with open(work) as w:
                parser = ijson.parse(w)
                last_concept = None
                
                for prefix, event, value in parser:
                    match prefix:
                        case "isACL": 
                            work_row["is_acl"] = value  # set during get_openalex_info()
                        case "corpusId": 
                            work_row["corpus_id"] = value  # CorpusID
                            work_row["s2orc_path"] = "/".join(work.split("/")[:-1]) + f"/s2orc-{value}.json"  # path to associated s2orc file
                        case "title": 
                            work_row["title"] = value
                        case "primary_location.source.display_name":  # venue where work was published
                            work_row["venue"] = value
                        case "authorships.item.author.id":  # add each author from the paper to author_ids
                            author_id = value.split("/")[-1]
                            work_row["author_ids"].append(author_id)

                            try: 
                                acl_contribs = len(literal_eval(author_df.loc[int(author_id[1:])]['acl_papers']))
                            except KeyError:  # if, somehow, the author was not put in the author_df
                                acl_contribs = -1

                            # store number of ACL contribs from the author who has most contributed to ACL
                            work_row["max_acl_contribs"] = max(work_row["max_acl_contribs"], acl_contribs)  
                        case "concepts.item.id": 
                            if not work_row["is_nlp"]:  # if work is not yet considered NLP, continue checking concepts
                                last_concept = value.split("/")[-1]
                        case "concepts.item.score":  # if work is not yet NLP, check if the current concept is NLP and above the "NLP" threshold
                            if not work_row["is_nlp"] and last_concept in concepts and value > threshold: 
                                work_row["is_nlp"] = True
                        case "locations_count":
                            break
            
            pbar.update(1)
            batch.append(work_row)

            if len(batch) >= batch_size:
                df = pd.concat([df, pd.DataFrame(batch)], ignore_index=True)
                batch = []

        df = pd.concat([df, pd.DataFrame(batch)], ignore_index=True)  # update df with all works from current subdir

    if start > 0 or end < 10000:
        df.to_csv(f"{csvs_path}/papers_subcsv_{start}-{end}.csv")
    else:  # if no custom range was set, store df in a single, complete CSV
        df.to_csv(f"{csvs_path}/papers.csv")


def merge_csvs():
    """Combine any/all papers_subcsv_{start}-{end}.csv CSVs into a single CSV. Used to assemble 
    results of job-based csb_builder() calls.

    Parameters
    ----------
        None 
    
    Returns
    ----------
        None
    """
    merged_csv = pd.DataFrame(columns=csv_columns)
    
    # combine all subcsvs into one single, complete CSV
    for csv in glob.iglob(f"{csvs_path}/papers_sub*"):
        df = pd.read_csv(csv)
        merged_csv = pd.concat([merged_csv, df], ignore_index=True)

    merged_csv.to_csv(f"{csvs_path}/papers_merged.csv")


if __name__ == "__main__":
    pass