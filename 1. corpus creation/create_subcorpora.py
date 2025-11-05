from paths import *
from credentials import headers, mailto

from os.path import exists
from os import mkdir, makedirs, remove, walk
import requests
from tqdm import tqdm 
from urllib.request import urlretrieve as urlretrieve
import glob
from gzip import open as gunzip
from shutil import copyfileobj
from cprint import cprint 
import ijson
import json
from re import sub as re_sub
from unicodedata import normalize
from ast import literal_eval
from hashlib import md5

def download_s2orc(call_extract: bool = False, extract_works: bool = True, delete_jsonls: bool = False):
    """Downloads and gunzips S2ORC JSONL files from Semantic Scholar.

    Parameters
    ----------
        call_extract (bool): whether to extract files from gunzipped JSONLs now, rather
                             than calling extract_from_s2orc later; significantly increases
                             function runtime, since extract_from_s2orc is particularly slow
                             when run in sequence
        extract_works (bool): see extract_from_s2orc
        delete_jsonls (bool): see extract_from_s2orc
    
    Returns
    ----------
        None
    """
    if not exists(s2orc_path): mkdir(s2orc_path)

    # while 'latest' was previously the release used, datasets after 2024-01-02 seem off-spec -- 
    # i.e. rather than there being 30 files, there are ~200-500. unclear what the cause of 
    # this is, but for now, we're relegating ourselves to using the older version, since 
    # this is presumably an issue on SemanticScholar's end
    s2orc = "https://api.semanticscholar.org/datasets/v1/release/2024-01-02/dataset/s2orc"
    db_files = requests.get(s2orc, headers=headers).json()["files"]
    
    with tqdm(range(len(db_files)), desc="Downloading S2ORC") as pbar:
        for i in pbar:
            s2orc_jsonl = f"{s2orc_path}/s2orc-{i}.jsonl"
            s2orc_gz = s2orc_jsonl + ".gz"
            if not (exists(s2orc_jsonl) or exists(s2orc_gz)):
                urlretrieve(db_files[i], s2orc_gz)
            
            pbar.update(1)

    with tqdm(glob.glob(f"{s2orc_path}/*.gz"), leave=False, desc="Gunzipping S2ORC") as pbar:
        for f in pbar:  
            pbar.set_description(f"Gunzipping {f}")         
            with gunzip(f, "rb") as f_in:
                with open(f[:-3], "wb") as f_out:
                    copyfileobj(f_in, f_out)

            pbar.set_description(f"Deleting {f}")
            remove(f)  # once .gz file is gunzipped, delete it
        
            if call_extract:
                pbar.set_description(f"Extracting {f}")
                s2orc_num = int(f.split("-")[-1].split(".")[0])
                extract_from_s2orc(s2orc_num, s2orc_num + 1, extract_works, delete_jsonls)
            
            tqdm.write(f"Finished {f}")
            pbar.update(1)


def download_s2_papers(call_extract: bool = False, delete_jsonls: bool = False):
    """Downloads and gunzips the Semantic Scholar 'Papers' JSONL files.

    Parameters
    ----------
        call_extract (bool): whether to extract metadata from gunzipped JSONLs now, rather than
                             calling extract_from_papers later; significantly increases function 
                             runtime
        delete_jsonls (bool): see extract_from_papers

    Returns
    ----------
        None
    """
    if not exists(s2_papers_db_path): mkdir(s2_papers_db_path)

    # L38
    s2_papers = "https://api.semanticscholar.org/datasets/v1/release/2024-01-02/dataset/papers"
    db_files = requests.get(s2_papers, headers=headers).json()["files"]
    for i in tqdm(range(len(db_files)), desc="Downloading Papers"): 
        papers_jsonl = f"{s2_papers_db_path}/papers-{i}.jsonl"
        papers_gz = papers_jsonl + ".gz"
        if not (exists(papers_jsonl) or exists(papers_gz)):
            urlretrieve(db_files[i], papers_gz)

    with tqdm(glob.glob(f"{s2_papers_db_path}/*.gz"), leave=False, desc="Gunzipping Papers") as pbar:
        for f in pbar:           
            with gunzip(f, "rb") as f_in:
                with open(f[:-3], "wb") as f_out:
                    copyfileobj(f_in, f_out)

            pbar.set_description(f"Deleting {f}")
            remove(f)  # once .gz file is gunzipped, delete it

            if call_extract:
                pbar.set_description(f"Extracting {f}")
                papers_num = int(f.split("-")[-1].split(".")[0])
                extract_from_papers(start=papers_num, end=papers_num + 1, delete_jsonls=delete_jsonls)

            tqdm.write(f"Finished {f}")
            pbar.update(1)


def extract_from_s2orc(start: int = 0, end: int = 30, extract_works: bool = True, delete_jsonls: bool = False):
    """Using the downloaded S2ORC dataset, extract individual paper JSON files and organize
    based on whether that paper was published at ACL.

    Files are stored in their own directories named for their CorpusID. These directories
    will, later, additionally contain a paper's metadata from the Papers dataset, and 
    from OpenAlex. Paper directories are stored, naively, within a directory named for their
    contents' first four CorpusID digits.
    
    This function additionally creates two txt files that store all ACL and non-ACL 
    CorpusIDs for future use.

    Parameters
    ----------
        start (int): which S2ORC JSONL file to start at (for job segmentation)
        end (int): which S2ORC JSONL file to end at
        extract_works (bool): whether to extract individual works from JSONL files; if False, 
                              still creates destination directories and grouping directories
        delete_jsonls (bool): whether to delete each JSONL file after extraction from it is complete
                              
    Returns
    ----------
        None
    """
    if not exists(s2orc_path): raise LookupError("path to S2ORC JSONL files is invalid")

    for dir in [sub_a, sub_c]:  # ACL and non-ACL directories
        if not exists(dir): mkdir(dir)

    s2orc_jsonls = tqdm(range(start, end))
    for i in s2orc_jsonls:
        curr_jsonl = f"{s2orc_path}/s2orc-{i}.jsonl"

        with open(curr_jsonl, encoding="utf-8") as f:
            with tqdm(total=366000, leave=False, desc=f"Looping through {curr_jsonl.split('/')[-1]}") as pbar:  # ~366k papers per JSONL
                for l in f:  # loop through every JSON in the JSONL
                    curr_corpusid = ""
                    curr_is_acl = False 

                    parser = ijson.parse(l)
                    for prefix, event, value in parser:
                        if prefix == "corpusid":  # store the CorpusID
                            curr_corpusid = str(value)
                            continue
                        elif prefix == "externalids.acl":  # check if ACL
                            curr_is_acl = True if value else False
                            break  # no further prefixes are (currently) relevant
                        else:
                            continue
                    
                    if not curr_corpusid.strip(): continue

                    # store files in subdirs named for the first four digits of a CorpusID
                    subdir_name = curr_corpusid[:4] 

                    subdir = f"{sub_a if curr_is_acl else sub_c}/{subdir_name}"
                    paper_dir = f"{subdir}/{curr_corpusid}"
                    s2orc_file = f"{paper_dir}/s2orc-{curr_corpusid}.json"

                    if not exists(s2orc_file):
                        makedirs(paper_dir, exist_ok=True)
                        
                        # if extract_works == False, create an *empty file* for all works
                        j = json.loads(l if extract_works else "{}")
                        with open(s2orc_file, 'w') as sf:
                            json.dump(j, sf, indent=4)
                        
                        pbar.set_description(f"Created file at {s2orc_file}")

                        with open(f"{datasets_path}/{'' if curr_is_acl else 'non_'}acl_corpusids.txt", 'a') as cf:
                            cf.write(f"{curr_corpusid}\n")

                    pbar.update(1)
        
        if delete_jsonls: remove(curr_jsonl)            


def extract_from_papers(batch_size: int = 5000, start: int = 0, end: int = 30, delete_jsonls: bool = False):
    """For each paper in the Papers database, create {corpusId}.json (in either the ACL or non-ACL
    directory, as appropriate) containing Semantic Scholar info (e.g. corpusId, externalIds, etc.).

    Parameters
    ----------
        batch_size (int): number of files to batch for writing
        start (int): which Papers JSONL file to start at (for job segmentation)
        end (int): which Papers JSONL file to end at
        delete_jsonls (bool): whether to delete each JSONL file after extraction from it is complete
    
    Returns
    ----------
        None
    """
    
    if not exists(s2_papers_db_path): raise LookupError("path to Papers JSONL files is invalid")

    acl_corpusids = set()
    other_corpusids = set()

    # load ACL and non-ACL CorpusID sets from files; see L134
    with open(f"{datasets_path}/acl_corpusids.txt") as f:
        for line in tqdm(f, total=80000):
            acl_corpusids.add(line.strip())
    
    with open(f"{datasets_path}/non_acl_corpusids.txt") as f:
        for line in tqdm(f, total=10000000):
            other_corpusids.add(line.strip())

    batch = {}  # from /path/to/make/file/at/{CorpusID}.json to paper metadata
    batched_is_acl = {}  # from /path/...{CorpusID.json} to is_acl (True or False)

    def write_batch():
        with tqdm(batch, leave=False) as batch_bar:
            for file_path in batch_bar:
                batch_bar.set_description(f'Writing file at {file_path}')
                with open(file_path, 'w') as f2:
                    json.dump(batch[file_path], f2, indent=4)

        batch.clear()
        batched_is_acl.clear() 

    papers_jsonls = tqdm(range(start, end))
    for i in papers_jsonls:
        curr_jsonl = f"{s2_papers_db_path}/papers-{i}.jsonl"

        with open(curr_jsonl, encoding="utf-8") as f:
            with tqdm(total=7300000, leave=False, desc=f"Looping through {curr_jsonl.split('/')[-1]}") as pbar:
                for l in f:  # loop through every JSON in the JSONL
                    curr_corpusid = ""
                    curr_is_acl = False 

                    parser = ijson.parse(l)
                    for prefix, event, value in parser: 
                        if prefix == "corpusid":
                            curr_corpusid = str(value)  # store the CorpusID
                        elif prefix == "externalids.ACL":
                            curr_is_acl = True if value else False
                            break
                        else: 
                            continue 
                    
                    if not curr_corpusid.strip(): continue  # missing CorpusID, somehow
                    elif not (curr_corpusid in acl_corpusids or curr_corpusid in other_corpusids):
                        # if the current CorpusID has not been seen previously, note it
                        with open(f"{datasets_path}/missing_from_s2orc.txt", "a") as f:
                            f.write(f"{curr_corpusid}\n")

                        # and add the CorpusID to the relevant file
                        with open(f"{datasets_path}/{'' if curr_is_acl else 'non_'}acl_corpusids.txt", 'a') as f:
                            f.write(f"{curr_corpusid}\n")

                    if curr_is_acl: acl_corpusids.discard(curr_corpusid)
                    else: other_corpusids.discard(curr_corpusid)

                    subdir_name = curr_corpusid[:4]

                    paper_dir = f"{sub_a if curr_is_acl else sub_c}/{subdir_name}/{curr_corpusid}"
                    paper_out = f"{paper_dir}/{curr_corpusid}.json"

                    if not exists(paper_out):
                        makedirs(paper_dir, exist_ok=True)  
                        
                        batched_is_acl[paper_out] = curr_is_acl
                        batch[paper_out] = json.loads(l)

                        pbar.set_description(f"Batched {paper_out}")
                    else:
                        pbar.set_description(f"Not batching {paper_out}")
                    
                    if len(batch) >= batch_size: write_batch()
                    pbar.update(1)
        
        write_batch()  # write out any remaining files (may be < batch_size)
        if delete_jsonls: remove(curr_jsonl)
    
    write_batch()


def process_title(title: str):
    """Normalizes and cleans the provided paper title.
    
    Parameters
    ----------
        title (str): a title
    
    Returns
    ----------
        str: the processed title
    """
    u = normalize("NFKC", title)
    l = u.lower()  # lowercase
    n = re_sub(r"\d+", " ", l)  # remove numbers
    p = re_sub(r"[^\w ]", " ", n)  # remove punctuation and non-space whitespace (e.g. tab, newline)
    s = re_sub(r" {2,}", " ", p) # remove multiple spaces (enforce single-spacing) 
    w = s.strip()  # remove extra head/tail whitespace 

    return w


def get_openalex_info(mailto: str = mailto, verbose: bool = False, start: int = 0, end: int = 10000,
                      get_ids_from_s2orc: bool = True):
    """Loop through every paper exctracted from S2ORC and/or Papers, matching it to its OpenAlex
    equivalent. Create a file W{OpenAlexID}.json for each, which contains the found OpenAlex 
    metadata.

    OpenAlex data is used to supplement any metadata that might be missing from the Papers
    database, and to help (somewhat) improve author disambiguation at a later step. 
    
    Parameters
    ----------
        mailto (str): the email associated with OpenAlex (if any; registering one increases query rate limit)
        verbose (bool): 
        start (int): the subdirectory to begin with (first four digits of CorpusID; for job segmentation)
        end (int): the subdirectory to end with
    
    Returns 
    ----------
        None 
    """
    # create files to track whether a given CorpusID has been found or failed in OpenAlex
    found_ids_filepath = f"{datasets_path}/openalex_found_{start}-{end}.txt"
    unfound_ids_filepath = f"{datasets_path}/openalex_unfound_{start}-{end}.txt"

    if not exists(found_ids_filepath):
        with open(found_ids_filepath, 'w') as f:
            found_ids = set()
    else:
        with open(found_ids_filepath) as f:
            found_ids = {line.strip() for line in f}

    if not exists(unfound_ids_filepath):
        with open(unfound_ids_filepath, 'w') as f:
            unfound_ids = set()
    else:
        with open(unfound_ids_filepath) as f:
            unfound_ids = {line.strip() for line in f}

    def write_unfound(unfound_corpus_id):  # add a new CorpusID to unfound_ids
        with open(unfound_ids_filepath, 'a') as f:
            f.write(f"{unfound_corpus_id}\n")
            unfound_ids.add(unfound_corpus_id)
        
        # create a blank file at unfound_corpus_id identifying that the paper could not be found in OpenAlex
        # TODO: this file might stick around even if, somehow, the paper is found via S2ORC stuff rather than Paper?
        #       somewhat unlikely edge case since S2ORC info should always = Papers, but possible
        paper_path = f"{sub_a if is_acl else sub_c}/{unfound_corpus_id[:4]}/{unfound_corpus_id}/NOT_IN_OPENALEX"
        with open(paper_path, 'w') as f: pass

    endpoint = "https://api.openalex.org/works"
    # "Where do you get your API key, you ask? For now, please just use an MD5 hash of your email address."
    api_key = md5(mailto.encode("utf-8")).hexdigest() 
    params = {"mailto": mailto, "api_key": api_key, "per-page": 100}
    
    batches = {"mag": {}, "doi": {}, "date": {}, "year": {}, "title": {}}
    batches_info = {}  # {CorpusID: {"mag": ..., "doi": ..., etc.}}; raw identifiers for batched CorpusIDs
    in_batches = {} # CorpusID: (identifier, exact_key_stored_in_batches)

    def get_batch(identifier: str, b: list): 
        """Get a batch of 50 results from OpenAlex.

        Parameters
        ----------
            identifier (str): which of MAG/DOI/etc. should be used to find papers in OpenAlex
            b (list): a list of MAG/DOI/etc.

        Returns 
        ----------
            None 
        """       
        results_dict = {}  # {CorpusID: result JSON}
        
        match identifier:
            case "mag" | "doi":
                filter_string = f"{identifier}:{'|'.join(b)}"  # find these 50 MAG/DOI
                params["filter"] = filter_string

                results = {}
                if verbose: tqdm.write("About to do requests.get...")
                while 'results' not in results:  # loop until successful query complete
                    if verbose: tqdm.write('Looping until batch results successfully queried')
                    try:
                        results = requests.get(endpoint, params=params)
                        if verbose: tqdm.write(f"{results.request.url}")
                        results = results.json()
                        if 'error' in results.keys(): tqdm.write(f"error in openalex results: {results['error']} \nmessage: {results['message']}")
                    except:
                        if verbose: 
                            tqdm.write(f'Trying requests.get again, id={identifier}')
                            tqdm.write(params["filter"])
                        continue
                
                results = results['results']
                if verbose: tqdm.write("done requests.get, gotten results")

                # some MAGs/DOIs cannot be found in OpenAlex; make sure to confirm which of these
                # an identifier match failure happened to
                item_info = {id.lower() for id in b}  # all MAGs/DOIs in this batch

                for r in results:
                    # MAG/DOI value that was retrieved; .lower() because DOIs aren't case-sensitive, 
                    # but exactly matching one requires sensitivity
                    curr_info = r["ids"][identifier][0 if identifier == "mag" else 16:].lower()  
                    if curr_info in item_info:  # if the found MAG/DOI matches a MAG/DOI that we searched
                        corpus_id = None 

                        for k in batches[identifier]:
                            if batches[identifier][k] == curr_info:
                                corpus_id = k  # CorpusID associated with the current MAG/DOI from results
                        
                        if not corpus_id: continue  # if match but no CorpusID, we already found it

                        # isACL is somewhat redundant because of Subcorpus A, but doesn't hurt to keep
                        update_dict = {"isACL": is_acl, "corpusId": corpus_id, 
                                       "foundVia": identifier}
                        r = {**update_dict, **r}  # append update_dict to the query results
                        results_dict[corpus_id] = r  # add the query results to results_dict

                        # on a success, remove CorpusID from various batch dicts
                        del batches[identifier][corpus_id]
                        del batches_info[corpus_id]
                        del in_batches[corpus_id]
            case "date" | "year" | "title":
                remove_from_batches = set()

                # unlike MAGs/DOIs, dates/years/titles require individual queries
                for corpus_id in tqdm(batches[identifier], desc=f"Making individual queries ({identifier})", leave=False):  
                    curr_info = batches[identifier][corpus_id]
                    params["filter"] = curr_info  # date/year/title acts as the filter string implicitly

                    results = {}
                    if verbose: tqdm.write("About to do requests.get...")

                    while 'results' not in results:
                        if verbose: tqdm.write('Starting a while loop')
                        try:
                            results = requests.get(endpoint, params=params)
                            if verbose: tqdm.write(f"{results.request.url}")
                            results = results.json()
                            if 'error' in results.keys(): tqdm.write(f"error in openalex results: {results['error']} \nmessage: {results['message']}")
                        except:
                            if verbose: 
                                tqdm.write(f'Trying requests.get again, id={identifier}')
                                tqdm.write(params["filter"])
                            continue
                    
                    # first result *should* almost always be what we want, but sometimes isn't; confirm correct paper
                    results = results["results"]
                    if verbose: tqdm.write("done requests.get, gotten results")
                    
                    if results:
                        for r in results:
                            result_title = process_title(r["title"])
                            orig_title = process_title(batches_info[corpus_id]["title"])
                            
                            if result_title == orig_title:
                                update_dict = {"isACL": is_acl, "corpusId": corpus_id,
                                               "foundVia": identifier}
                                r = {**update_dict, **r}
                                results_dict[corpus_id] = r

                                # remove from batches after loop finishes to avoid error
                                remove_from_batches.add(corpus_id)
                                
                                del batches_info[corpus_id]
                                del in_batches[corpus_id]
                                break  # when a match has been made, break

                for remove in remove_from_batches: 
                    del batches[identifier][remove]

        result_items = tqdm(results_dict.items(), desc=f"Writing OpenAlex files ({identifier})", leave=False)
        for corpus_id, r in result_items:
            openalex_id = r["id"][21:]
            paper_path = f"{sub_a if r['isACL'] else sub_c }/{corpus_id[:4]}/{corpus_id}/{openalex_id}.json"
            
            with open(paper_path, "w") as f:
                json.dump(r, f, indent=4)
            
            with open(found_ids_filepath, 'a') as f:
                f.write(f"{corpus_id}\n")
                found_ids.add(corpus_id)

        # those identifiers still in in_batches failed to be found in OpenAlex
        failed_for_identifier = {c for c in set(in_batches.keys()) if in_batches[c][0] == identifier}

        for unfound_corpus_id in failed_for_identifier:
            item_info = batches_info[unfound_corpus_id]
            del batches[identifier][unfound_corpus_id]  # remove item from batch type it failed
            del in_batches[unfound_corpus_id]  # then remove from in_batches, since we'll re-add shortly

            batches_info[unfound_corpus_id][identifier] = None  # make the current identifier type unusable for future queries

            for key in item_info.keys():  # "mag", "doi", etc.; earlier key = higher priority
                curr_info = batches_info[unfound_corpus_id][key]  # value for given identifier; something or None
                if curr_info:  # if the current key (identifier) has a value (i.e. is valid)
                    match key:
                        case "mag":  # prefer and use MAG when possible
                            batches[key][unfound_corpus_id] = curr_info 
                            in_batches[unfound_corpus_id] = (key, curr_info) 
                            break
                        case "doi":  # next best identifier is DOI (.lower(), since DOI isn't case sensitive)
                            curr_info = curr_info.lower()
                            batches[key][unfound_corpus_id] = curr_info 
                            in_batches[unfound_corpus_id] = (key, curr_info) 
                            break
                        case "title" | "date" | "year":  # must create filter string for batch
                            title = batches_info[unfound_corpus_id]["title"]
                            if not title:  # without a title, it's impossible to find the correct paper
                                del batches_info[unfound_corpus_id]
                                write_unfound(unfound_corpus_id)
                                break
                            filter_string = f"""title.search:{re_sub(r'[,:!"]', ' ', title.lower())}"""
                            
                            if key != "title":  # try title + date/year, before trying only the title
                                filter_string += f",publication_{key}:{curr_info}"  #  ,date/year:filter_string
                            
                            batches[key][unfound_corpus_id] = filter_string
                            in_batches[unfound_corpus_id] = (key, filter_string)
                            break
                elif key == 'title':  # if curr_info is invalid and we're already on the title, we've failed to find a match
                    del batches_info[unfound_corpus_id]
                    write_unfound(unfound_corpus_id)
                    break
                    
                       
    def check_batch(identifier: str, bypass: bool = False, verbose: bool = True):
        """If there are >= 50 items batched for the given identifier, build up the batch list 
        and call get_batch.

        Parameters
        ----------
            identifier (str): which of MAG/DOI/etc. should be used to find papers in OpenAlex
            bypass (bool): whether to do_batch (i.e. get_batch) for any number of queued identifiers, 
                           rather than waiting for there to be >= 50
            verbose (bool): whether to provide verbose details about the number of each identifier, 
                            total batched across identifiers (which should be the same as the # in
                            in_batches and batches_info)
        Returns
        ----------
            None
        """
        batch = []            

        def do_batch():  # build a batch, then get_batch()
            nmag = len(batches["mag"])
            ndoi = len(batches["doi"])
            ndate = len(batches["date"])
            nyear = len(batches["year"])
            ntitle = len(batches["title"])
            ntotal = nmag + ndoi + ndate + nyear + ntitle

            if verbose:
                tqdm.write(f"# MAG:   {nmag} \n# DOI:   {ndoi} \n# date:  {ndate} \n# year:  {nyear}\n" +
                        f"# title: {ntitle} \nTOTAL {ntotal} =? IN_BATCHES {len(in_batches)} =? " +
                        f"BATCHES_INFO {len(batches_info)}\n")
                # tqdm.write(f"{batches}")
            
            # add 50 of the given identifier to its batch
            for key in batches[identifier].values():
                if len(batch) >= 50:
                    break 
                else:
                    batch.append(key)

            get_batch(identifier, batch)
            batch.clear()

        if bypass:    
            while len(batches[identifier]) > 0: 
                do_batch()
        elif len(batches[identifier]) >= 50:
            if verbose: 
                tqdm.write(f"IDENTIFIER {identifier} >= 50!!!")
            
            while len(batches[identifier]) >= 50: 
                do_batch()
            
            if verbose:
                tqdm.write(f"FINISHED {identifier} (len = {len(batches[identifier])})" + 
                        "\n--------------------\n\n")
            
        match identifier:  # if MAG was just completed, check if DOI ready to go; etc.
            case "mag": check_batch("doi", verbose=verbose)
            case "doi": check_batch("date", verbose=verbose)
            case "date": check_batch("year", verbose=verbose)
            case "year": check_batch("title", verbose=verbose)


    def add_to_batches(corpus_id: str, paper_ids: dict):
        """
        Parameters 
        ----------
            corpus_id (str): the CorpusID of the paper to add to batches
            paper_ids (dict): the available identifiers and their values for the paper
        
        Returns 
        ----------
            None
        """
        batches_info[corpus_id] = paper_ids
        
        # to add IDs to their batch, first check to see if the ID is already in batches_info
        if batches_info[corpus_id]["mag"]:
            curr_mag = batches_info[corpus_id]["mag"]
            
            batches["mag"][corpus_id] = curr_mag
            in_batches[corpus_id] = ("mag", curr_mag)
            
            check_batch("mag", verbose=verbose)
        elif batches_info[corpus_id]["doi"]:
            curr_doi = batches_info[corpus_id]["doi"].lower()  # see L451
            
            batches["doi"][corpus_id] = curr_doi
            in_batches[corpus_id] = ("doi", curr_doi)
            
            check_batch("doi", verbose=verbose)
        elif not batches_info[corpus_id]['title']:  # if no title, and no MAG/DOI, failure
            del batches_info[corpus_id]
            if corpus_id in in_batches: 
                del in_batches[corpus_id]
        else:  # build the paper's filter string from the title, at minimum
            filter_string = f"""title.search:{re_sub(r'[,:!"]', ' ', batches_info[corpus_id]['title'].lower())}"""
            
            if batches_info[corpus_id]["date"]:
                curr_date = batches_info[corpus_id]["date"]
                filter_string += f",publication_date:{curr_date}"
                
                batches["date"][corpus_id] = filter_string
                in_batches[corpus_id] = ("date", filter_string)

                check_batch("date", verbose=verbose)
            elif batches_info[corpus_id]["year"]:
                curr_year = batches_info[corpus_id]["year"]
                filter_string += f",publication_year:{curr_year}"
                
                batches["year"][corpus_id] = filter_string
                in_batches[corpus_id] = ("year", filter_string)
                
                check_batch("year", verbose=verbose)
            else:
                batches["title"][corpus_id] = filter_string
                in_batches[corpus_id] = ("title", filter_string)

                check_batch("title", verbose=verbose)

    pbar = tqdm(total=int(11000000/(10000/(end-start))), desc="Looping through papers")

    for subcorpus in [sub_a, sub_c]:
        is_acl = subcorpus == sub_a 

        subdirs = tqdm([str(x) for x in range(start, end)], leave=False)
        for subdir in subdirs:
            subdirs.set_description(f'Looping through {subcorpus}/{subdir}')
            papers = glob.iglob(f"{subcorpus}/{subdir}/*/*.json")
            
            for paper in papers:
                # normalize paper path, i.e. replace \ with / 
                paper = paper.replace("\\", "/")

                # for now, only work with Papers papers (i.e. skip OpenAlex and S2ORC papers)
                if "W" in paper or "s2orc" in paper: 
                    pbar.update(1)
                    continue  
                
                # if the OpenAlex data has already been found/failed, skip
                curr_corpusid = paper.split("/")[-2] 

                if curr_corpusid in found_ids or curr_corpusid in unfound_ids:
                    pbar.update(1)
                    continue                
                
                paper_ids = {}
                with open(paper) as f:
                    parser = ijson.parse(f)
                    for prefix, event, value in parser:
                        match prefix:
                            case "externalids.MAG": paper_ids["mag"] = value 
                            case "externalids.DOI": paper_ids["doi"] = value.lower().split(',')[0] if value else value
                            case "publicationdate": paper_ids["date"] = value
                            case "year": paper_ids["year"] = value
                            case "title": paper_ids["title"] = value
                            case "journal": break  # the first key after all currently relevant info
                    
                    if paper_ids["doi"]:  # format all DOIs
                        paper_ids['doi'] = re_sub(r'[^\w\.\/\(\)]', '', paper_ids['doi'])

                add_to_batches(curr_corpusid, paper_ids)
                pbar.update(1)

            if get_ids_from_s2orc:
                # next, loop through S2ORC papers that didn't have a matching entry in Papers
                s2orc_papers = glob.iglob(f"{subcorpus}/{subdir}/*/s2orc-*.json")
                for paper in s2orc_papers:
                    paper = paper.replace("\\", "/")
                    curr_corpusid = paper.split("/")[-2]

                    if curr_corpusid in found_ids or curr_corpusid in unfound_ids:
                        pbar.update(1)
                        continue

                    paper_ids = {}
                    with open(paper) as f:  # get paper_ids from full extracted S2ORC files
                        j = json.load(f)
                        if j and j['externalids']:
                            if j['externalids']['mag']:
                                paper_ids['mag'] = j['externalids']['mag']
                            if j['externalids']['doi']:
                                paper_ids['doi'] = j['externalids']['doi']
                        if j and j['content'] and j['content']['annotations'] and j['content']['annotations']['title']:
                            curr_title_index = literal_eval(j['content']['annotations']['title'])
                            if j['content']['text']:
                                paper_ids['title'] = j['content']['text'][int(curr_title_index[0]['start']):int(curr_title_index[0]['end'])]
                                if len(paper_ids['title']) > 500:
                                    paper_ids['title'] = None
                        
                        for id in ['mag', 'doi', 'date', 'year', 'title']:
                            if id not in paper_ids:
                                paper_ids[id] = None

                        if paper_ids["doi"]:  # format all DOIs
                            paper_ids['doi'] = re_sub(r'[^\w\.\/\(\)]', '', paper_ids['doi'])
                        

                    add_to_batches(curr_corpusid, paper_ids)
                    pbar.update(1)
        
        # to avoid mixing ACL and non-ACL together in a batch, make sure to bypass to empty all current batches out
        check_batch("mag", True, verbose=verbose)
        check_batch("doi", True, verbose=verbose)
        check_batch("date", True, verbose=verbose)
        check_batch("year", True, verbose=verbose)
        check_batch("title", True, verbose=verbose)

        cprint(f"Finished {start}-{end} for {subcorpus}", c="c")
    
    pbar.close()
    cprint(f"Finished {start}-{end} for both Subcorpus A and Subcorpus C", c="g")


def extract_authors():
    """Create an author file for every author present in OpenAlex files. Each author file contains the OpenAlex ID 
    of ACL and non-ACL paper that they have written.
        
    Parameters
    ----------
        None
    
    Returns
    ----------
        None
    """
    # track the CorpusID of every paper whose authors have been extracted (i.e. the full author file might not 
    # be complete, but the specific paper doesn't need to be looked at again)
    seen_papers_filepath =  f"{datasets_path}/seen_papers_for_author_extract.txt"
    if not exists(seen_papers_filepath):
        with open(seen_papers_filepath, "w") as f:  # make the file if it doesn't exist
            seen_papers = set()
    else:
         with open(seen_papers_filepath) as f:
            seen_papers = {l.strip() for l in f}

    makedirs(authors_path, exist_ok=True)
    
    papers = glob.iglob(f"{corpora_path}/subcorpus_*/*/*/W*.json")  
    for paper in tqdm(papers, total=11000000, desc="Extracting authors from all papers"):
        paper = paper.replace("\\", "/")
        paper_split = paper.split('/')
        corpus_id = paper_split[-2]

        if corpus_id in seen_papers:  continue  # don't duplicate author contribs!

        paper_is_acl = False if paper_split[-4] == 'subcorpus_c' else True
        authors = []

        # extract all author IDs from the OpenAlex file
        with open(paper) as f:
            parser = ijson.parse(f)
            for prefix, event, value in parser:
                if prefix == 'authorships.item.author.id':
                    authors.append(value.split('/')[-1])
                if prefix == 'countries_distinct_count':
                    break
        
        for author_id in authors:
            author_subdir = author_id[1:5]  # like with CorpusIDs, group authors by the first four *digits* of their ID
            author_path = f"{authors_path}/{author_subdir}"
            author_file = f"{author_path}/{author_id}.json"
            
            makedirs(author_path, exist_ok=True)

            try: # if an author has been extracted previously, we should append/modify their file
                with open(author_file, "r") as f: 
                    author_dict = json.load(f)
            except FileNotFoundError:
                author_dict = {"acl_papers": [], "non_acl_papers": []}
            
            # cast to sets for extra insurance against duplicate papers
            acl_papers = set(author_dict["acl_papers"])  
            non_acl_papers = set(author_dict["non_acl_papers"])

            if paper_is_acl: acl_papers.add(corpus_id)
            else: non_acl_papers.add(corpus_id)

            # then cast back into lists, which are serializable
            author_dict["acl_papers"] = list(acl_papers)  
            author_dict["non_acl_papers"] = list(non_acl_papers)

            with open(author_file, "w") as f: 
                json.dump(author_dict, f, indent=4)

        seen_papers.add(corpus_id)
        with open(seen_papers_filepath, "a") as f:
            f.write(f"{corpus_id}\n")
    

def write_openalex_filepaths():
    openalex_paths = f"{datasets_path}/openalex_paths.txt"

    with open(openalex_paths, "a") as f:
        for sub in [sub_a, sub_c]:
            for dir, _, files in walk(sub): 
                for filename in files: 
                    if filename[0] == "W":
                        f.write(dir.replace("\\", "/") + "/" + filename + "\n")



    # oa_paths = get_file_paths_multiprocessing(paper_dirs)
    # print('Got all paths to OpenAlex works')
    # with open(f"{datasets_path}/openalex_paths.txt", 'a') as f:
    #     for path in oa_paths:
    #         f.write(path + '\n')

### ----------
### MULTIPROCESSING CODE BELOW THIS POINT: EXPERIMENTAL, BUT PREVIOUSLY FUNCTIONAL
### ----------

# Functions must be top-level for multiprocessing
def process_file(path):
    path_split = path.split('/')
    if path_split[-1].startswith('W'):
        is_acl = True if path_split[-4] == 'subcorpus_a' else False
        corpusid = path_split[-2]
        with open(path) as f:
            author_ids = extract_author_ids(f)
            return [(author_id, corpusid, is_acl) for author_id in author_ids]
        
def extract_author_ids(f):
    authors = []
    parser = ijson.parse(f)
    for prefix, event, value in parser:
        if prefix == 'authorships.item.author.id':
            authors.append(value.split('/')[-1])
        if prefix == 'countries_distinct_count':
            break
    return authors

def extract_authors_2():
    from multiprocessing import Pool
            
    def load_openalex_paths(subcorpus):
        with open(f"{datasets_path}/openalex_paths.txt") as f:
            oa_paths = {l.strip() for l in tqdm(f, desc='Loading oa paths')}
            return [x for x in tqdm(oa_paths, desc='filtering by subcorpus') if subcorpus in x]
    
    def update_authors_dict(authors_dict, author_info):
        author_id, corpusid, is_acl = author_info
        if author_id not in authors_dict:
            authors_dict[author_id] = {'acl_papers': [], 'non_acl_papers': []}
        if is_acl:
            authors_dict[author_id]['acl_papers'].append(corpusid)
        else:
            authors_dict[author_id]['non_acl_papers'].append(corpusid)
    
    def process_files(subcorpus, authors_dict):
        paths = load_openalex_paths(subcorpus)
        print('Finished collecting paths')
        print('')
        with Pool() as pool:
            results = pool.map(process_file, paths)
            for result in tqdm(results, desc='updating authors_dict with pool results'):
                for author_info in result:
                    update_authors_dict(authors_dict, author_info)
        print(f"finished processing {subcorpus}")

    def write_json_files(authors_dict):
        authors_dir = f"{corpora_path}/authors2"
        makedirs(authors_dir, exist_ok=True)
        for author_id, papers in tqdm(authors_dict.items(), desc = 'writing json files'):
            subdir = author_id[:5]
            subdir_path = f"{authors_dir}/{subdir}"
            makedirs(subdir_path, exist_ok=True)
            with open(f"{subdir_path}/{author_id}.json", "w") as f:
                json.dump(papers, f, indent=4)
    
    def remove_dupes(authors_dict):
        for author in tqdm(authors_dict, desc='Removing dupes'):
            authors_dict[author]['acl_papers'] = list(set(authors_dict[author]['acl_papers']))
            authors_dict[author]['non_acl_papers'] = list(set(authors_dict[author]['non_acl_papers']))

    authors_dict = {}
    process_files('subcorpus_a', authors_dict)
    process_files('subcorpus_c', authors_dict)
    remove_dupes(authors_dict)
    write_json_files(authors_dict)

if __name__ == "__main__":
    pass