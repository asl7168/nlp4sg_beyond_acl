from credentials import headers
from os.path import exists, isdir, isfile
from os import mkdir, makedirs, replace, listdir, rename
from os import remove as os_remove
import requests
from tqdm import tqdm 
from urllib.request import urlretrieve as urlretrieve
import glob
from gzip import open as gunzip
from shutil import copyfileobj
from cprint import cprint 
import ijson
import json
import jsonlines
from re import compile, sub as re_sub
from unicodedata import normalize
from math import ceil
from ast import literal_eval

# NOTE: while functions provide the option to provide custom filepaths, we recommend changing
# ONLY corpora_path -- all other predefinted paths rely on it, and custom filepaths are untested

corpora_path = "/projects/b1170/corpora/comp_ling_meta"
datasets_path = f"{corpora_path}/datasets"
s2orc_path = f"{datasets_path}/s2orc"
s2_papers_db_path = f"{datasets_path}/s2_papers"

sub_a = f"{corpora_path}/subcorpus_a"
# sub_a_s2 = f"{sub_a}/s2"
# sub_a_oa = f"{sub_a}/open_alex"

sub_b = f"{corpora_path}/subcorpus_b"
# sub_b_s2 = f"{sub_b}/s2"
# sub_b_oa = f"{sub_b}/open_alex"

sub_c = f"{corpora_path}/subcorpus_c"
# sub_c_s2 = f"{sub_c}/s2"
# sub_c_oa = f"{sub_c}/open_alex"

# papers_path = f"{s2_path}/papers"
# relevant_path = f"{papers_path}/relevant"
# acl_papers_path = f"{s2_path}/papers/acl"
# acl_authors_path = f"{acl_papers_path}/authors"
# over_three_path = f"{acl_authors_path}/over_three"
# acl_s2_path = f"{acl_papers_path}/s2"
# acl_openalex_path = f"{acl_papers_path}/open_alex"

def download_s2orc(s2orc_dst: str = s2orc_path):
    """Downloads and gunzips s2orc from Semantic Scholar

    Parameters
    ----------
        s2orc_dst (str): path in which to save s2orc files
    
    Returns
    ----------
        None
    """
    if not exists(s2orc_dst): mkdir(s2orc_dst)

    s2orc = "https://api.semanticscholar.org/datasets/v1/release/latest/dataset/s2orc"
    db_files = requests.get(s2orc, headers=headers).json()["files"]
    for i in tqdm(range(len(db_files)), desc="Downloading s2orc"):
        urlretrieve(db_files[i], f"{s2orc_dst}/s2orc-{i}.jsonl.gz")

    for f in tqdm(glob.glob(f"{s2orc_dst}/*.gz"), desc="gunzipping"):           
        with gunzip(f, "rb") as f_in:
            with open(f[:-3], "wb") as f_out:
                copyfileobj(f_in, f_out)


def download_s2_papers(s2_papers_dst: str = s2_papers_db_path):
    """Downloads and gunzips s2orc from Semantic Scholar

    Parameters
    ----------
        s2orc_dst (str): path in which to save s2orc files
    
    Returns
    ----------
        None
    """
    if not exists(s2_papers_dst): mkdir(s2_papers_dst)

    s2_papers = "https://api.semanticscholar.org/datasets/v1/release/latest/dataset/papers"
    db_files = requests.get(s2_papers, headers=headers).json()["files"]
    for i in tqdm(range(len(db_files)), desc="Downloading Papers"): 
        if not exists(f"{s2_papers_dst}/papers-{i}.jsonl.gz"):
            urlretrieve(db_files[i], f"{s2_papers_dst}/papers-{i}.jsonl.gz")

    for f in tqdm(glob.glob(f"{s2_papers_dst}/*.gz"), desc="gunzipping"):           
        with gunzip(f, "rb") as f_in:
            with open(f[:-3], "wb") as f_out:
                copyfileobj(f_in, f_out)


def extract_from_s2orc(only_acl: bool = False, s2orc_path: str = s2orc_path, acl_dst: str = sub_a, 
                       other_dst: str = sub_c, corpusids_dst: str = datasets_path, start: int = 0,
                       end: int = 30):
    """Find all papers in s2orc, creating subdirectories for future organization; and storing ACL 
    corpusIds in one file and non-ACL corpusIDs in another

    Parameters
    ----------
        only_acl (bool): whether to extract only ACL files from s2orc
        s2orc_path (str): path to s2orc JSONL downloads
        acl_dst (str): path where ACL organization subdirs will be made
        other_dst (str): path where non-ACL organization subdirs will be made
        corpusids_dst (str): where to store files containing ACL and non-ACL corpusIds
        start (int): which s2orc file to start at (for job segmentation)
        end (int): which s2orc file to end at
    
    Returns
    ----------
        None
    """
    if not exists(s2orc_path): raise LookupError("path to extracted s2orc JSONL files is invalid")

    for dir in [acl_dst, other_dst]:
        if not exists(dir): mkdir(dir)

    s2orc_jsonls = tqdm(range(start, end))
    for i in s2orc_jsonls:
        curr_jsonl = f"{s2orc_path}/s2orc-{i}.jsonl"

        with open(curr_jsonl) as f:
            with tqdm(total=366000) as pbar:  # ~366k papers per JSONL
                while True:
                    curr_corpusid = ""
                    curr_is_acl = False 

                    l = f.readline()
                    if not l: break

                    parser = ijson.parse(l)
                    for prefix, event, value in parser:
                        if prefix == "corpusid":  # store the corpusid
                            curr_corpusid = str(value)
                            continue
                        elif prefix == "externalids.acl":  # check if ACL
                            curr_is_acl = True if value else False
                            break  # don't care about anything after this, so break
                        else:
                            continue
                    
                    # to improve future efficiency, store s2(orc) files in subdirs named for 
                    # the first four digits of a corpusId
                    subdir_name = curr_corpusid[:4] 

                    if curr_is_acl:
                        subdir = f"{acl_dst}/{subdir_name}"
                        paper_dir = f"{subdir}/{curr_corpusid}"
                        s2orc_file = f"{paper_dir}/s2orc-{curr_corpusid}.json"
                        if not exists(s2orc_file):
                            makedirs(paper_dir, exist_ok=True)
                            j = json.loads(l)
                            with open(s2orc_file, 'w') as sf:
                                json.dump(j, sf, indent=4)
                            tqdm.write(f"created file at {s2orc_file}")

                            # with open(f"{corpusids_dst}/acl_corpusids.txt", 'a') as cf:
                            #     cf.write(f"{curr_corpusid}\n")
                            
                    else:
                        if not only_acl:  # only save non-ACL if only_ACL == False
                            subdir = f"{other_dst}/{subdir_name}"
                            paper_dir = f"{subdir}/{curr_corpusid}"
                            s2orc_file = f"{paper_dir}/s2orc-{curr_corpusid}.json"
                            if not exists(s2orc_file):
                                makedirs(paper_dir, exist_ok=True)
                                j = json.loads(l)
                                with open(s2orc_file, 'w') as sf:
                                    json.dump(j, sf, indent=4)
                                tqdm.write(f"created file at {s2orc_file}")

                                # with open(f"{corpusids_dst}/non_acl_corpusids.txt", 'a') as cf:
                                #     cf.write(f"{curr_corpusid}\n")

                    pbar.update(1)
                    

def get_s2_info(s2_papers_db: str = s2_papers_db_path, acl_dst: str = sub_a, 
                other_dst: str = sub_c, corpusids_src: str = datasets_path,
                batch_size: int = 5000):
    """For each paper in the Papers database, create {corpusId}.json (in either the ACL or non-ACL
    directory, as appropriate) containing Semantic Scholar info (e.g. corpusId, externalIds, etc.)

    Used instead of DEPRECATED get_s2_info_api(), which queries SemanticScholar

    Parameters
    ----------
        s2_papers_db (str): whether to extract only ACL files from s2orc
        acl_dst (str): path in which to save {corpusId}.json files, where corpusId is from a \
                       s2orc ACL file
        other_dst (str): path in which to save {corpusId}.json files, where corpusId is from \
                         a s2orc non-ACL file
        corpusids_src (str): path in which files containing ACL and non-ACL corpusIds are stored
        batch_size (int): number of files to batch for writing
    
    Returns
    ----------
        None
    """
    
    acl_corpusids = set()
    other_corpusids = set()

    # load corpusid sets from files
    with open(f"{corpusids_src}/acl_corpusids.txt") as f:
        for line in tqdm(f, total=80000):
            acl_corpusids.add(line.strip())
    
    with open(f"{corpusids_src}/non_acl_corpusids.txt") as f:
        for line in tqdm(f, total=10000000):
            other_corpusids.add(line.strip())

    batch = {}  # {/path/to/make/file/at/{corpusId}.json: papers_db_info}

    def write_batch():
        for file_path in tqdm(batch, leave=False):
            tqdm.write(f'Writing file at {file_path}')
            # with open(file_path, 'w') as f2:
            #     json.dump(batch[file_path], f2, indent=4)
        
        batch.clear()


    corpusid_pattern = compile(r'"corpusid":(\d+),')
    db_files = glob.glob(f"{s2_papers_db}/*.jsonl")

    for db_file in tqdm(db_files):
        with open(db_file) as f:
            with tqdm(total=7300000, leave=False, 
                      desc=f"Looping through {db_file.split('/')[-1]}") as pbar:
                for line in f:
                    match = corpusid_pattern.search(line)  # pull the corpusId from the curr line
                    
                    if match: curr_corpusid = match.group(1)
                    else: continue  # if there's no match, skip this line; something is wrong
                    
                    subdir_name = curr_corpusid[:4]

                    # if ACL corpusId, we'll make {corpusId}.json in acl_dst; then, make the file
                    # the key in a batch dict where associated value is the loaded JSON string 
                    # (curr line) where the value is the loaded JSON string (curr line) from the 
                    # curr papers dataset JSONL file
                    subdir_and_file = f"{subdir_name}/{curr_corpusid}/{curr_corpusid}.json"
                    if curr_corpusid in acl_corpusids:  
                        makedirs(f"{acl_dst}/{subdir_name}/{curr_corpusid}", exist_ok=True)
                        if not exists(f"{acl_dst}/{subdir_and_file}"):
                            batch[f"{acl_dst}/{subdir_and_file}"] = json.loads(line)
                            tqdm.write('added one to batch')
                        acl_corpusids.remove(curr_corpusid)
                    elif curr_corpusid in other_corpusids:  # same as L329, but for non-ACL
                        makedirs(f"{other_dst}/{subdir_name}/{curr_corpusid}", exist_ok=True)
                        if not exists(f"{other_dst}/{subdir_and_file}"):
                            batch[f"{other_dst}/{subdir_and_file}"] = json.loads(line)
                            tqdm.write('added one to batch')
                        other_corpusids.remove(curr_corpusid)

                    if len(batch) >= batch_size:  # when batch is full, write all stored files
                        write_batch()

                    pbar.update(1)

    write_batch()  # write out all remaining stored files (may be < batch_size)


def process_title(title: str):
    """Normalizes and cleans the provided title
    
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


def extract_annotation(s2orc_fo, field : str):
    # Returns the desired field from the text of a s2orc file object
    j = json.load(s2orc_fo)
    if not j['content']['annotations'][field]: return None
    data = j['content']['annotations'][field]
    data = literal_eval(data)
    out = []
    for i in data:
        out.append(j['content']['text'][i['start']:i['end']])

    return out


def load_completed_openalex_ids():
    found_filepaths = glob.glob(f"{datasets_path}/openalex_found_*.txt")
    unfound_filepaths = glob.glob(f"{datasets_path}/openalex_unfound_*.txt")
    found_ids = set()
    unfound_ids = set()
    for path in tqdm(found_filepaths):
        with open(path) as f:
            for line in f:
                found_ids.add(line.strip())
    for path in tqdm(unfound_filepaths):
        with open(path) as f:
            for line in f:
                unfound_ids.add(line.strip())
    return found_ids, unfound_ids

def get_openalex_info(mailto: str = "", acl_src: str = sub_a, other_src: str = sub_c, 
                      datasets_path: str = datasets_path, verbose: bool = False, start: int = 0, end: int = 10000,
                      use_all_completed: bool = False):
    """TODO
    """
    # Make files to keep track of which OpenAlex files have already been found/not found
    found_ids_filepath = f"{datasets_path}/openalex_found_{start}-{end}.txt"
    unfound_ids_filepath = f"{datasets_path}/openalex_unfound_{start}-{end}.txt"

    if use_all_completed:
        # This loads the set of all found and unfound ids, instead of just the ones within start-end.
        # I made this because I found a better set of start/end values, and there is no file telling which in those
        # ranges have already been found -Jason
        found_ids, unfound_ids = load_completed_openalex_ids()
    else:
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

    def write_unfound(unfound_corpus_id):
        with open(unfound_ids_filepath, 'a') as f:
            f.write(f"{unfound_corpus_id}\n")
            unfound_ids.add(unfound_corpus_id)
        paper_path = f"{unfound_corpus_id[:4]}/{unfound_corpus_id}/NOT_IN_OPENALEX"
        if is_acl:
            paper_path = acl_src + '/' + paper_path
        else:
            paper_path = other_src + '/' + paper_path
        with open(paper_path, 'w') as f:
            pass


    endpoint = "https://api.openalex.org/works"
    params = {"mailto": mailto, "per-page": 100}
    
    batches = {"mag": {}, "doi": {}, "date": {}, "year": {}, "title": {}}
    batches_info = {}  # {corpusId: {keys = "mag", "doi", ...}}; raw identifiers for batched corpusIds
    in_batches = {} # corpusId: (identifier, exact_key_stored_in_batches)

    def get_batch(identifier: str, b: list): 
        """Get a batch of 50 results from OpenAlex
        """       
        results_dict = {}  # {corpusId: result JSON}
        
        match identifier:
            case "mag" | "doi":
                filter_string = f"{identifier}:{'|'.join(b)}"
                params["filter"] = filter_string

                results = {}
                if verbose: tqdm.write("About to do requests.get...")
                while 'results' not in results:
                    if verbose: tqdm.write('Starting a while loop')
                    try:
                        results = requests.get(endpoint, params=params)
                        tqdm.write(f"{results.request.url}")
                        results = results.json()
                        if 'error' in results.keys(): tqdm.write(f"error in openalex results: {results['error']} \nmessage: {results['message']}")
                    except:
                        if verbose: 
                            tqdm.write(f'Trying requests.get again, id={identifier}')
                            print(params["filter"])
                        continue
                results = results['results']
                if verbose: tqdm.write("done requests.get, gotten results")

                # since some MAGs/DOIs mismatch/don't exist in OpenAlex, confirm which papers 
                # we found
                item_info = {v.lower() for v in b}  # all MAGs/DOIs in our batch

                for r in results:
                    # MAG/DOI value that was retrieved; .lower() because DOIs aren't case-sensitive, 
                    # but exactly matching one requires sensitivity
                    curr_info = r["ids"][identifier][0 if identifier == "mag" else 16:].lower()  
                    if curr_info in item_info:  # if the found MAG/DOI matches a MAG/DOI that we searched
                        corpus_id = None 

                        for k in batches[identifier]:
                            if batches[identifier][k] == curr_info:
                                corpus_id = k  # corpusId for the result MAG/DOI
                        
                        if not corpus_id: continue  # if match but no corpus_id, we already found it

                        # isACL is somewhat redundant because of Subcorpus A, but doesn't hurt to keep
                        update_dict = {"isACL": is_acl, "corpusId": corpus_id, 
                                       "foundVia": identifier}
                        r = {**update_dict, **r}
                        results_dict[corpus_id] = r
                        del batches[identifier][corpus_id]
                        del batches_info[corpus_id]
                        del in_batches[corpus_id]
            case "date" | "year" | "title":
                remove_from_batches = set()

                for corpus_id in tqdm(batches[identifier], desc=f"Making individual queries ({identifier})", 
                                      leave=False):  
                    curr_info = batches[identifier][corpus_id]
                    params["filter"] = curr_info  # don't need to make the filter string, since it's the key/id

                    results = {}
                    if verbose: tqdm.write("About to do requests.get...")
                    while 'results' not in results:
                        if verbose: tqdm.write('Starting a while loop')
                        try:
                            results = requests.get(endpoint, params=params)
                            tqdm.write(f"{results.request.url}")
                            results = results.json()
                            if 'error' in results.keys(): tqdm.write(f"error in openalex results: {results['error']} \nmessage: {results['message']}")
                        except:
                            if verbose: 
                                tqdm.write(f'Trying requests.get again, id={identifier}')
                                tqdm.write(params["filter"])
                            continue
                    if verbose: tqdm.write("done requests.get, gotten results")
                    
                    # first result *should* almost always be what we want, but sometimes isn't; check all
                    results = results["results"]
                    
                    if results:
                        for result in results:
                            result_title = process_title(result["title"])
                            orig_title = process_title(batches_info[corpus_id]["title"])
                            if result_title == orig_title:
                                update_dict = {"isACL": is_acl, "corpusId": corpus_id}
                                result = {**update_dict, **result}
                                results_dict[corpus_id] = result

                                # remove from batches after loop finishes to avoid error
                                remove_from_batches.add(corpus_id)
                                
                                del batches_info[corpus_id]
                                del in_batches[corpus_id]
                                break  # when a match has been made, break

                for remove in remove_from_batches: del batches[identifier][remove]

        result_items = tqdm(results_dict.items(), desc=f"Writing OpenAlex files ({identifier})", leave=False)
        for corpus_id, open_alex_result in result_items:
            if is_acl: dst_path = acl_src 
            else: dst_path = other_src  

            openalex_id = open_alex_result["id"][21:]
            paper_path = f"{dst_path}/{corpus_id[:4]}/{corpus_id}/{openalex_id}.json"
            with open(paper_path, "w") as f:
                json.dump(open_alex_result, f, indent=4)
            
            with open(found_ids_filepath, 'a') as f:
                f.write(f"{corpus_id}\n")
                found_ids.add(corpus_id)

        failed_for_identifier = {c for c in set(in_batches.keys()) if in_batches[c][0] == identifier}

        for unfound_corpus_id in failed_for_identifier:
            item_info = batches_info[unfound_corpus_id]
            del batches[identifier][unfound_corpus_id]  # remove item from batch type it failed
            del in_batches[unfound_corpus_id]  # then remove from in_batches, since we'll re-add shortly

            batches_info[unfound_corpus_id][identifier] = None

            for key in item_info.keys():  # "mag", "doi", etc.; earlier key = higher priority
                curr_info = batches_info[unfound_corpus_id][key]  # value for given identifier; something or None
                if curr_info: 
                    match key:
                        case "mag":  # can use raw MAG/DOI in batch
                            batches[key][unfound_corpus_id] = curr_info 
                            in_batches[unfound_corpus_id] = (key, curr_info) 
                            break
                        case "doi":
                            curr_info = curr_info.lower()
                            batches[key][unfound_corpus_id] = curr_info 
                            in_batches[unfound_corpus_id] = (key, curr_info) 
                            break
                        case "title" | "date" | "year":  # must create filter string for batch
                            title = batches_info[unfound_corpus_id]["title"]
                            if not title:
                                del batches_info[unfound_corpus_id]
                                write_unfound(unfound_corpus_id)
                                break
                            filter_string = f"""title.search:{re_sub(r'[,:!"]', ' ', title.lower())}"""
                            
                            if key != "title": 
                                filter_string += f",publication_{key}:{curr_info}"  #  ,date/year:filter_string
                            
                            batches[key][unfound_corpus_id] = filter_string
                            in_batches[unfound_corpus_id] = (key, filter_string)
                            break
                elif key == 'title':
                    del batches_info[unfound_corpus_id]
                    write_unfound(unfound_corpus_id)
                    break
                    
                       
    def check_batch(identifier: str, bypass: bool = False, verbose: bool = True):
        """Check if there are >= 50 items waiting to be batched for an identifier; if so, build 
        the batch list and call get_batch
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
            
            for key in batches[identifier].values():
                if len(batch) >= 50:
                    break 
                else:
                    batch.append(key)

            get_batch(identifier, batch)
            batch.clear()

        if bypass:    
            while len(batches[identifier]) > 0: do_batch()
        elif len(batches[identifier]) >= 50:
            if verbose: tqdm.write(f"IDENTIFIER {identifier} >= 50!!!")
            while len(batches[identifier]) >= 50: do_batch()
            if verbose:
                tqdm.write(f"FINISHED {identifier} (len = {len(batches[identifier])})" + 
                        "\n--------------------\n\n")
            
        match identifier:  # try to keep the flowdown relatively clean as we work
            case "mag": check_batch("doi", verbose=verbose)
            case "doi": check_batch("date", verbose=verbose)
            case "date": check_batch("year", verbose=verbose)
            case "year": check_batch("title", verbose=verbose)


    def add_to_batches(corpus_id: str, paper_ids: dict):
        batches_info[corpus_id] = paper_ids
        
        # Adding IDs to batches (set of ids for each id type), by first checking to see if the id exists 
        # in batched
        print(batches_info)
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
        elif not batches_info[corpus_id]['title']:
            del batches_info[corpus_id]
            if corpus_id in in_batches: del in_batches[corpus_id]
        else:
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

    is_acl = True
    pbar = tqdm(total=int(11000000/(10000/(end-start))), desc="Looping through all papers")

    for subcorpus in [acl_src, other_src]:
        if subcorpus == acl_src: is_acl = True
        else: is_acl = False 

        subdirs = tqdm([str(x) for x in range(start, end)], leave=False)
        for subdir in subdirs:
            subdirs.set_description(f'Looping through {subcorpus}/{subdir}')
            papers = glob.iglob(f"{subcorpus}/{subdir}/*/*.json")
            
            # Loop through papers begins here
            for paper in papers:
                # if not running from clean, avoid OpenAlex results and full extracted s2orc papers
                if "W" in paper or "s2orc" in paper: continue  
                # if the openalex paper has already been found, also skip
                curr_corpusid = paper.split("/")[-2]
                if curr_corpusid in found_ids or curr_corpusid in unfound_ids:
                    pbar.update(1)
                    continue                
                pbar.update(1)
                
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
                            case "journal": break
                    
                    if paper_ids["doi"]:
                        paper_ids['doi'] = re_sub(r'[^\w\.\/\(\)]', '', paper_ids['doi'])

                add_to_batches(curr_corpusid, paper_ids)

            # Next, loop through s2orc papers (looking specifically for ones that didn't have an s2 in the dir)
            s2orc_papers = glob.iglob(f"{subcorpus}/{subdir}/*/s2orc-*.json")
            for paper in s2orc_papers:
                curr_corpusid = paper.split("/")[-2]
                if curr_corpusid in found_ids or curr_corpusid in unfound_ids: # Check to see if we've already attempted to find this corpusid
                    pbar.update(1)
                    continue

                paper_ids = {}
                
                # Get paper_ids from paper, this time specifically for a s2orc file (we have to load json here)
                with open(paper) as f:
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

                    if paper_ids["doi"]:
                        paper_ids['doi'] = re_sub(r'[^\w\.\/\(\)]', '', paper_ids['doi'])
                    

                add_to_batches(curr_corpusid, paper_ids)
                pbar.update(1)
        
        # don't mix ACL and non-ACL in batch to make our lives easier
        check_batch("mag", True, verbose=verbose)
        check_batch("doi", True, verbose=verbose)
        check_batch("date", True, verbose=verbose)
        check_batch("year", True, verbose=verbose)
        check_batch("title", True, verbose=verbose)

        cprint(f"Finished {start}-{end} for {subcorpus}", c="c")
    
    pbar.close()
    cprint(f"Finished {start}-{end} for both Subcorpus A and Subcorpus C", c="g")


def extract_authors(acl_src: str = sub_a, non_acl_src: str = sub_c, corpora_path: str = corpora_path, 
                    datasets_path: str = datasets_path):
    """ Goes through all of the ACL openalex files, and creates an author file for each author.
        The lines of this file are the OpenAlex IDs of the ACL papers that author has written.
    
    Parameters
    ----------
        acl_src (str): the path to subcorpus A
        non_acl_src (str): the path to subcorpus C
        corpora_path (str): the main path to all corpora (this is where the authors directory will go)
    
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

    authors_path = f"{corpora_path}/authors"
    makedirs(authors_path, exist_ok=True)
    
    # TODO: allow alternate subcorpus name option (e.g. using acl_src and non_acl_src); but how do to that with iglob?
    papers = glob.iglob(f"{corpora_path}/subcorpus_*/*/*/W*.json")  
    for paper in tqdm(papers, total=11000000, desc="Extracting authors from all papers"): # papers estimate
        paper_split = paper.split('/')
        corpus_id = paper_split[-2]

        if corpus_id in seen_papers:  continue  # if we've seen this paper and its authors before, move on

        # paper_id = paper_split[-1][:-5]
        paper_is_acl = False if paper_split[-4] == 'subcorpus_c' else True  # L77
        authors = []

        with open(paper) as f:
            parser = ijson.parse(f)
            for prefix, event, value in parser:
                if prefix == 'authorships.item.author.id':
                    authors.append(value.split('/')[-1])
                if prefix == 'countries_distinct_count':
                    break
        
        for author in authors:
            author_subdir = author[1:5]
            author_path = f"{authors_path}/{author_subdir}"
            author_file = f"{author_path}/{author}.json"
            
            makedirs(author_path, exist_ok=True)  # make the author subdir if it doesn't exist

            try:
                with open(author_file, "r") as f: # if we've started extracting this author before, get their JSON
                    author_dict = json.load(f)
            except FileNotFoundError:
                author_dict = {"acl_papers": [], "non_acl_papers": []}
            
            acl_papers = set(author_dict["acl_papers"])  # temporarily cast to set to ensure no duplicates
            non_acl_papers = set(author_dict["non_acl_papers"])

            if paper_is_acl: acl_papers.add(corpus_id)
            else: non_acl_papers.add(corpus_id)

            author_dict["acl_papers"] = list(acl_papers)  # cast back into list, which is serializable
            author_dict["non_acl_papers"] = list(non_acl_papers)

            with open(author_file, "w") as f:  # save 
                json.dump(author_dict, f, indent=4)

        seen_papers.add(corpus_id)
        with open(seen_papers_filepath, "a") as f:
            f.write(f"{corpus_id}\n")


# These two functions are used in extract_authors_2 but they have to be top-level for multiprocessing
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

def get_paper_dirs(corpora_path: str = corpora_path, datasets_path: str = datasets_path):
    with open(f"{datasets_path}/acl_corpusids.txt") as f:
        acl_corpusids = {l.strip() for l in tqdm(f)}
    with open(f"{datasets_path}/non_acl_corpusids.txt") as f:
        non_acl_corpusids = {l.strip() for l in tqdm(f)}
    all_corpusids = acl_corpusids | non_acl_corpusids

    paper_dirs = []
    for id in tqdm(all_corpusids, desc='Making paths to paper dirs'):
        path = f"{corpora_path}/"
        path += 'subcorpus_a/' if id in acl_corpusids else 'subcorpus_c/'
        path += f"{id[:4]}/{id}"
        paper_dirs.append(path)

    return paper_dirs

def get_openalex_files(directory):
    print('.', end='')
    return glob.glob(f"{directory}/W*.json")

def get_file_paths_multiprocessing(directories):
    from multiprocessing import Pool
    with Pool() as pool:
        results = pool.map(get_openalex_files, directories)
    return [file for sublist in results for file in sublist]

def write_openalex_filepaths(datasets_path: str = datasets_path):
    paper_dirs = get_paper_dirs()
    print('Got dirs')
    oa_paths = get_file_paths_multiprocessing(paper_dirs)
    print('Got oa paths')
    with open(f"{datasets_path}/openalex_paths.txt", 'a') as f:
        for path in oa_paths:
            f.write(path + '\n')


def extract_authors_2(corpora_path: str = corpora_path, datasets_path: str = datasets_path):
    from multiprocessing import Pool
            
    def load_openalex_paths(subcorpus):
        with open(f"{datasets_path}/openalex_paths.txt") as f:
            oa_paths = {l.strip() for l in tqdm(f, desc='Loading oa paths')}
            return [x for x in tqdm(oa_paths, desc='filtering by subcorpus') if subcorpus in x]

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
    
    def update_authors_dict(authors_dict, author_info):
        author_id, corpusid, is_acl = author_info
        if author_id not in authors_dict:
            authors_dict[author_id] = {'acl_papers': [], 'non_acl_papers': []}
        if is_acl:
            authors_dict[author_id]['acl_papers'].append(corpusid)
        else:
            authors_dict[author_id]['non_acl_papers'].append(corpusid)

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
    # get_openalex_info(mailto='adamleif2024@u.northwestern.edu', start=2559, end=2560, verbose=True)
    extract_authors_2()
    # pass