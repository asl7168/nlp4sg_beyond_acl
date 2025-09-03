import json
from collections import Counter
from os import listdir, mkdir, rename
from os.path import isdir, exists
from tqdm import tqdm
import ijson

s2_path = "/projects/b1170/corpora/semantic_scholar"
s2orc_path = f"{s2_path}/s2orc"
s2orc_papers_path = f"{s2orc_path}/papers"
papers_path = f"{s2_path}/papers"
acl_papers_path = f"{s2_path}/papers/acl"
acl_authors_path = f"{acl_papers_path}/authors"
over_three_path = f"{acl_authors_path}/over_three"
acl_s2_path = f"{acl_papers_path}/s2"
acl_openalex_path = f"{acl_papers_path}/open_alex"

def looking_at_concepts():
    c = Counter()
    subdirs = [a for a in listdir(acl_openalex_path) if isdir(f"{acl_openalex_path}/{a}")]
    for subdir in tqdm(subdirs):
        path = f"{acl_openalex_path}/{subdir}"
        acl_files = [a for a in listdir(path) if '.json' in a]
        for file in tqdm(acl_files, leave=False):
            f = open(f"{path}/{file}")
            j = json.load(f)
            f.close()
            for concept in j['concepts']:
                c[concept['display_name']] += concept['score']
        print(c)
    f = open(f"{acl_openalex_path}/concepts.json", 'w')
    json.dump(c, f, indent=4)
    f.close()
    return

def find_acl_papers_without_concept():
    nlp_id = 'C204321447'
    ai_id = 'C154945302'
    ling_id = 'C41895202'
    comp_sci = 'C41008148'
    check_concepts = {'C204321447', 'C154945302', 'C41895202', 'C23123220', 'C203005215', 'C119857082', 
                'C186644900', 'C28490314', 'C2777530160'}
    l = []
    subdirs = [a for a in listdir(acl_openalex_path) if isdir(f"{acl_openalex_path}/{a}")]
    for subdir in tqdm(subdirs):
        path = f"{acl_openalex_path}/{subdir}"
        acl_files = [a for a in listdir(path) if '.json' in a]
        for file in tqdm(acl_files, leave=False):
            f = open(f"{path}/{file}")
            j = json.load(f)
            f.close()
            concepts = {c['id'][21:] for c in j['concepts']}
            j_id = j['id'][21:]
            if not concepts & check_concepts:
                l.append(j_id)
    with open(f"{acl_openalex_path}/papers_without_robs_concepts.txt", 'w') as f:
        for line in l:
            f.write(f"{line}\n")
    return

def concept_checking():
    """for all papers"""
    subdirs = [a for a in listdir(papers_path) if isdir(f"{papers_path}/{a}")]
    num_passed, num_failed = 0,0
    check_concepts = {'C204321447', 'C154945302', 'C41895202', 'C23123220', 'C203005215', 'C119857082', 
                'C186644900', 'C28490314', 'C2777530160', 'C137293760'}
    
    for subdir in tqdm(subdirs):
        path = f"{papers_path}/{subdir}"
        work_files = [a for a in listdir(path) if '.json' in a]
        pbar = tqdm(work_files, leave=False)
        for file in pbar:
            pbar.set_description(f"{num_passed} - {num_failed}")
            f = open(f"{path}/{file}")
            j = json.load(f)
            f.close()
            concepts = {c['id'][21:] for c in j['concepts']}
            lang = j['language']
            if not lang == 'en':
                num_failed += 1
                continue
            if not concepts & check_concepts:
                num_failed += 1
                continue
            num_passed += 1
        print(f"{num_passed} - {num_failed}")

def acl_papers_languages():
    c = Counter()
    subdirs = [a for a in listdir(acl_openalex_path) if isdir(f"{acl_openalex_path}/{a}")]
    for subdir in tqdm(subdirs):
        path = f"{acl_openalex_path}/{subdir}"
        acl_files = [a for a in listdir(path) if '.json' in a]
        for file in tqdm(acl_files, leave=False):
            f = open(f"{path}/{file}")
            j = json.load(f)
            f.close()
            lang = j['language']
            c[lang] += 1
    com = c.most_common()
    f = open(f"{acl_openalex_path}/acl_papers_languages.json", 'w')
    json.dump(com, f, indent=4)
    f.close()
    return

def acl_abstract_count():
    subdirs = [a for a in listdir(acl_openalex_path) if isdir(f"{acl_openalex_path}/{a}")]
    abstract_count = 0
    files_lookedat = 0
    for subdir in tqdm(subdirs):
        path = f"{acl_openalex_path}/{subdir}"
        acl_files = [a for a in listdir(path) if '.json' in a]
        pbar = tqdm(acl_files, leave=False)
        for file in pbar:
            pbar.set_description(f"{abstract_count} / {files_lookedat}")
            f = open(f"{path}/{file}")
            j = json.load(f)
            f.close()
            if j['abstract_inverted_index']:
                abstract_count += 1
            files_lookedat += 1
    print(f"{abstract_count} / {files_lookedat}")
    # RESULTS: 57161 / 71326 ACL papers here have abstract data

test_abstract = {'a': [0, 2, 4], 
                 'man': [1],
                 'plan': [3],
                 'canal': [5],
                 'panama': [6]}

def reconstruct_abstract(abstract: dict):
    content = abstract.items()
    result = []
    for word, indices in content:
        for index in indices:
            result.append((word,index))
    sorted_result = sorted(result, key=lambda x:x[1])
    return ' '.join([x[0] for x in sorted_result])

def abstract_testing():
    subdirs = [a for a in listdir(acl_openalex_path) if isdir(f"{acl_openalex_path}/{a}")]
    for subdir in tqdm(subdirs):
        path = f"{acl_openalex_path}/{subdir}"
        acl_files = [a for a in listdir(path) if '.json' in a]
        for file in tqdm(acl_files, leave=False):
            f = open(f"{path}/{file}")
            j = json.load(f)
            f.close()
            if j['abstract_inverted_index']:
                print(reconstruct_abstract(j['abstract_inverted_index']))
    return

def null_language():
    subdirs = [a for a in listdir(acl_openalex_path) if isdir(f"{acl_openalex_path}/{a}")]
    for subdir in tqdm(subdirs):
        path = f"{acl_openalex_path}/{subdir}"
        acl_files = [a for a in listdir(path) if '.json' in a]
        for file in tqdm(acl_files, leave=False):
            f = open(f"{path}/{file}")
            j = json.load(f)
            f.close()
            if j['language'] == None:
                print(j['ids'])
    
def is_acl(id):
        """Given an OA W id, is it in /acl/open_alex?
        """
        if id[1:4] not in set(listdir(acl_openalex_path)):
            return False
        path = f"{acl_openalex_path}/{id[1:4]}"
        if f"{id}.json" in listdir(path):
            return True
        else:
            return False
        
def repair_works():
    subdirs = [a for a in listdir(papers_path) if isdir(f"{papers_path}/{a}") and a != 'acl']
    for subdir in tqdm(subdirs):
        path = f"{papers_path}/{subdir}"
        work_files = [a for a in listdir(path) if '.json' in a]
        for file in tqdm(work_files, leave=False):
            id = file[:-5]
            if is_acl(id):
                f = open(f"{path}/{file}")
                j = json.load(f)
                f.close()
                
                if not 'isACL' in j:
                    if "IsAcl" in j:
                        del j["IsAcl"]

                    c = j["corpusId"]
                    del j["corpusId"]

                    update_dict = {"isACL": True, "corpusId": c}
                    j = {**update_dict, **j}

                    with open(f"{path}/{file}", 'w') as f:
                        json.dump(j, f, indent=4)
            
def repair_works2():
    complete = [100,103,104,107,108,109,110,111,112,114,115,116,119,121,122,123,124,125,126,127,128,
                129,130,133,134,135,137,138,139,140,141,142,145,146,149,150,151,152,153,154,157,158,
                160,161,163,164,165,166,169,171,172,175,176,177,178,179,180,183,184,187,188,189,190,
                191,192,194,195,196,199,200,201,203,204,205,208,209,210,211,212,213,214,215,216,217,
                222,223,224,226,227,228,229,230,231,234,235,238,239,240,241,242,243,246,247,251,252,
                253,254,255,258,259,260,261,263,264,265,266,267,268,269,272,273,276,277,278,280,281,
                283,284,285,288,289,290,291,292,293,294,295,296,297,300,301,302,303,304,305,306,309,
                310,311,312,314,315,316,317,318,320,322,323,324,326,327,328,330,331,332,334,335,336,
                338,339,340,341,342,343,344,346,347,348,352,353,354,355,356,357,358,359,360,361,362,
                364,365,367,368,369,370,371,372,373,376,377,378,380,381,382,384,385,389,390,391,394,
                396,397,398,400,401,403,405,406,407,409,411,412,415,416,417,419,420,423,424,425,427,
                428,429,430,431,432,433,435,436,437,439,441,442,443,444,446,448,449,450,453,454,455,
                457,458,459,460,461,462,469,470,471,473,474,475,477,478,479,481,483,484,485,486,487,
                489,491,492,495,496,497,500,501,504,505,506,507,509,513,516,517,518,519,521,523,524,
                525,528,529,532,533,534,536,537,542,543,544,546,547,548,549,550,551,554,555,559,560,
                561,562,563,566,567,572,573,575,578,579,580,584,585,586,587,588,589,592,593,596,597,
                598,599,601,605,606,607,609,610,612,613,614,617,618,620,621,622,623,624,625,626,629,
                630,631,632,633,636,637,638,640,643,644,647,649,650,651,652,655,656,659,660,661,663,
                664,666,667,668,670,672,673,674,675,676,677,678,679,681,682,685,686,687,690,692,694,
                697,698,700,702,703,705,706,711,712,714,715,718,719,720,723,724,726,727,731,732,733,
                735,736,737,738,740,741,743,744,745,747,748,749,750,751,752,753,755,756,757,761,762,
                763,764,765,766,767,768,769,770,771,773,774,775,776,777,778,779,780,781,782,783,785,
                786,787,789,790,791,792,793,795,798,799,800,801,802,803,804,806,807,808,810,812,813,
                814,816,818,819,820,821,824,825,826,827,828,829,832,833,836,837,838,839,840,841,842,
                844,845,846,848,849,850,851,852,853,854,855,856,857,858,862,863,864,866,867,868,869,
                870,871,874,875,878,879,881,882,883,886,888,890,892,893,894,895,896,898,899,901,902,
                903,904,907,908,910,913,916,918,919,921,922,925,927,928,929,930,932,933,937,938,940,
                941,942,943,944,946,949,951,952,953,955,956,957,958,959,960,963,964,967,968,969,970,
                971,972,975,976,979,981,982,983,984,987,988,990,993,994,995,996,997,998,999]
    
    complete = list(map(lambda x: str(x), complete))
    subdirs = tqdm([a for a in listdir(papers_path) if a not in complete and 
                    isdir(f"{papers_path}/{a}") and "a"])

    for subdir in subdirs:
        subdirs.set_description(f"Looping through /papers/{subdir}")
        path = f"{papers_path}/{subdir}"
        work_files = [a for a in listdir(path) if '.json' in a]
        
        subdirs.write(f"Checking {subdir}")
        for file in tqdm(work_files, leave=False):
            f_path = f"{path}/{file}"
            
            if not exists(f"{acl_openalex_path}/{subdir}/{file}"):  # if not acl file
                f = open(f_path, "r")
                try:
                    j = json.load(f)
                    f.close()
                except:
                    f.close()
                    subdirs.write(f"EMPTY {file}")
                    continue

                if not 'isACL' in j:  # if this file hasn't been fixed already
                    update_dict = {"isACL": False, "corpusId": None}
                    o = {**update_dict, **j}
                    with open(f_path, 'w') as f3:
                        json.dump(o, f3, indent=4)
                    
                    subdirs.write(f"Fixed {file}")
        
        subdirs.write("-----\n")


def another_one():
    backup_path = "/projects/b1170/corpora/semantic_scholar/papers/acl/open_alex_backup_and_other_files"
    dirs = tqdm([d for d in listdir(backup_path) if isdir(f"{backup_path}/{d}")])

    for dir in dirs:
        dir_path = f"{backup_path}/{dir}"
        dirs.set_description(f"Checking {dir_path}")
        files = tqdm([f for f in listdir(dir_path)], leave=False)

        for file in files:
            acl_dir_path = f"{acl_openalex_path}/{dir}"
            acl_file_path = f"{acl_dir_path}/{file}"
            backup_file_path = f"{dir_path}/{file}"
            papers_file_path = f"{papers_path}/{dir}/{file}"

            # if not exists(papers_file_path):
            if not exists(acl_file_path): 

                f = open(backup_file_path)
                j = json.load(f)
                f.close()

                if "IsAcl" in j: del j["IsAcl"]
                if "isAcl" in j: del j["isAcl"]
                c = j["corpusId"]
                del j["corpusId"]
                update_dict = {"isACL": True, "corpusId": c}
                o = {**update_dict, **j}

                # f2 = open(papers_file_path, "w")
                # json.dump(o, f2, indent=4)
                # f2.close()

                # if not exists(acl_file_path): 
                if not exists(acl_dir_path): mkdir(acl_dir_path)
                f3 = open(acl_file_path, "w")
                json.dump(o, f3, indent=4)
                f3.close()
                dirs.write(f"Fixed {file}")

def repair_ai_concept(papers_path: str = papers_path, 
                         concepts: set = {'C204321447', 'C41895202', 'C23123220', 
                                          'C203005215', 'C119857082', 'C186644900', 'C28490314', 
                                          'C2777530160', 'C137293760'},
                         threshold: float = 0.0):
    """For every OpenAlex paper/work JSON file, filter out broken, non-English, non-computational
    linguistics papers

    Parameters
    ----------
        papers_path (str): path to all OpenAlex papers (sorted into subdirs of the form NNN)
        concepts (listof str): list of computational linguistics concepts
        threshold (float): concept score must be greater than this to count as a concept match
        
    Returns
    ----------
        None
    """
    if threshold < 0 or threshold > 1:
        raise ValueError(f"threshold value must be between 0.0 and 1.0\nthreshold={threshold}")

    relevant_path = f"{papers_path}/relevant"
    irrelevant_path = f"{papers_path}/irrelevant"

    if not exists(relevant_path): mkdir(relevant_path)
    if not exists(irrelevant_path): mkdir(irrelevant_path)

    works_dirs = tqdm([d for d in listdir(relevant_path) if "a" not in d 
                       and isdir(f"{relevant_path}/{d}")], 
                       desc="Filtering relevant works")
    for wdir in works_dirs:
        works_dirs.write(f"Filtering {wdir}")

        dir_path = f"{relevant_path}/{wdir}"
        papers = tqdm([p for p in listdir(dir_path) if ".json" in p], leave=False)

        for paper in papers:
            papers.set_description(f"Filtering {wdir}")

            keep_paper = False
            src_paper_path = f"{dir_path}/{paper}"
            
            f = open(src_paper_path)
            parser = ijson.parse(f)            
            
            count = 0
            seen_acl = False  # ensure isACL is in each paper; if it's not, note that
            last_seen_concept = None

            try:
                for prefix, event, value in parser:
                    # print(f"prefix={prefix}, event={event}, value={value}")
                    
                    if count > 1 and not seen_acl:
                        keep_paper = False
                        break
                    
                    if value == "isACL":
                        seen_acl = True
                        continue
                    
                    if prefix == "isACL" and value == True:  # TODO: confirm if value will evaluate as bool
                        keep_paper = True
                        break
                    elif prefix == "language" and value != "en":
                        keep_paper = False
                        break
                    elif prefix == "concepts.item.id":
                        last_seen_concept = value.split("/")[-1]
                    elif prefix == "concepts.item.score" and last_seen_concept in concepts and value > threshold:
                        keep_paper = True
                        break
                    
                    count += 1
                
                f.close()
            except Exception as e:
                f.close()
                works_dirs.write(f"{paper} EMPTY")
                continue

            if seen_acl:
                # tqdm.write(f"paper: {paper}, keep_paper: {keep_paper}")
                if keep_paper:
                    pass
                else:
                    irrelevant_subdir = f"{irrelevant_path}/{wdir}"
                    if not exists(irrelevant_subdir): mkdir(irrelevant_subdir)
                    rename(src_paper_path, f"{irrelevant_subdir}/{paper}")
                    # print(src_paper_path, f"{irrelevant_subdir}/{paper}")
                    # Now also move the s2orc file
                    s2orc_paper_path = f"{s2orc_papers_path}/{wdir}/{paper}"
                    if exists(s2orc_paper_path):
                        dest_path = f"{s2orc_path}/only_ai_papers/{paper}"
                        rename(s2orc_paper_path, dest_path)
                        # print(s2orc_paper_path, dest_path)

            else:
                works_dirs.write(f"{paper} MISSING 'isACL' key")
            
        works_dirs.write("-------\n")



if __name__ == "__main__":
    # acl_abstract_count()
    # repair_works2()
    # another_one()
    repair_ai_concept()