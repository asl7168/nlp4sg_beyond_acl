import pandas as pd
import ast, swifter

def get_abstract_text(s2orc_path):
    try:
        s2orc_file = pd.read_json(s2orc_path)
        abstract_ixs = ast.literal_eval(s2orc_file['content']['annotations']['abstract'])[0]
        abstract = s2orc_file['content']['text'][int(abstract_ixs['start']):int(abstract_ixs['end'])]
        return(abstract)
    except:
        return('')

def get_year(openalex_path):
    try:
        with open(openalex_path, 'r') as f:
            openalex_data = json.loads(f.read())
            print(openalex_data['publication_year'])
            return(openalex_data['publication_year'])
    except:
        return('')

#df = pd.read_csv('/projects/p31502/data_allocation/corpora/comp_ling_meta/datasets/csvs/papers_merged_nlporacl.csv')
#df['abstract'] = df.swifter.apply(lambda row: get_abstract_text(row['s2orc_path']), axis=1)

df = pd.read_csv('/projects/p31502/projects/comp_ling_meta/nlp4sg_results_task_1_nlporacl.csv')
df = df[df['abstract'].notna()]
df = df[df['title'].notna()]
df['publication_year'] = df.swifter.apply(lambda row: get_year(row['openalex_path']), axis=1)
df['acl_author'] = df['max_acl_contribs'].apply(lambda x: True if x>=3 else False)

df.to_csv('/projects/p31502/projects/comp_ling_meta/nlp4sg_results.csv')