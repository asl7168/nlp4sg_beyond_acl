import csv, sys, json

input_csv = csv.DictReader(open(sys.argv[1]))
output_csv = csv.DictWriter(open('augmented_'+sys.argv[1], 'w'), fieldnames = input_csv.fieldnames  + ['year', 'nlp_concept', 'any_concept'])
output_csv.writeheader()

concepts = set(['C204321447',  'C23123220', 'C203005215', 'C119857082', 'C186644900', 'C28490314', 'C137293760'])

nlp_concept = 'C204321447'

for row in input_csv:
    j = json.load(open(row['openalex_path']))
    row['year'] = j['publication_year']

    row['any_concept'] = 0
    row['nlp_concept'] = 0

    for concept in j['concepts']:
        concept_id = concept['id'].split('/')[-1]
        if concept_id == nlp_concept:
            row['nlp_concept'] = 1
        if concept_id in concepts:
            row['any_concept'] = 1

    output_csv.writerow(row)
