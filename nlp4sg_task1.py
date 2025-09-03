import torch
from datasets import load_dataset
from transformers import AutoModelForSequenceClassification,AutoTokenizer,pipeline
import pandas as pd

device = torch.device('cuda:0' if torch.cuda.is_available() else 'cpu')
print('Using device:', device)

def load_data(dataset_path):
    dataset = load_dataset("csv", data_files={'test':dataset_path})
    return(dataset)

def main(dataset):
    tokenizer = AutoTokenizer.from_pretrained("feradauto/scibert_nlp4sg", truncation=True)
    model = AutoModelForSequenceClassification.from_pretrained("feradauto/scibert_nlp4sg").to(device)

    tokenizer_kwargs = {'padding': True, 'truncation': True, 'max_length': 512}

    classifier = pipeline(
        "text-classification",
        model=model,
        tokenizer=tokenizer, **tokenizer_kwargs
    )

    labels = []
    scores = []

    data_all = load_data(dataset)
    batch_size = 16

    for i in range(0, len(data_all['test']), batch_size):
        batch = data_all['test'][i:i+batch_size]

        texts = []
        for j in range(len(batch['title'])):
            text = ""
            if batch['title'][j]:
                text += batch['title'][j]
            if batch['abstract'][j]:
                text += batch['abstract'][j]
            texts.append(text)

        inputs = tokenizer(texts, return_tensors="pt", padding=True, truncation=True, max_length=512).to(device)
        outputs = model(**inputs)
        logits = outputs.logits

        predictions = torch.argmax(logits, dim=1).tolist()
        scores_batch = torch.nn.functional.softmax(logits, dim=1).max(dim=1).values.tolist()

        labels.extend(['NLP4SG' if p == 1 else 'Not NLP4SG' for p in predictions])
        scores.extend(scores_batch)

        if (i+batch_size)%1000==0:
            print(f"progress: {i + batch_size}/{len(data_all['test'])}")

    return labels, scores

if __name__ == '__main__':
    corpus_path = '/projects/p31502/data_allocation/corpora/comp_ling_meta/datasets/csvs/papers_merged_nlporacl_abstracts.csv'
    preds, scores = main(corpus_path)
    df = pd.read_csv(corpus_path)
    df['nlp4sg_label'] = preds
    df['nlp4sg_score'] = scores
    df.to_csv('nlp4sg_results_task_1.csv')