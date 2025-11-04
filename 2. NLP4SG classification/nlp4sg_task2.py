#####################
# Predict UN Sustainable Development Goals associated with NLP4SG papers
#####################

import csv, os, sys, openai, argparse, torch
import pandas as pd
import numpy as np
from transformers import pipeline
from datasets import load_dataset

device = torch.device('cuda:0' if torch.cuda.is_available() else 'cpu')
print('Using device:', device)

candidate_labels = ['No Poverty',
 'No Hunger',
 'Good Health and Well-Being',
 'Quality Education',
 'Gender Equality',
 'Clean Water and Sanitation',
 'Affordable and Clean Energy',
 'Decent Work and Economic Growth',
 'Industry, Innovation and Infrastructure',
 'Reduced Inequalities',
 'Sustainable Cities and Communities',
 'Responsible Consumption and Production',
 'Climate Action',
 'Life Below Water',
 'Life on Land',
 'Peace, Justice and Strong Institutions',
 'Partnership for the Goals']

candidate_goals = [
'sdg1',
'sdg2',
'sdg3',
'sdg4',
'sdg5',
'sdg6',
'sdg7',
'sdg8',
'sdg9',
'sdg10',
'sdg11',
'sdg12',
'sdg13',
'sdg14',
'sdg15',
'sdg16',
'sdg17'
]

descs=['End poverty in all its forms everywhere',
   'End hunger, achieve food security and improved nutrition and promote sustainable agriculture',
   'Ensure healthy lives and promote well-being for all at all ages',
   'Ensure inclusive and equitable quality education and promote lifelong learning opportunities for all',
   'Achieve gender equality and empower all women and girls',
   'Ensure availability and sustainable management of water and sanitation for all',
   'Ensure access to affordable, reliable, sustainable and modern energy for all',
   'Promote sustained, inclusive and sustainable economic growth, full and productive employment and decent work for all',
   'Build resilient infrastructure, promote inclusive and sustainable industrialization and foster innovation',
   'Reduce inequality within and among countries',
   'Make cities and human settlements inclusive, safe, resilient and sustainable',
   'Ensure sustainable consumption and production patterns',
   'Take urgent action to combat climate change and its impacts',
   'Conserve and sustainably use the oceans, seas and marine resources for sustainable development',
   'Protect, restore and promote sustainable use of terrestrial ecosystems, sustainably manage forests, combat desertification, and halt and reverse land degradation and halt biodiversity loss',
   'Promote peaceful and inclusive societies for sustainable development, provide access to justice for all and build effective, accountable and inclusive institutions at all levels',
   'Strengthen the means of implementation and revitalize the global partnership for sustainable development']

class MLModel:
    def __init__(self):
        pass
    
    def predict(self, data):
        
        prediction = self.perform_prediction(data)
        processed_response = self.process_response(prediction)
        return processed_response
    
    def perform_prediction(self, processed_data):
        raise NotImplementedError
    def process_response(self, data):
        raise NotImplementedError

class OpenAIModel(MLModel):
    def __init__(self,model_version=None):
        self.model_version = model_version
        self.preprompt="There is an NLP paper with the title and abstract:\n"
        self.question="Which of the UN goals does this paper directly contribute to? Provide the goal number and name."
        self.OPENAI_API_KEY=os.getenv("OPENAI_API_KEY")
        self.sdg_map = {
            'sdg1': ['goal1', 'goal 1:', 'poverty'],
            'sdg2': ['goal2', 'hunger'],
            'sdg3': ['goal3',  'health'],
            'sdg4': ['goal4', 'quality education', 'education'],
            'sdg5': ['goal5', 'gender'],
            'sdg6': ['goal6', 'clean water'],
            'sdg7': ['goal7', 'energy'],
            'sdg8': ['goal8', 'decent work', 'economic growth', 'employment'],
            'sdg9': ['goal9', 'industry', 'innovation', 'infrastructure'],
            'sdg10': ['goal10', 'inequal'],
            'sdg11': ['goal11', 'sustainable cities'],
            'sdg12': ['goal12', 'responsible consumption', 'sustainable consumption'],
            'sdg13': ['goal13', 'climate'],
            'sdg14': ['goal14', 'life below water', 'ocean'],
            'sdg15': ['goal15', 'life on land'],
            'sdg16': ['goal16', 'peace', 'justice'],
            'sdg17': ['goal17', 'partnership']
        }
        self.response_columns=['prompt','response','sdg1','sdg2','sdg3','sdg4','sdg5','sdg6','sdg7','sdg8','sdg9', 'sdg10','sdg11','sdg12','sdg13','sdg14','sdg15','sdg16','sdg17']
    
    def perform_prediction(self, data):
        prediction=dict()
        data=self.preprompt+data+"\n"+self.question
        completion = openai.Completion.create(engine=self.model_version, prompt=data,temperature=0,max_tokens=100,logprobs=1)
        prediction['response']=completion.choices[0].text
        prediction['prompt']=data
        return prediction
    
    def process_response(self, pred):
        predictions = {'sdg1': 0.0, 'sdg2': 0.0, 'sdg3': 0.0, 'sdg4': 0.0, 'sdg5': 0.0, 'sdg6': 0.0, 'sdg7': 0.0, 'sdg8': 0.0, 'sdg9': 0.0, 'sdg10': 0.0, 'sdg11': 0.0, 'sdg12': 0.0, 'sdg13': 0.0, 'sdg14': 0.0, 'sdg15': 0.0, 'sdg16': 0.0, 'sdg17': 0.0}
        for sdg_label, substrings in self.sdg_map.items():
            for substring in substrings:
                if substring in pred['response'].lower():
                    predictions[sdg_label]=1
        predictions['prompt']=pred['prompt']
        predictions['response']=pred['response']
        return predictions

def load_data(dataset_path):
    dataset = load_dataset("csv", data_files={'test':dataset_path})['test']
    return(dataset)


def main(args):
    data=load_data("./nlp4sg_results_task_1.csv")
    model=OpenAIModel(args['model'])
    with open("results_task_2.csv", 'w', newline='') as file:
        csv_writer = csv.writer(file)
        cols=['ID', 'title', 'abstract', 'text', 'year', 'nlp4sg_score']
        csv_writer.writerow(cols+model.response_columns)
        for d in data:
            output=model.predict(d['text'])
            final_output=[d['ID'],d['title'],d['abstract'],d['text'],d['year'],d['nlp4sg_score']]
            for c in model.response_columns:
                final_output.append(output[c])
            csv_writer.writerow(final_output)

if __name__ == '__main__':
    args=argparse.ArgumentParser()
    args.add_argument("--model", type=str, default="text-davinci-002")
    args=vars(args.parse_args())
    main(args)