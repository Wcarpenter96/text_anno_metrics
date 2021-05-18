import datetime
import numpy as np 
import os
import pandas as pd
import requests
import time
import timeit
import zipfile


'''
    Variables to update:

    * ANNO_JOB: Job ID of ADAP job where each chunk is annotated by 2 annotators 
    * DISAGREEMENT_RESOLUTION: Job ID of ADAP QA job where only disagreements between annotators are reviewed (95% Batch)
    * FULL_REVIEW: Job ID of ADAP QA job where every annotation in the chunk is reviewed, including those where annotators agree (5% Batch)
    * API_KEY: Your ADAP API Key (Do not store this value in the script!)

'''

# UPDATE START
ANNO_JOB = 1234567
DISAGREEMENT_RESOLUTION = 1234567
FULL_REVIEW = 1234567
API_KEY ='DO NOT STORE'
# UPDATE END

# Project settings
labels = ['coref','entity'] # annotation labels
data_column = 's3_location' # job annotation input
results_header = 'annotation_edit' # job annotation input

def timer_desc(time_start):
    time_end = timeit.default_timer()
    elapsed_minutes = round((time_end - time_start) / 60, 2)
    return(f'--- Total Running Time: {elapsed_minutes} minutes.')


def regenerate_jobid(list_job_ids, params):
    '''
    Takes in a list of job IDs and param {'key': my_api_key, 'type': reporttype}
    '''
    counter = 0
    for jid in list_job_ids:
        response = requests.post(f'https://api.figure-eight.com/v1/jobs/{jid}/regenerate', params=params)
        counter += 1
        print(f"Regenerating {params['type'].upper()} report {jid} -- Report # {counter}")

def downloadextract_jobid(list_job_ids, params):
    '''

    Args:
        list_job_ids ([type]): [description]
        params ([type]): [description]
    '''
    for jid in list_job_ids:
        counter = 0
        time_start = timeit.default_timer()
        print(f"Running download command for {params['type'].upper()} report {jid}")
        url = f'https://api.appen.com/v1/jobs/{jid}.csv'
        while True:
            response = requests.get(url, params=params)
            counter += 1
            print(f"-- Response: {response.status_code} -- {str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))} -- Count:{counter}")
            if response.status_code == 200:
                print("That's good!")
                break
            time.sleep(1)
        response = requests.get(url, params=params)
        while not response.ok:
            print(f"Download Failed: Attempting re-download {params['type'].upper()} report {jid}")
            response = requests.get(url, params=params)
            time.sleep(1)

        fname = f'{params["type"]}_{jid}.zip'
        print(f'Download complete for job {jid} Saving report as {fname}\n{timer_desc(time_start)}')
        output = open(fname, 'wb')
        output.write(response.content)
        output.close()

        print(f'-- Extracting {fname}')
        with zipfile.ZipFile(fname) as file:
            file.extractall()

        df = pd.read_csv(fname)
        df['jobid'] = jid
        os.remove(fname)

def api_downloadReports(list_job_ids):
    params = {'key': API_KEY, 'type': 'full'}
    regenerate_jobid(list_job_ids, params)
    downloadextract_jobid(list_job_ids, params)

def clean_df(df):
    return df.drop(columns=['_unit_id','_created_at','_id','_started_at','_tainted','_channel','_trust','_country','_region','_city','_ip'])

def create_chunk_cols(df):
    df['chunk_source'] = df[data_column]
    df['chunk_anno'] = df[results_header]
    df['combined_chunk_anno'] = df[f'{results_header}_qa']
    df['chunk_qa'] = df[f'{results_header}_qa']
    return df

def create_token_dict(json_file_url,labels):
    json_obj = requests.get(json_file_url).json()
    json_obj_spans = json_obj['spans']
    token_dict = {}
    for span in json_obj_spans:
        span_tokens = span['tokens']
        span_text = ' '.join([x['text'] for x in span_tokens])
        new_token_key = (span_text, span_tokens[0]['startIdx'], span_tokens[-1]['endIdx'])
        classnames = span['classnames']
        for label in labels:
            if label in classnames:
                token_dict[new_token_key] = span
    return token_dict 

def create_label_dict(token_dict,label):
    label_dict = {}
    for token in token_dict:
        classnames = token_dict[token]['classnames']
        if label in classnames:
            label_dict[token] = token_dict[token]
    return label_dict

def get_precision(true_positives, false_positives):
    precision = true_positives/(true_positives + false_positives)

    return precision


def get_recall(true_positives, false_negatives):
    recall = true_positives/(true_positives + false_negatives)

    return recall

def get_totals(token_dict, gold_token_dict):
    false_positives = 0
    false_negatives = 0
    true_positives = 0

    for token_key in token_dict:
        if token_key in gold_token_dict:
            true_positives += 1
        else:
            false_positives += 1
    for token_key in gold_token_dict:
        if token_key not in token_dict:
            false_negatives += 1

    return true_positives, false_positives, false_negatives

# Calculate precision and recall
def get_pr(token_dict, token_dict_gold):
    true_positives, false_positives, false_negatives = get_totals(token_dict, token_dict_gold)
    precision = 0
    recall = 0
    if true_positives + false_positives != 0:
        precision = get_precision(true_positives, false_positives)
    if true_positives + false_negatives != 0:
        recall = get_recall(true_positives, false_negatives)
        
    return {'precision':precision,'recall':recall}

def get_correction_totals(token_dict, token_dict_gold, token_dict_source):
    
    annotator = set(token_dict)
    gold = set(token_dict_gold)
    source = set(token_dict_source)
            
#     Correct additions
    true_positives = len(annotator.intersection(gold)-source)
#     Correct removals
    true_positives += len(source-annotator-gold)
#     Incorrect additions
    false_positives = len(annotator-gold-source)
#     Incorrect removals
    false_positives += len(source.intersection(gold)-annotator)
#     Failed to add
    false_negatives = len(gold-annotator-source)
#     Failed to remove
    false_negatives += len(annotator.intersection(source)-gold)
    
    return true_positives,false_positives,false_negatives

# Calculate correction precision and correction recall
def get_correction_pr(token_dict,token_dict_gold,token_dict_source):
    true_positives,false_positives,false_negatives = get_correction_totals(token_dict,token_dict_gold,token_dict_source)
    
    precision = 0.0
    recall = 0.0
    if true_positives + false_positives != 0:
        precision = get_precision(true_positives, false_positives)
    if true_positives + false_negatives != 0:
        recall = get_recall(true_positives, false_negatives)
      
    return {'precision':precision,'recall':recall}

def is_subset(a1,a2,b1,b2):
    return (b1 < a1 and a2 <= b2) or (b1 <= a1 and a2 < b2)
    
def is_superset(a1,a2,b1,b2):
    return (a1 < b1 and b2 <= a2) or (a1 <= b1 and b2 < a2)
    
def is_partial_overlap(a1,a2,b1,b2):
    return a1<b1<a2<b2 or b1<a1<b2<a2

def get_overlap(token,token_dict_gold):
    sub,sup,po = 0,0,0
    s,e = token[1],token[2]
    for token_gold in token_dict_gold:
        sg,eg = token_gold[1],token_gold[2]
        sub += 1 if is_subset(s,e,sg,eg) else 0 
        sup += 1 if is_superset(s,e,sg,eg) else 0
        po += 1 if is_partial_overlap(s,e,sg,eg) else 0
    return sub,sup,po

# Calculate span comparisons
def compare_spans(token_dict,token_dict_gold):
    subset,superset,partial_overlap = 0,0,0
    tokens,tokens_gold = [],[]
    for token in token_dict:
        if len(token_dict[token]['classnames']):
            tokens.append(token)
    for token in token_dict_gold:
        if len(token_dict_gold[token]['classnames']):
            tokens_gold.append(token)
    l = len(tokens)
    for token in tokens:
        sub,sup,po = get_overlap(token,tokens_gold)
        subset += sub
        superset += sup
        partial_overlap += po
    subset = subset/l
    superset = superset/l
    partial_overlap = partial_overlap/l
    return {'subset':subset,'superset':superset,'partial_overlap':partial_overlap}

# Calculate Fleiss Kappa IAA between annotators
def get_fleiss_k(annos,labels):
    M = []
    token_tally = {}
    # filter out tokens not shared between annotators
    for token_dict in annos:
        M.append([0,0])
        for token in token_dict: 
            classnames = token_dict[token]['classnames']
            for label in labels:
                if label in classnames:
                    token_tally[token] = token_tally.get(token,0) + 1
    tokens = {k:v for k,v in token_tally.items() if v==len(annos)}
    # Count instances of each label across annotators
    for token in tokens:
        for i,token_dict in enumerate(annos):
            classnames = token_dict[token]['classnames']
            for j,label in enumerate(labels):
                if labels[j] in classnames:
                    M[i][j] = M[i][j] + 1 
    # Find expected agreement (p_e) and actual agreement (p_a)
    M = np.matrix(M)
    m = len(tokens)
    n = len(annos)
    sum_sq_M = np.sum(np.square(M))
    p_a = (sum_sq_M-m*n)/(m*n*(m-1))
    q = M.sum(axis=0)/(m*n)
    p_e = np.sum(np.square(q))
    k = (p_a-p_e)/(1-p_e)
    return k

def get_annotator_vs_review(tbe, gold, source):

    pr = get_pr(tbe, gold)
    correction_pr = get_correction_pr(tbe, gold, source)
    span_comparison = compare_spans(tbe, gold)

    stats = {
        'precision': {
            'all': pr['precision']
        },
        'recall': {
            'all': pr['recall']
        },
        "correction": correction_pr,
        'span_comparison': span_comparison
    }
    # Calculate precision and recall for each label
    for label in labels:
        pr_label = get_pr(create_label_dict(tbe, label), create_label_dict(gold, label))

        stats['precision'][label] = pr_label['precision']
        stats['recall'][label] = pr_label['recall']

    return stats

def get_annotator_vs_annotator(chunk_anno_1,chunk_anno_2):
    # Get IAA between two annotators
    return {'iaa': get_fleiss_k([chunk_anno_1,chunk_anno_2],labels)}

def accum_metrics(dict,key,labels,metrics,function):
    a = dict.get(key,{})
    a[f'{function}_ct'] = a.get(f'{function}_ct',0) + 1
    a[f'{function}_precision'] = a.get(f'{function}_precision',0) + metrics['precision']['all']
    a[f'{function}_recall'] = a.get(f'{function}_recall',0) + metrics['precision']['all']
    for label in labels:
        a[f'{function}_precision_{label}'] = a.get(f'{function}_precision_{label}',0) + metrics['precision'][label]
    a[f'{function}_correction_precision'] = a.get(f'{function}_correction_precision',0) + metrics['correction']['precision']
    a[f'{function}_correction_recall'] = a.get(f'{function}_correction_recall',0) + metrics['correction']['recall']
    a[f'{function}_subset'] = a.get(f'{function}_subset',0) + metrics['span_comparison']['subset']
    a[f'{function}_superset'] = a.get(f'{function}_superset',0) + metrics['span_comparison']['superset']
    a[f'{function}_partial_overlap'] = a.get(f'{function}_partial_overlap',0) + metrics['span_comparison']['partial_overlap']
    dict[key] = a
    return dict

# Looping through disagreement resolution rows
def get_disagreement_resolution(row,annotators):
    unit_id = row['_unit_id']
    worker_id = row['_worker_id']
    chunk_source = create_token_dict(row['chunk_source'],labels)
    chunk_anno = create_token_dict(row['chunk_anno'],labels)
    chunk_qa = create_token_dict(row['chunk_qa'],labels)
    print(f'Comparing unit {unit_id} from {worker_id} in job {ANNO_JOB} to 95% QA batch ({DISAGREEMENT_RESOLUTION}) and source...')
    # Annotator_vs_Disagreement_resolution 
    avdr_metrics = get_annotator_vs_review(chunk_anno,chunk_qa,chunk_source)
    annotators = accum_metrics(annotators,row['_worker_id'],labels,avdr_metrics,'disagreement_resolution')

# Looping through full review rows
def get_full_review(row,annotators,csvfr_agg):
    unit_id = row['_unit_id']
    worker_id = row['_worker_id']
    chunk_source = create_token_dict(row['chunk_source'],labels)
    chunk_anno = create_token_dict(row['chunk_anno'],labels)
    combined_chunk_anno = create_token_dict(row['combined_chunk_anno'],labels)
    chunk_qa = create_token_dict(row['chunk_qa'],labels)
    # Annotator_vs_Full_review 
    print(f'Comparing unit {unit_id} from {worker_id} in job {ANNO_JOB} to 5% QA batch ({FULL_REVIEW}) and source...')
    avfr_metrics = get_annotator_vs_review(chunk_anno,chunk_qa,chunk_source)
    annotators = accum_metrics(annotators,row['_worker_id'],labels,avfr_metrics,'full_review')
    # Client_system_vs_Full_review 
    csvfr_metrics = get_annotator_vs_review(chunk_source,chunk_qa,chunk_source)
    csvfr_agg = accum_metrics(csvfr_agg,'agg',labels,csvfr_metrics,'')

def normalize_metrics(dict):
    for key in dict:
        cts = {}
        for item in dict[key]:
            if '_ct' in item:
                cts[item] = dict[key][item]
        for ct in cts:
            function = ct.replace('_ct','')
            for item in dict[key]:
                if function in item and '_ct' not in item:
                    dict[key][item] = dict[key][item]/cts[ct]
    return dict 


def main():

    api_downloadReports([ ANNO_JOB, DISAGREEMENT_RESOLUTION, FULL_REVIEW ])

    df_a = pd.read_csv(f'f{ANNO_JOB}.csv')
    df_d = pd.read_csv(f'f{DISAGREEMENT_RESOLUTION}.csv')
    df_f = pd.read_csv(f'f{FULL_REVIEW}.csv')

    df_ad = pd.merge(df_a,df_d,suffixes=['','_qa'],on=['doc_id','chunk_id'])
    df_af = pd.merge(df_a,df_f,suffixes=['','_qa'],on=['doc_id','chunk_id'])

    df_ad = create_chunk_cols(df_ad)
    df_af = create_chunk_cols(df_af)

    # For Annotator Metrics Aggregation
    annotators = {}
    # For Client_system_vs_Full_review Average for all chunks by all annotators Aggregation
    csvfr_agg = {}
    
    df_ad.apply(lambda x:get_disagreement_resolution(x,annotators),axis=1)
        
    df_af.apply(lambda x:get_full_review(x,annotators,csvfr_agg),axis=1)
        
    annotators = normalize_metrics(annotators)

    df_annotators = pd.DataFrame.from_dict(annotators, orient='index').reset_index()
    df_annotators['_worker_id'] = df_annotators['index'] 
    df_annotators.drop(df_annotators.filter(regex='_ct|index').columns, axis=1, inplace=True)
    df_annotators.to_csv('annotator_metrics.csv',index=False)

if __name__ == '__main__':
    main()