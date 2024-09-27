import os
from bs4 import BeautifulSoup
import requests
from multiprocessing.pool import ThreadPool
import json
import csv
import hashlib
import uuid
import functools

dir_path = os.path.dirname(os.path.realpath(__file__))

def parse_item(default_url, tuple):
    i, record = tuple
    url = record['url'] if 'url' in record else default_url
    title = record['title'].strip() if 'title' in record else record['คำถาม'] if 'คำถาม' in record else 'ทั่วไป'
    text = record['text'].strip() if 'text' in record else record['คำตอบ'] if 'คำตอบ' in record else ''
    return {
        'url': url,
        'title': title,
        'text': title + ' ' + text
    }


def parse_table(config, threads=None):

    input_file = config['input_file'] if 'input_file' in config else "input.csv"
    output_file = config['output_file'] if 'output_file' in config else "input.csv.json"

    if threads is None:
        threads = 4
    else:
        threads = int(threads)

    with open(input_file, 'r') as f:
        reader = csv.DictReader(f)
        input_records = list(reader)

    print(f'Parsing {len(input_records)} records')
        
    augmented_parse_item = functools.partial(parse_item, config['default_url'])

    # Create a queue within the context of the manager
    documents = {}
    with ThreadPool(processes=threads) as pool:
        results = pool.map(augmented_parse_item, zip(range(len(input_records)), input_records))
        
        for result in results:
            # hash question for id
            id = hashlib.md5(result['title'].encode()).hexdigest()
            documents[id] = result
            
        pool.close()

    # save to file
    with open(output_file, 'w') as f:
        json.dump(documents, f)

    print('Done')