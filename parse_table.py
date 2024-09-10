import os
from bs4 import BeautifulSoup
import requests
from multiprocessing.pool import ThreadPool
import argparse
import json
import csv
import hashlib
import uuid

# argument -i input domain -o output file
parser = argparse.ArgumentParser(description='Web Crawler')
parser.add_argument('-i', '--input', help='Input data', required=False)
parser.add_argument('-o', '--output', help='Output file', required=False)
parser.add_argument('-t', '--threads', help='Number of threads', required=False)
args = parser.parse_args()

dir_path = os.path.dirname(os.path.realpath(__file__))


def parse_item(tuple):
    i, record = tuple
    url = record['url'] if 'url' in record else f"https://km.lab.ai/index.php/%E0%B9%80%E0%B8%AA%E0%B8%99%E0%B8%AD%E0%B9%81%E0%B8%99%E0%B8%B0%E0%B8%9A%E0%B8%97%E0%B8%84%E0%B8%A7%E0%B8%B2%E0%B8%A1"
    title = record['title'].strip() if 'title' in record else record['คำถาม'] if 'คำถาม' in record else 'ทั่วไป'
    text = record['text'].strip() if 'text' in record else record['คำตอบ'] if 'คำตอบ' in record else ''
    return {
        'url': url,
        'title': title,
        'text': title + ' ' + text
    }


if __name__ == '__main__':
    # web crawling

    input_file = args.input
    if input_file is None:
        input_file = 'input.csv'

    output_file = args.output
    if output_file is None:
        output_file = 'input.csv.json'

    threads = args.threads
    if threads is None:
        threads = 4
    else:
        threads = int(threads)


    with open(input_file, 'r') as f:
        reader = csv.DictReader(f)
        input_records = list(reader)
        

    # Create a queue within the context of the manager
    documents = {}
    with ThreadPool(processes=threads) as pool:
        results = pool.map(parse_item, zip(range(len(input_records)), input_records))
        
        for result in results:
            # hash question for id
            id = hashlib.md5(result['title'].encode()).hexdigest()
            documents[id] = result
            
        pool.close()


    # save to file
    with open(output_file, 'w') as f:
        json.dump(documents, f)

    print('Done')