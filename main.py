import os
from bs4 import BeautifulSoup
import requests
from multiprocessing.pool import ThreadPool
import argparse
import json
import urllib.parse

# argument -i input domain -o output file
parser = argparse.ArgumentParser(description='Web Crawler')
parser.add_argument('-i', '--input', help='Input config', required=True)
parser.add_argument('-t', '--threads', help='Number of threads', required=False)
args = parser.parse_args()

dir_path = os.path.dirname(os.path.realpath(__file__))

def fetch_html(url):
    try:
        response = requests.get(url)
        return response.text
    except Exception as e:
        print(f'Error fetching {url}')
        return None


def parse_page(url):
    url = urllib.parse.unquote(url)
    print(f'Parsing {url}')

    html_text = fetch_html(url)
    soup = BeautifulSoup(html_text, 'html.parser')

    all_texts = soup.get_text()

    title = soup.title.string if soup.title else None

    # get all image url
    images = [img for img in soup.find_all('img', src=True)]

    # extract all links
    links = [a['href'] for a in soup.find_all('a', href=True)]

    full_links = []
    for link in links:
        # remove query string
        link = link.split('?')[0]
        # remove hash
        link = link.split('#')[0]

        full_link = None
        # make full link (check if it is relative link) if not check if it is in the same domain
        if link.startswith('http'):
            if not link.startswith(target_domain):
                continue
            full_link = link
        elif link.startswith('/'):
            full_link = target_domain + link
        elif link.startswith('#'):
            continue
        elif link.startswith('./'):
            # parse parts
            parts = url.split('/')
            full_link = '/'.join(parts[:-1]) + link[1:]
        
        if full_link is not None:
            full_links.append(urllib.parse.unquote(full_link))

    return {
        'url': url,
        'title': title,
        'images': [image['src'] for image in images],
        'text': all_texts
    }, full_links


if __name__ == '__main__':
    # web crawling

    with open(args.input, 'r') as f:
        config = json.load(f)

    target_domain = config["target_domain"]
    output_file = config["output_file"] if 'output_file' in config else 'data.json'
    ignore_prefixes = config["ignore_prefixes"] if 'ignore_prefixes' in config else []

    threads = args.threads
    if threads is None:
        threads = 4
    else:
        threads = int(threads)

    # Create a queue within the context of the manager
    documents = {}
    processing_urls = [target_domain]
    with ThreadPool(processes=threads) as pool:
        while True:
            # get all queue and map
            results = pool.map(parse_page, processing_urls)

            if len(results) == 0:
                break

            new_link_set = set()
            for result, full_links in results:
                url = result['url']
                documents[url] = result
                for link_url in full_links:
                    if link_url not in documents and link_url not in new_link_set and not any([link_url.startswith(prefix) for prefix in ignore_prefixes]):
                        new_link_set.add(link_url)
            processing_urls = list(new_link_set)
        pool.close()

    # save to file
    with open(output_file, 'w') as f:
        json.dump(documents, f)


    if "tabular_data" in config:
        from parse_table import parse_table
        parse_table(config["tabular_data"], threads)
    else:
        print('Done')