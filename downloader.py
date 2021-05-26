"""Downloads Wikipedia data dumps.
Functions:
    download_sql_dump: Downloads and decompress a Wikipedia SQL dump.
    get_dataframe: Builds a pandas.DataFrame from a Wikipedia SQL dump.
"""

from bs4 import BeautifulSoup
import json
import os
import requests
from tqdm import tqdm

BASE_URL = "https://dumps.wikimedia.org"

# Return the filename of a .sql file
def _get_name(language, dump, file):
    return "{}wiki-{}-{}.xml".format(language, dump, file)

# Return the url where a .sql.gz file is located
def _get_url(language, dump, file):
    dump_base_url = f"{language}wiki/{dump}"
    filename = _get_name(language, dump, file)
    return "/{dump_base_url}/{filename}.bz2"

def load_dump_status(dump):
    with open(f'status-{dump}.json') as f:
        return json.loads(f.read())

def save_dump_status(j, dump):
    with open(f'status-{dump}.json', 'w') as f:
        f.write(json.dumps(j))

def get_latest_dump():
    r = requests.get('https://dumps.wikimedia.org/enwiki/')
    soup = BeautifulSoup(r.text, "html.parser")
    hrefs = [a.get('href') for a in soup.find_all('a')]
    hrefs = [href for href in hrefs if href not in ['../', 'latest/']]
    dump = list(sorted(hrefs))[-1]
    return dump.replace('/', '')

def search_dump_files(s, dump='20210501'):
    try:
        j = load_dump_status(dump)
    except:
        try:
            r = requests.get(f'{BASE_URL}/enwiki/{dump}/dumpstatus.json')
            j = r.json()
            try:
                save_dump_status(j, dump)
            except: pass
        except Exception as e:
            print('failed on dump', dump, e, j, j.text)
    
    jobs = [k for k in j['jobs'].keys()]
    matches = []
    for k in jobs:
        if 'files' in j['jobs'][k]:
            for fname in j['jobs'][k]['files']:
                if s in fname:
                    matches.append(j['jobs'][k]['files'][fname])
    return matches

# NOTE: I rewrote this method to use tqdm
def download_sql_dump(language='en', file='pages-articles-multistream', dump="latest", target_dir="."):
    """Downloads and decompresses a Wikipedia SQL dump.
    Args:
        language: Wikipedia name (language code).
        file: File name.
        dump: Dump version.
        target_dir: Target directory.
    """
    fname = os.path.join(target_dir, _get_name(language, dump, file))
    url = _get_url(language, dump, file)
    download_wikipedia_file(BASE_URL + url, fname)

def download_wikipedia_file(url, fname=None):
    if not fname:
        fname = url.split('/')[-1]
    if url[0] != '/':
        url = '/'+url
    print('requesting', url, 'and saving to', fname)
    r = requests.get(BASE_URL + url, stream=True)
    print("streaming request:", r)
    total_size = int(r.headers["Content-Length"])
    print('downloading', total_size, 'bytes to', fname)
    with tqdm.wrapattr(open(fname, 'wb'), 'write', miniters=1, 
                       total=int(r.headers.get('content-length', 0))) as fout:
        for chunk in r.iter_content(chunk_size=4096):
            fout.write(chunk)
    print('done')
    return fname

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='Search for and download Wikipedia dump files.')
    helptext = '''
Possible commands:
    - download <url> <filename>
    - search <string>: returns all filenames containing the string
'''
    parser.add_argument('command', choices=['download', 'help', 'latest', 'search'], help=helptext, nargs=1)
    parser.add_argument('params', nargs="*")
    
    args = parser.parse_args()
    # TODO: less-awkward argparse syntax 
    if args.command[0] == 'search':
        print(args.params)
        results = search_dump_files(args.params[0])
        max_len = max([len(item['url']) for item in results])
        for item in results:
            print(f'{item["url"]:100} {round(int(item["size"])/1_000_000):8} MB')
    if args.command[0] == 'download':
        download_wikipedia_file(*args.params)
    if args.command[0] == 'help':
        print(helptext)
    if args.command[0] == 'latest':
        print('latest:', get_latest_dump())