'''
Allows iteration over a Wikipedia XML dump file (uncompressed)
'''

TEST_FILE='./wiki_sample.xml'
TOTAL_PAGES = 19_000_000

def make_string(l):
    return ''.join(l)

# copied from indexer.py, but changing bytes to strings
def iterdump(xml_filename=TEST_FILE, limit=0):
    new_xml = []
    current_page = 0
    with open(xml_filename, mode='r', encoding='utf=8') as f:
        for line in f:
            if '<page>' == line:
                current_page += 1
                yield current_page, make_string(new_xml)
                new_xml = []
            elif '  <page>\n' == line:
                current_page += 1
                yield current_page, make_string(new_xml)
                new_xml = []
            elif '<page>' in line:
                current_page += 1
                yield current_page, make_string(new_xml)
                new_xml = []
            if limit > 0 and limit <= current_page:
                break
            new_xml.append(line)

def iterdump_indices(xml_filename=TEST_FILE, current_page=0, limit=0):
    start_idx = 0
    end_idx = 0
    current_page = 0
    with open(xml_filename, 'rb') as f:
        for line in f:
            if b'<page>' in line:
#                 if start_idx == 0:
#                     start_idx = end_idx
#                     end_idx = end_idx + len(line)
#                     continue
                current_page += 1
                yield start_idx, end_idx + line.index(b'<page>')
                start_idx = end_idx + line.index(b'<page>')
                end_idx += len(line)
                if limit > 0 and current_page >= limit:
                    break
#                 f.seek(end_idx)
            else:
                end_idx += len(line)
        yield start_idx, end_idx

def read_page(start_idx, end_idx, xml_filename=TEST_FILE):
    with open(xml_filename, 'rb') as f:
        f.seek(start_idx)
        return f.read(end_idx - start_idx).decode('utf-8')

class Unbuffered(object):
   def __init__(self, stream):
       self.stream = stream
   def write(self, data):
       self.stream.write(data)
       self.stream.flush()
   def writelines(self, datas):
       self.stream.writelines(datas)
       self.stream.flush()
   def __getattr__(self, attr):
       return getattr(self.stream, attr)

import sys
sys.stdout = Unbuffered(sys.stdout)

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='Iterate over the contents of a Wikipedia dump file.')
    parser.add_argument('dumpfile', nargs='?')
    args = parser.parse_args()

    if args.dumpfile:
        print(args.dumpfile)
    else:
        import os
        dumpfiles = [fname for fname in os.listdir('.') if any(s in fname for s in ('.sql', '.xml'))]
        print("possible dumpfiles:", dumpfiles)