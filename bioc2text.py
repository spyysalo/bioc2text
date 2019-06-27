#!/usr/bin/env python3

import sys
import os
import tarfile

import xml.etree.ElementTree as ET

from random import random
from contextlib import closing
from logging import warning


# Passage types to exclude from output. These are chosen to
# exclude material that could break the flow of text.
EXCLUDED_PASSAGE_TYPE = set([
    'ref',
    'table',
    'table_caption',
    'table_caption_title',
    'table_footnote',
    'table_footnote_title',
    'table_title',
    'fig',
    'fig_caption',
    'fig_caption_title',
    'fig_footnote',
    'footnote',
    'footnote_title',
])


# Section types to exclude from output, chosen as above.
EXCLUDED_SECTION_TYPE = set([
    'REF',
    'FIG',
    'TABLE'
])


def argparser():
    from argparse import ArgumentParser
    ap = ArgumentParser()
    ap.add_argument('-l', '--limit', default=None, type=int,
                    help='maximum number of documents to process')
    ap.add_argument('-m', '--min-length', metavar='INT', default=None, type=int,
                    help='minimum document length (characters) to output')
    ap.add_argument('-r', '--random', metavar='RATIO', default=None,
                    type=float, help='process random ratio of documents')
    ap.add_argument('file', nargs='+', help='BioC data')
    return ap


def find_only(element, match):
    """Return the only matching child of the given element.

    Raise KeyError if no match and FormatError if multiple matches.
    """
    found = element.findall(match)
    if not found:
        raise KeyError('Error: expected 1 %s, got %d' % (match, len(found)))
    elif len(found) > 1:
        raise FormatError('Error: expected 1 %s, got %d' % (match, len(found)))
    else:
        return found[0]


def is_tar_gz(path):
    return path.endswith('.tar.gz') or path.endswith('.tgz')


def is_xml_gz(path):
    return path.endswith('.xml.gz')


def inner_text(element):
    return ''.join(element.itertext())


def get_section_type(passage):
    types = []
    for infon in passage.findall('infon'):
        if infon.attrib.get('key') == 'section_type':
            types.append(infon.text)
    if not types:
        return None
    elif len(types) > 1:
        warning('multiple section types: {}'.format(types))
    return types[0]


def get_passage_type(passage):
    types = []
    for infon in passage.findall('infon'):
        if infon.attrib.get('key') == 'type':
            types.append(infon.text)
    if not types:
        return None
    elif len(types) > 1:
        warning('multiple passage types: {}'.format(types))
    return types[0]


def process_passage(passage, document, options):
    texts = []
    if get_passage_type(passage) in EXCLUDED_PASSAGE_TYPE:
        return []
    elif get_section_type(passage) in EXCLUDED_SECTION_TYPE:
        return []
    for text in passage.findall('.//text'):
        texts.append(inner_text(text))
    texts = [t for t in texts if t and not t.isspace()]
    return texts


def process_document(document, options, count):
    if options.random is not None and random() > options.random:
        return 0
    doc_id = find_only(document, 'id').text
    texts = []
    for element in document:
        if element.tag == 'passage':
            texts.extend(process_passage(element, document, options))
        elif element.tag == 'infon':
            pass    # could be license etc.
        elif element.tag == 'id':
            pass
        else:
            warning('unexpected tag in document {}: {}'.format(
                element.tag, doc_id))
    doc_text = '\n'.join(texts)
    if options.min_length is not None and len(doc_text) < options.min_length:
        return 0
    if count != 0:
        print()    # blank line separates documents
    print(doc_text)
    return 1


def process_stream(stream, name, options, count=0):
    for event, element in stream:
        if event != 'end' or element.tag != 'document':
            continue
        count += process_document(element, options, count)
        if options.limit is not None and count >= options.limit:
            break
        element.clear()
    return count


def process_tar_gz(path, options):
    count = 0
    with tarfile.open(path, 'r:gz') as tar:
        for member in tar:
            if not member.isfile():
                continue
            ext = os.path.splitext(member.name)[1]
            if not ext == '.xml':
                continue
            with closing(tar.extractfile(member)) as xml:
                count = process_stream(ET.iterparse(xml), member.name, options,
                                       count)
            if options.limit is not None and count >= options.limit:
                break
    return count


def process(path, options):
    if is_tar_gz(path):
        return process_tar_gz(path, options)
    elif is_xml_gz(path):
        with gzip.GzipFile(path) as gzfile:
            return process_stream(ET.iterparse(gzfile), path, options)
    else:
        # assume XML
        return process_stream(ET.iterparse(path), path, options)


def main(argv):
    args = argparser().parse_args(argv[1:])
    count = 0
    for path in args.file:
        count += process(path, args)
        if args.limit is not None and count >= args.limit:
            break
    print('done, processed {} documents'.format(count), file=sys.stderr)
    return 0


if __name__ == '__main__':
    sys.exit(main(sys.argv))
