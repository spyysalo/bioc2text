#!/usr/bin/env python3

import sys
import os

import xml.etree.ElementTree as ET

from logging import warning


def argparser():
    from argparse import ArgumentParser
    ap = ArgumentParser()
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


def process_passage(passage, document, options):
    if get_section_type(passage) == 'REF':
        return
    for text in passage.findall('.//text'):
        print(inner_text(text))


def process_document(document, options):
    doc_id = find_only(document, 'id').text
    for element in document:
        if element.tag == 'passage':
            process_passage(element, document, options)
        elif element.tag == 'infon':
            pass    # could be license etc.
        elif element.tag == 'id':
            pass
        else:
            warning('unexpected tag in document {}: {}'.format(
                element.tag, doc_id))


def process_stream(stream, name, options):
    for event, element in stream:
        if event != 'end' or element.tag != 'document':
            continue
        process_document(element, options)
        element.clear()


def process(path, options):
    if is_tar_gz(path):
        raise NotImplementedError
    elif is_xml_gz(path):
        with gzip.GzipFile(path) as gzfile:
            return process_stream(ET.iterparse(gzfile), path, options)
    else:
        # assume XML
        return process_stream(ET.iterparse(path), path, options)


def main(argv):
    args = argparser().parse_args(argv[1:])
    for path in args.file:
        process(path, args)
    return 0


if __name__ == '__main__':
    sys.exit(main(sys.argv))
