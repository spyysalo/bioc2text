#!/usr/bin/env python3

# If imports fail, try installing scispacy and the model:
# pip3 install scispacy
# pip3 install https://s3-us-west-2.amazonaws.com/ai2-s2-scispacy/releases/v0.2.0/en_core_sci_md-0.2.0.tar.gz

import sys
import os
import re

import scispacy
import spacy

from sspostproc import refine_split


DOC_ID_RE = re.compile(r'^-+ DOC-ID: ".*" -+$')


pipeline = spacy.load('en_core_sci_md', disable=['parser', 'tagger', 'ner'])
pipeline.add_pipe(pipeline.create_pipe('sentencizer'))


def argparser():
    from argparse import ArgumentParser
    ap = ArgumentParser()
    ap.add_argument('text', nargs='+')
    return ap


def sentences(passage, refine=True):
    split = []
    if not passage.endswith('\n'):
        passage += '\n'    # spacy needs the newline
    analyzed = pipeline(passage)
    for sentence in analyzed.sents:
        text = str(sentence)
        if text and not text.isspace():
            split.append(text.rstrip('\n'))
    if refine:
        split = refine_split('\n'.join(split)).split('\n')
    return split


def main(argv):
    args = argparser().parse_args(argv[1:])
    for path in args.text:
        with open(path) as f:
            for ln, l in enumerate(f, start=1):
                l = l.rstrip()
                if DOC_ID_RE.match(l):
                    print(l)
                elif not l or l.isspace():
                    print(l)
                else:
                    for s in sentences(l):
                        print(s)
    return 0


if __name__ == '__main__':
    sys.exit(main(sys.argv))
