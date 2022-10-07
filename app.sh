#!/bin/bash

set -xe

source $HOME/miniconda-recent/bin/activate

TDIR=$(mktemp -d)

virtualenv $TDIR

source $TDIR/bin/activate

pip install oda-knowledge-base[rdf,cwl,notebook] requests logging_tree

python /home/savchenk/oda-bot/odabot/cli.py poll-github-events

rm -rf $TDIR
