#!/bin/bash

source $HOME/miniconda-recent/bin/activate

python /home/savchenk/oda-bot/odabot/cli.py update-workflows --loop 60
