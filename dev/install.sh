#!/bin/bash
FOLDER=~/.ssh

if [ -d "$FOLDER" ]; then
    cp -rvp ~/.ssh ssh/
else
    echo "Directory ./ssh does not exist!"
fi
if [ -f "environment.yml" ]; then
    rm environment.yml
fi
if [ -f "requirements.txt" ]; then
    rm requirements.txt
fi

cp -rvp ../Icesat2/environment.yml .
cp -rvp ../Icesat2/requirements.txt .

docker-compose build 

docker-compose up -d 