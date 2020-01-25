#!/bin/bash

sudo apt-get remove docker docker-engine docker.io containerd runc
sudo apt-get update
sudo apt install \
    apt-transport-https \
    ca-certificates \
    curl \
    gnupg-agent \
    software-properties-common
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -
sudo add-apt-repository \
   "deb [arch=amd64] https://download.docker.com/linux/ubuntu \
   $(lsb_release -cs) \
   stable"
sudo apt-get update
sudo apt install docker-ce docker-ce-cli containerd.io
sudo apt install python-pip
pip install "celery[redis]" simple_salesforce flask pandas tqdm openpyxl apscheduler yaml
sudo docker run --name nirvana_redis -p 6379:6379 -d redis
sudo docker kill nirvana_redis