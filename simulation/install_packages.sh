#!/bin/bash
sudo python3 -m pip install --upgrade timezonefinder pytz 'apache-beam[gcp]'
sudo python3 -m  pip install --upgrade google-cloud-pubsub

# maybe needed to install also
sudo python3 -m  pip install google-apitools