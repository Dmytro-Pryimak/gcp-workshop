#!/bin/bash
python3 simulate.py --project $(gcloud config get-value project) --startTime '2015-02-01 00:00:00 UTC' --endTime '2015-03-01 00:00:00 UTC' --speedFactor 5000
