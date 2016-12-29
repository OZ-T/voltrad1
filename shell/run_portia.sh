#!/usr/bin/env bash
docker run -i -t --rm -v /home/david/portia/data:/app/slyd/data:rw -p 9001:9001 --name portia portia
