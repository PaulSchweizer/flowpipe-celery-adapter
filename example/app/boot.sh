#!/bin/bash
celery -A app worker -l INFO
