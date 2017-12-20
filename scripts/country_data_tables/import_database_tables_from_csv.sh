#!/bin/bash

find -iname '*.csv' -exec pgfutter --db "db_dhs_global" --port "5433" --schema "public" --user "krishnab" --pass "3kl4vx71" --ignore-errors csv {} \;
