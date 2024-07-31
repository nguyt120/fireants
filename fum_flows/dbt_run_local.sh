#!/bin/bash
selected=$1
branch=$(git symbolic-ref HEAD | sed -e 's,.*/\(.*\),\1,')
echo $branch
if [ -z $selected ];
then
    dbt run --vars "branch: $branch"
else
    dbt run --select $selected --vars "branch: $branch"
fi