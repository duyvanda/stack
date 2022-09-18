#!/bin/bash
STATUS="$(airflow jobs check | head | cut -f 1 -d ' ')";
if [ "$STATUS" = "Found" ]; then
    echo "Service started";
    exit 0;
else
    echo "***Service NOT started*** --> $STATUS";  
    exit 1;
fi
exit;