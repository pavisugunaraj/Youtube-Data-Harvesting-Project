YOUTUBE DATA SCRAPPING

A Brief description about this project

<img width="960" alt="file1" src="https://github.com/pavisugunaraj/Youtube-Data-Harvesting-Project/assets/156047936/cf6855b0-17f1-4b92-8007-cf73be11cc89">

DEPLOYMENT

To dploy this project run

pip install streamlit

pip install google-api-python-client

pip install pymongo

pip install pandas

pip install psycopg2

ENVIRONMENT LIBRARIES

from googleapiclient.discovery import build

import psycopg2

import pymongo

import pandas as pd

import streamlit as st

MONGODB DATABASE

The data scrapped from youtube is pushed to the Mongodb database and it will be in JSON format

POSTGRES SQL

The unstructured data from Mongodb is taken and it is converted into a structured format using postgres sql

ANALYSIS

Here, we have set of queries to analysis the data which is in the table format

SCREENSHOTS

TO INSERT A CHANNEL ID:

Here we have to give the channel id and click "collect and store details" button

After the details stored in the Mongodb, it will show the message as "details inserted successfully" 

<img width="960" alt="file2" src="https://github.com/pavisugunaraj/Youtube-Data-Harvesting-Project/assets/156047936/380d139a-479b-462b-9b32-f0f6966c9131">

TO INSERT THE DETAILS IN SQL

In order the store the data in table format, we have to click "Migrate to sql"

Here the data will be stored in SQL and it will show the message as "table created succesfully"

<img width="952" alt="file3" src="https://github.com/pavisugunaraj/Youtube-Data-Harvesting-Project/assets/156047936/88745a05-d904-4bef-bbac-b8c500024054">

Totally there are 3 tables Channels,Videos and comments

<img width="960" alt="file4" src="https://github.com/pavisugunaraj/Youtube-Data-Harvesting-Project/assets/156047936/4697158e-45d7-4f33-9059-bb8e2f789719">

<img width="960" alt="file5" src="https://github.com/pavisugunaraj/Youtube-Data-Harvesting-Project/assets/156047936/7d241f19-fa36-48fd-be1f-8e41e3ff01a3">

<img width="953" alt="file6" src="https://github.com/pavisugunaraj/Youtube-Data-Harvesting-Project/assets/156047936/28b5c695-656a-4f14-aa08-0d7cd1e3fbb9">

IN ANALYSIS,there are set of queries. If we click the queries it will show the answers for that particular queries

<img width="960" alt="file7" src="https://github.com/pavisugunaraj/Youtube-Data-Harvesting-Project/assets/156047936/ce47b1c1-e64d-4cec-9d0c-68e831a8a3f1">

<img width="920" alt="file8" src="https://github.com/pavisugunaraj/Youtube-Data-Harvesting-Project/assets/156047936/27369600-1547-49b5-950c-5c2a8ae29b3a">



