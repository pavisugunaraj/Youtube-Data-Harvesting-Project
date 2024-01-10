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



