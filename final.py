#Installation of reuired libraries
from googleapiclient.discovery import build
import psycopg2
import pymongo
import pandas as pd
import streamlit as st
#Getting of API key
api_key="AIzaSyDQ_K_xEi6XiiNChjXGTWJIF_oS4wRVG08"
youtube = build("youtube", "v3", developerKey=api_key)
#getting channel details with channel id
def get_channel_details(ch_id):
  
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=ch_id
    )
    response = request.execute()
    channel_details = dict(channel_name=response['items'][0]['snippet']['title'],
                         channel_id=response['items'][0]['id'],
                        description=response['items'][0]['snippet']['description'],
                        overall_id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads'],
                        subscriber_count=response['items'][0]['statistics']['subscriberCount'],
                        video_count=response['items'][0]['statistics']['videoCount'],
                        view_count=response['items'][0]['statistics']['viewCount'],
                        joined_at=response['items'][0]['snippet']['publishedAt'])
    return channel_details
#Generating all video ids for particular channel 
def get_video_ids(ch_id):
    video_ids=[]
    response1=youtube.channels().list(part="contentDetails",
                                     id=ch_id).execute()
    
    playlist_id=response1['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    next_page_token=None

    while True:
        
        response2=youtube.playlistItems().list(
                                                part='snippet',
                                                playlistId=playlist_id,
                                                maxResults=50,
                                                pageToken=next_page_token).execute()

        for i in range(len(response2['items'])):
            video_ids.append(response2['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token=response2.get('nextPageToken')

        if next_page_token is None:
            break
    
    return video_ids
#By getting video ids from the above function, here we are getting all the video datas
def get_video_info(video_ids):
  video_data=[]
  for video_id in video_ids:
    request=youtube.videos().list(
        part="snippet,contentDetails,statistics",
        id=video_id
    )
    response=request.execute()
    for items in response['items']:
      data=dict(channel_name=items['snippet']['channelTitle'],
                vid_title=items['snippet']['title'],
                vid_id=items['id'],
                vid_description=items['snippet']['description'],
                likes=items['statistics'].get('likeCount'),
                views=items['statistics']['viewCount'],
                comment_count=items['statistics'].get('commentCount'),
                vid_duration=convert_dur(items['contentDetails']['duration']),
                vid_publishedAt=items['snippet']['publishedAt']
                )
      video_data.append(data)
  return video_data
# In order to get the duration in correct format, we are using this function
def convert_dur(s):
    l=[]
    f=''
    for i in s:
        if i.isnumeric():
            f=f+i
        else:
            if f:
                l.append(f)
                f=''
    if 'H' not in s:
        l.insert(0,'00')
    if 'M' not in s:
        l.insert(1,'00')
    if 'S' not in s:
        l.insert(-1,'00')
    return ':'.join(l)
#after getting video ids we are generating all the comments related to that particular videos 
def get_comment_info(video_ids):

    comments = []
    try:
        for video_id in video_ids:                                          

            request = youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                maxResults=50
            )
            response = request.execute()

            for item in response['items']:                           
                data=dict(comment_id=item['snippet']['topLevelComment']['id'],
                            vid_id=item['snippet']['topLevelComment']['snippet']['videoId'],
                            comment_text=item['snippet']['topLevelComment']['snippet']['textOriginal'],
                            comment_author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            publishedAt=item['snippet']['topLevelComment']['snippet']['publishedAt'])
                comments.append(data)
    except:
        pass

    return comments
#connection with MongoDB
client=pymongo.MongoClient("mongodb+srv://pavisugunaraj:devmazhilan@cluster0.jz3hjen.mongodb.net/?retryWrites=true&w=majority")
db=client.Project
collection=db.data
#Here we are passing all the functions details to MongoDB and we are inserting all the information to MongoDB
def channel_info(ch_id):
    Channel_details=get_channel_details(ch_id)
    Video_Ids=get_video_ids(ch_id)
    Video_details=get_video_info(Video_Ids)
    Comment_details=get_comment_info(Video_Ids)
    
    data={
        "channel_info":Channel_details,
        "video_info":Video_details,
        "comment_info":Comment_details
    }
    db.Project.insert_one(data)
    return "details inserted successfully"
#Creation of Channel Table
def channel_table():
        mydb=psycopg2.connect(host="localhost",
                                user="postgres",
                                password="dev@0905",
                                database="project_data",
                                port="5432"
                                )
        cursor=mydb.cursor()

        drop_query='''drop table if exists channels_info'''
        cursor.execute(drop_query)
        mydb.commit()
        try:
                
            create_query= '''create table if not exists channels_info (channel_name varchar(100),
                                                                channel_id varchar(100) primary key,
                                                                description text,
                                                                overall_id varchar(100),
                                                                subscriber_count int,
                                                                video_count int,
                                                                view_count int,
                                                                joined_at date
                                                                )'''
            cursor.execute(create_query)
            mydb.commit()
        except:
            print("Channel table already created")


        ch_list=[]
        db=client.Project
        collection=db.Project
        for ch_data in collection.find({},{"_id":0,"channel_info":1}):
                ch_list.append((ch_data["channel_info"]))
        df=pd.DataFrame(ch_list)

        for index,row in df.iterrows():
            insert_query=''' insert into channels_info (channel_name,
                                                    channel_id,
                                                    description,
                                                    overall_id,
                                                    subscriber_count,
                                                    video_count,
                                                    view_count,
                                                    joined_at)
                                                    values(%s,%s,%s,%s,%s,%s,%s,%s)'''
                                                    
            values=(row['channel_name'],
                    row['channel_id'],
                    row['description'],
                    row['overall_id'],
                    row['subscriber_count'],
                    row['video_count'],
                    row['view_count'],
                    row['joined_at'])
                    
            try:
                    cursor.execute(insert_query,values)
                    mydb.commit()
            except:
                    print("channel_values are inserted already")
#Creation of Video Table
def video_table():
    
    mydb=psycopg2.connect(host="localhost",
                            user="postgres",
                            password="dev@0905",
                            database="project_data",
                            port="5432"
                            )
    cursor=mydb.cursor()

    drop_query='''drop table if exists video_info'''
    cursor.execute(drop_query)
    mydb.commit()

    video_query= '''create table if not exists video_info (channel_name varchar(100),
                                                                vid_title varchar(100),
                                                                vid_id varchar(50),
                                                                vid_description text,
                                                                likes int,
                                                                views int,
                                                                comment_count int,
                                                                vid_duration text,
                                                                vid_publishedAt timestamp)
                                                                '''
    cursor.execute(video_query)
    mydb.commit() 

    vid_list=[]
    db=client.Project
    collection=db.Project
    for vid_data in collection.find({},{"_id":0,"video_info":1}):
        for i in range(len(vid_data['video_info'])):
            vid_list.append(vid_data["video_info"][i])
    df1=pd.DataFrame(vid_list)

    for index,row in df1.iterrows():
            insert_query=''' insert into video_info (  channel_name,
                                                            vid_title,
                                                            vid_id,
                                                            vid_description,
                                                            likes,
                                                            views,
                                                            comment_count,
                                                            vid_duration,
                                                            vid_publishedAt)
                                                    values(%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
                                                    
            values=(row['channel_name'],
                    row['vid_title'],
                    row['vid_id'],
                    row['vid_description'],
                    row['likes'],
                    row['views'],
                    row['comment_count'],
                    row['vid_duration'],
                    row['vid_publishedAt']
                    )
                    

            cursor.execute(insert_query,values)
            mydb.commit()
        
#Creation of Comment Table
def comment_table():
    mydb=psycopg2.connect(host="localhost",
                            user="postgres",
                            password="dev@0905",
                            database="project_data",
                            port="5432"
                            )
    cursor=mydb.cursor()

    drop_query='''drop table if exists comment_info'''
    cursor.execute(drop_query)
    mydb.commit()

    comment_query= '''create table if not exists comment_info (comment_id varchar(40) primary key,
                                                                vid_id varchar(50),
                                                                comment_text text,
                                                                comment_author varchar(100),
                                                                publishedAt timestamp)'''
                                                                
    cursor.execute(comment_query)
    mydb.commit()

    cmt_list=[]
    db=client.Project
    collection=db.Project
    for cmt_data in collection.find({},{"_id":0,"comment_info":1}):
        for i in range(len(cmt_data['comment_info'])):
            cmt_list.append(cmt_data["comment_info"][i])
    df2=pd.DataFrame(cmt_list)
        
    for index,row in df2.iterrows():
            insert_query=''' insert into comment_info (comment_id,
                                                    vid_id,
                                                    comment_text,
                                                    comment_author,
                                                    publishedAt)
                                                    values(%s,%s,%s,%s,%s)'''
                                                    
            values=(row['comment_id'],
                    row['vid_id'],
                    row['comment_text'],
                    row['comment_author'],
                    row['publishedAt']
                    
                    )
                    

            cursor.execute(insert_query,values)
            mydb.commit()  
        
#Table creation    
def table():
    channel_table()
    video_table()
    comment_table()
    return "table created successfully"

def show_channel_table():
    ch_list=[]
    db=client.Project
    collection=db.Project
    for ch_data in collection.find({},{"_id":0,"channel_info":1}):
        ch_list.append((ch_data["channel_info"]))
    df=st.dataframe(ch_list)

    return df

def show_video_table():
    vid_list=[]
    db=client.Project
    collection=db.Project
    for vid_data in collection.find({},{"_id":0,"video_info":1}):
        for i in range(len(vid_data['video_info'])):
            vid_list.append(vid_data["video_info"][i])
    df1=st.dataframe(vid_list)

    return df1

def show_comment_table():
    cmt_list=[]
    db=client.Project
    collection=db.Project
    for cmt_data in collection.find({},{"_id":0,"comment_info":1}):
        for i in range(len(cmt_data['comment_info'])):
            cmt_list.append(cmt_data["comment_info"][i])
    df2=st.dataframe(cmt_list)

    return df2

#Streamlite Code
st.title(":blue[YOUTUBE DATA HARVESTING AND WAREHOUSING]")
    
channel_id=st.text_input("Enter the channel ID")

if st.button("collect and store detials"):
    ch_ids=[]
    db=client.Project
    collection=db.Project
    for ch_data in collection.find({},{"_id":0,"channel_info":1}):
        ch_ids.append(ch_data['channel_info']['channel_id'])

    if channel_id in ch_ids:
        st.success("Channel Details of the given details already exists")
    else:
        insert=channel_info(channel_id)
        st.success(insert)

if st.button("Migrate to SQL"):
    Tables=table()
    st.success(Tables)

show_table=st.radio("SELECT THE TABLE FOR VIEW",("CHANNELS","VIDEOS", "COMMENTS"))

if show_table=="CHANNELS":
    show_channel_table()

elif show_table=="VIDEOS":
    show_video_table()

elif show_table=="COMMENTS":
    show_comment_table()

mydb=psycopg2.connect(host="localhost",
                    user="postgres",
                    password="dev@0905",
                    database="project_data",
                    port="5432"
                    )
cursor=mydb.cursor()

question=st.selectbox("SELECT YOUR QUESTION",("1.What are the names of all the videos and their corresponding channels?",
                                            "2.Which channels have the most number of videos, and how many videos do they have?",
                                            "3.What are the top 10 most viewed videos and their respective channels?",
                                            "4.How many comments were made on each video, and what are their  corresponding video names?",
                                            "5.Which videos have the highest number of likes, and what are their  corresponding channel names?",
                                            "6.What is the total number of likes  for each video, and what are their corresponding video names?",
                                            "7.What is the total number of views for each channel, and what are their  corresponding channel names?",  
                                            "8.What are the names of all the channels that have published videos in the year2022?",   
                                            "9.What is the average duration of all videos in each channel, and what are their corresponding channel names?",   
                                            "10.Which videos have the highest number of comments, and what are their corressponding channel names?"))  

if question=="1.What are the names of all the videos and their corresponding channels?":
    query1='''select channel_name,vid_title from video_info'''
    cursor.execute(query1)
    mydb.commit()
    t1=cursor.fetchall()
    df=pd.DataFrame(t1,columns=['Channel_Name','Video_Name'])
    st.write(df)

elif question=="2.Which channels have the most number of videos, and how many videos do they have?":
    query2='''select channel_name,video_count from channels_info order by video_count desc '''
    cursor.execute(query2)
    mydb.commit()
    t2=cursor.fetchall()
    df1=pd.DataFrame(t2,columns=['Channel_Name','No of Videos'])
    st.write(df1)

elif question=="3.What are the top 10 most viewed videos and their respective channels?":
    query3='''select channel_name,vid_title,views from video_info order by views desc limit 10'''
    cursor.execute(query3)
    mydb.commit()
    t3=cursor.fetchall()
    df3=pd.DataFrame(t3,columns=['Channel_Name','Video_Title','View_Count'])
    st.write(df3)

elif question=="4.How many comments were made on each video, and what are their  corresponding video names?":
    query4='''select vid_title,comment_count from video_info'''
    cursor.execute(query4)
    mydb.commit()
    t4=cursor.fetchall()
    df4=pd.DataFrame(t4,columns=['Video_Title','Comment_Count'])
    st.write(df4)

elif question=="5.Which videos have the highest number of likes, and what are their  corresponding channel names?":
    query5='''select channel_name,vid_title,likes from video_info where likes is not null order by likes desc'''
    cursor.execute(query5)
    mydb.commit()
    t5=cursor.fetchall()
    df5=pd.DataFrame(t5,columns=['Channel_Name','Video_Title','Like_Count'])
    st.write(df5)

elif question=="6.What is the total number of likes  for each video, and what are their corresponding video names?":
    query6='''select vid_title,likes from video_info'''
    cursor.execute(query6)
    mydb.commit()
    t6=cursor.fetchall()
    df6=pd.DataFrame(t6,columns=['Video_Title','Like_Count'])
    st.write(df6)

elif question=="7.What is the total number of views for each channel, and what are their  corresponding channel names?":
    query7='''select channel_name,view_count from channels_info '''
    cursor.execute(query7)
    mydb.commit()
    t7=cursor.fetchall()
    df7=pd.DataFrame(t7,columns=['Channel_Name','View_Count'])
    st.write(df7)

elif question=="8.What are the names of all the channels that have published videos in the year2022?":
    query8='''select channel_name,vid_title,vid_publishedat from video_info where extract(year from vid_publishedat)=2022'''
    cursor.execute(query8)
    mydb.commit()
    t8=cursor.fetchall()
    df8=pd.DataFrame(t8,columns=['Channel_Name','Video_Title','Published at 2022'])
    st.write(df8)

elif question=="9.What is the average duration of all videos in each channel, and what are their corresponding channel names?":
    query9='''select AVG(extract(epoch from vid_duration::interval))/60 as dur_minutes,channel_name from video_info group by channel_name'''
    cursor.execute(query9)
    mydb.commit()
    t9=cursor.fetchall()
    df9=pd.DataFrame(t9,columns=['Video Duration','Channel Name'])
    st.write(df9)

elif question=="10.Which videos have the highest number of comments, and what are their corressponding channel names?":

    query10='''select channel_name,vid_title,comment_count from video_info where comment_count is not null order by comment_count desc'''
    cursor.execute(query10)
    mydb.commit()
    t10=cursor.fetchall()
    df10=pd.DataFrame(t10,columns=['Channel_Name','Video_Title','Comment_Count'])
    st.write(df10)



