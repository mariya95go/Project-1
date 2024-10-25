from googleapiclient.discovery import build 
import pymongo
import psycopg2
import pandas as pd
import streamlit as st

#Api key accessing
def Api_connect():   
    Api_Id = "AIzaSyAuHfJlvOxPzHAL43C8TX8oCDFEcAOGvDE"

    api_service_name = "youtube"
    api_version = "v3"

    youtube = build(api_service_name,api_version,developerKey=Api_Id)
    return youtube
#accessing fun
youtube = Api_connect() 

#channels details
def get_channel_info(channel_id):
    request = youtube.channels().list(                    
        part = "snippet,contentDetails,statistics",
        id   =  channel_id                                      
        )
    response = request.execute() 

    for i in response['items']:
        data = dict(Channel_name = i["snippet"]["title"],
                    Channel_id = i["id"],
                    Subscribers = i['statistics']['subscriberCount'],
                    Views = i["statistics"]["viewCount"],
                    Total_Videos = i["statistics"]["videoCount"],
                    Channel_Description = i["snippet"]["description"],
                    Playlist_Id = i["contentDetails"]["relatedPlaylists"]["uploads"])
    return data 

#video ids
def get_videos_ids(channel_id):
    video_ids = []
    response = youtube.channels().list(id = channel_id,
                                    part = 'contentDetails').execute()
    Playlist_Id = response['items'][0]['contentDetails']['relatedPlaylists'] ['uploads'] 

    next_page_token = None

    while True:
        response1 = youtube.playlistItems().list(
                                        part = 'snippet',
                                        playlistId = Playlist_Id,
                                        maxResults = 50,
                                        pageToken = next_page_token).execute() 
        for i in range(len(response1['items'])):
            video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token = response1.get('nextPageToken')

        if next_page_token is None:
            break 
    return video_ids 

#video information
def get_video_info(video_ids):
    video_data = []
    for video_id in video_ids:
        request = youtube.videos().list(
                part = "snippet,contentDetails,statistics",
                id = video_id
        )
        response = request.execute() 
        
        for item in response["items"]:
            data = dict(Channel_Name = item['snippet']['channelTitle'],
                        Channel_Id = item['snippet']['channelId'],
                        Video_Id = item['id'],
                        Title = item['snippet']['title'],
                        Tags = item['snippet'].get('tags'),
                        Thumbnail = item['snippet']['thumbnails']['default']['url'],
                        Description = item['snippet'].get('description'), 
                        Published_Date = item['snippet']['publishedAt'],
                        Duration = item['contentDetails']['duration'],
                        Views = item['statistics']['viewCount'],
                        Likes = item['statistics'].get('likecount'),
                        Comments = item['statistics'].get('commentCount'),
                        Favorite_Count = item['statistics']['favoriteCount'],
                        Definition = item['contentDetails']['definition'],
                        Caption_Status = item['contentDetails']['caption']
                        )
            video_data.append(data)
    return video_data 

#comment details
def get_comment_info(video_ids):
    Comment_data = []
    try:
        for video_id in video_ids:
            request = youtube.commentThreads().list(
                part = "snippet",
                videoId = video_id,
                maxResults = 50
            )
            response = request.execute() 

            for item in response['items']:
                data = dict(Comment_Id = item['snippet']['topLevelComment']['id'],
                            Video_Id = item['snippet']['topLevelComment']['snippet']['videoId'],
                            Comment_Text = item['snippet']['topLevelComment']['snippet']['textDisplay'],
                            Comment_Author = item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            Comment_Published = item['snippet']['topLevelComment']['snippet']['publishedAt'])
                
                Comment_data.append(data)
    except:
        pass 
    return Comment_data

#playlist details
def get_playlist_info(channel_id):
    next_page_token = None
    All_info = []
    while True:
        request = youtube.playlists().list(
            part = 'snippet,contentDetails',
            channelId = channel_id,
            maxResults = 50,
            pageToken = next_page_token
        )
        response = request.execute()

        for item in response['items']:
            data = dict(Playlist_id = item['id'],
                        Title = item['snippet']['title'],
                        Channel_Id = item['snippet']['channelId'],
                        Channel_Name = item['snippet']['channelTitle'],
                        PublishedAt = item['snippet']['publishedAt'],
                        Video_Count = item['contentDetails']['itemCount']
                        )
            All_info.append(data)
        next_page_token = response.get('nextPageToken')
        if next_page_token is None:
            break 
    return All_info

#mongoDB upload
client = pymongo.MongoClient("mongodb+srv://esther:qAwmCtUilY6iGEnO@cluster0.8rsh29d.mongodb.net/?retryWrites=true&w=majority")
db = client["Youtube_dataharvesting"]

def channel_details(channel_id):
    ch_details = get_channel_info(channel_id)
    pl_details = get_playlist_info(channel_id)
    vi_ids     = get_videos_ids(channel_id)
    vi_details = get_video_info(vi_ids)
    com_details = get_comment_info(vi_ids)
    
    coll1 = db["channel_details"]
    coll1.insert_one({"channel_information":ch_details,"playlist_information":pl_details,
                      "video_information":vi_details,"comment_information":com_details})
   
    return "successfully completed"

#table creation (channels,playlists,videos,comments)
#get MongoDB database
#create database Youtube_data1 in postgre sql

#channels_table
def channels_table():
        db1 = psycopg2.connect(host = "localhost",
                                user = "postgres",
                                password = "Majella",
                                database = "Youtube_data1",
                                port = "5432")
        cursor = db1.cursor()
        #exists channels drop
        drop_query = '''drop table if exists channels'''
        cursor.execute(drop_query)
        db1.commit()
        #new channel create
        try:
                create_query ='''create table if not exists channels(Channel_Name varchar(100),
                                                                        Channel_Id varchar (80) primary key,
                                                                        Subscribers bigint,
                                                                        Views bigint,
                                                                        Total_Videos int,
                                                                        Channel_Description text,
                                                                        Playlist_Id varchar(80))'''
                cursor.execute(create_query)
                db1.commit()
        except:
                print("table created") 

        #table create/data frame
        ch_list =[]
        db = client["Youtube_dataharvesting"]
        coll1 = db["channel_details"]

        for ch_info in coll1.find({},{"_id":0,"channel_information":1}):
                ch_list.append(ch_info["channel_information"]) 
        df1 = pd.DataFrame(ch_list)

        #insert values in table
        for index, row in df1.iterrows():
                insert_query ='''insert into channels(Channel_name,
                                                        Channel_id,
                                                        Subscribers,
                                                        Views,
                                                        Total_Videos,
                                                        Channel_Description,
                                                        Playlist_Id)
                                                        
                                                        values(%s,%s,%s,%s,%s,%s,%s)'''
                values = (row['Channel_name'],
                        row['Channel_id'],
                        row['Subscribers'],
                        row['Views'],
                        row['Total_Videos'],
                        row['Channel_Description'],
                        row['Playlist_Id']) 
                try:
                        cursor.execute(insert_query, values)
                        db1.commit()
                except:
                        print("channel values are inserted") 

#playlists_table
def playlist_table():
        db1 = psycopg2.connect(host = "localhost",
                                        user = "postgres",
                                        password = "Majella",
                                        database = "Youtube_data1",
                                        port = "5432")
        cursor = db1.cursor()

        drop_query = '''drop table if exists playlists'''
        cursor.execute(drop_query)
        db1.commit()
                
        create_query ='''create table if not exists playlists(Playlist_id varchar(100) primary key,
                                                                                Title varchar(100),
                                                                                Channel_Id varchar(100),
                                                                                Channel_Name varchar(100),
                                                                                PublishedAt timestamp,
                                                                                Video_Count int)'''
                                                                        
        cursor.execute(create_query)
        db1.commit()

        pl_list =[]
        db = client["Youtube_dataharvesting"]
        coll1 = db["channel_details"]

        for pl_info in coll1.find({},{"_id":0,"playlist_information":1}):
                        for i in range(len(pl_info["playlist_information"])):
                                pl_list.append(pl_info["playlist_information"][i])
        df2 = pd.DataFrame(pl_list) 

        for index, row in df2.iterrows():
                insert_query ='''insert into playlists(Playlist_id,
                                                        Title,
                                                        Channel_Id,
                                                        Channel_Name,
                                                        PublishedAt,
                                                        Video_Count)
                                                        
                                                        
                                                        values(%s,%s,%s,%s,%s,%s)'''
                values = (row['Playlist_id'],
                        row['Title'],
                        row['Channel_Id'],
                        row['Channel_Name'],
                        row['PublishedAt'],
                        row['Video_Count'])
                
                
                cursor.execute(insert_query, values)
                db1.commit()

#videos_table
def videos_table():
        db1 = psycopg2.connect(host = "localhost",
                                        user = "postgres",
                                        password = "Majella",
                                        database = "Youtube_data1",
                                        port = "5432")
        cursor = db1.cursor()

        drop_query = '''drop table if exists videos'''
        cursor.execute(drop_query)
        db1.commit()
                
        create_query ='''create table if not exists videos(Channel_Name varchar(100),
                                                        Channel_Id varchar(100),
                                                        Video_Id varchar(30) primary key,
                                                        Title varchar(150),
                                                        Tags text,
                                                        Thumbnail varchar(200),
                                                        Description text, 
                                                        Published_Date timestamp,
                                                        Duration interval,
                                                        Views bigint,
                                                        Likes bigint,
                                                        Comments int,
                                                        Favorite_Count int,
                                                        Definition varchar(20),
                                                        Caption_Status varchar(50))'''
                                                                
        cursor.execute(create_query)
        db1.commit() 

        vi_list =[]
        db = client["Youtube_dataharvesting"]
        coll1 = db["channel_details"]

        for vi_info in coll1.find({},{"_id":0,"video_information":1}):
                for i in range(len(vi_info["video_information"])):
                        vi_list.append(vi_info["video_information"][i])
        df3 = pd.DataFrame(vi_list) 

        for index, row in df3.iterrows():
                insert_query ='''insert into videos(Channel_Name,
                                                        Channel_Id,
                                                        Video_Id,
                                                        Title,
                                                        Tags,
                                                        Thumbnail,
                                                        Description, 
                                                        Published_Date,
                                                        Duration,
                                                        Views,
                                                        Likes,
                                                        Comments,
                                                        Favorite_Count,
                                                        Definition,
                                                        Caption_Status)                                        
                                                        
                                                        values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
                values = (row['Channel_Name'],
                        row['Channel_Id'],
                        row['Video_Id'],
                        row['Title'],
                        row['Tags'],
                        row['Thumbnail'],
                        row['Description'],
                        row['Published_Date'],
                        row['Duration'],
                        row['Views'],
                        row['Likes'],
                        row['Comments'],
                        row['Favorite_Count'],
                        row['Definition'],
                        row['Caption_Status'])
                
                
                cursor.execute(insert_query, values)
                db1.commit()

#comments_table
def comments_table():
        db1 = psycopg2.connect(host = "localhost",
                                        user = "postgres",
                                        password = "Majella",
                                        database = "Youtube_data1",
                                        port = "5432")
        cursor = db1.cursor()

        drop_query = '''drop table if exists comments'''
        cursor.execute(drop_query)
        db1.commit()
                
        create_query ='''create table if not exists comments(Comment_Id varchar(100) primary key,
                                                                Video_Id varchar(50),
                                                                Comment_Text text,
                                                                Comment_Author varchar(150),
                                                                Comment_Published timestamp)'''
                                                                        
        cursor.execute(create_query)
        db1.commit()

        com_list =[]
        db = client["Youtube_dataharvesting"]
        coll1 = db["channel_details"]

        for com_info in coll1.find({},{"_id":0,"comment_information":1}):
                        for i in range(len(com_info["comment_information"])):
                                com_list.append(com_info["comment_information"][i])
        df4 = pd.DataFrame(com_list) 


        for index, row in df4.iterrows():
                insert_query ='''insert into comments(Comment_Id,
                                                                Video_Id,
                                                                Comment_Text,
                                                                Comment_Author,
                                                                Comment_Published)
                                                        
                                                        
                                                        values(%s,%s,%s,%s,%s)'''
                values = (row['Comment_Id'],
                        row['Video_Id'],
                        row['Comment_Text'],
                        row['Comment_Author'],
                        row['Comment_Published'])
                
                
                cursor.execute(insert_query, values)
                db1.commit()

#Df for streamlit
def tables():
    channels_table()
    playlist_table()
    videos_table()
    comments_table()

    return "tables created successfully"

#show table(channels,playlists,videos,comments) format output in streamlit
def show_channels_table():
    ch_list =[]
    db = client["Youtube_dataharvesting"]
    coll1 = db["channel_details"]

    for ch_info in coll1.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_info["channel_information"]) 
    df1 = st.dataframe(ch_list) 
    return df1

def show_playlists_table():
    pl_list =[]
    db = client["Youtube_dataharvesting"]
    coll1 = db["channel_details"]

    for pl_info in coll1.find({},{"_id":0,"playlist_information":1}):
                            for i in range(len(pl_info["playlist_information"])):
                                    pl_list.append(pl_info["playlist_information"][i])
    df2 = st.dataframe(pl_list) 
    return df2

def show_videos_table():
    vi_list =[]
    db = client["Youtube_dataharvesting"]
    coll1 = db["channel_details"]

    for vi_info in coll1.find({},{"_id":0,"video_information":1}):
                    for i in range(len(vi_info["video_information"])):
                            vi_list.append(vi_info["video_information"][i])
    df3 = st.dataframe(vi_list) 
    return df3

def show_comments_table():
    com_list =[]
    db = client["Youtube_dataharvesting"]
    coll1 = db["channel_details"]

    for com_info in coll1.find({},{"_id":0,"comment_information":1}):
                            for i in range(len(com_info["comment_information"])):
                                    com_list.append(com_info["comment_information"][i])
    df4 = st.dataframe(com_list) 
    return df4 

#streamlit code
with st.sidebar:
    st.title(":purple[YOUTUBE DATAHARVESTING AND WAREHOUSING]")
    st.header("Expertise in this Youtube")
    st.caption("Scripting in python")
    st.caption("Data Collections")
    st.caption("MongoDB")
    st.caption("API Integration")
    st.caption("Data Management using MongoDB and SQL")
channel_id = st.text_input("Enter the channel_ID")
if st.button("collect and store data"):
    ch_ids = []
    db = client["Youtube_dataharvesting"]
    coll1 = db["channel_details"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_ids.append(ch_data["channel_information"]["Channel_id"])
    if channel_id in ch_ids:
        st.success("given channel_id already exists")
    else:
        insert = channel_details(channel_id)
        st.success(insert)
#create a table format - sql
if st. button("Migrate to sql"):
    Table = tables()
    st.success(Table)
#output in dataframe
show_table = st.radio("SELECT THE TABLE FOR VIEW",("CHANNELS","PLAYLISTS","COMMENTS","VIDEOS"))     
if show_table == "CHANNELS":
    show_channels_table()
elif show_table == "PLAYLISTS":
    show_playlists_table()
elif show_table == "VIDEOS":
    show_videos_table()
elif show_table == "COMMENTS":
    show_comments_table()       

#SQL connection (10 queries)
db1 = psycopg2.connect(host = "localhost",
                                user = "postgres",
                                password = "Majella",
                                database = "Youtube_data1",
                                port = "5432")
cursor = db1.cursor()

queries = st.selectbox("select your queries",("1. Names of all the videos and their corresponding channels",
                                              "2. Which channels with most number of videos",
                                              "3. Top 10 most viewed videos and their respective channels",
                                              "4. How many comments in each video, and their corresponding video names",
                                              "5. Which videos with highest number of likes, and their corresponding channel names",
                                              "6. Total number of likes and dislikes for each video, and their corresponding video names",
                                              "7. Total number of views for each channel, and their corresponding channel names",
                                              "8. Names of all the channels that have published videos in the year 2022",
                                              "9. Average duration of all videos in each channel, and their corresponding channel names",
                                              "10. Which videos with highest number of comments, and their corresponding channel names"))

if queries == "1. Names of all the videos and their corresponding channels":
    query1 = '''select title as videos, channel_name as channelname from videos'''
    cursor.execute(query1)
    db1.commit()
    #fetch all fun
    t1 = cursor.fetchall()
    df = pd.DataFrame(t1, columns = ["video title", "channel name"])
    st.write(df) 
    
elif queries == "2. Which channels with most number of videos":
    query2 = '''select channel_name as channelname, total_videos as no_videos from channels
                order by total_videos desc'''
    cursor.execute(query2)
    db1.commit()
    #fetch all fun
    t2 = cursor.fetchall()
    df2 = pd.DataFrame(t2, columns = ["channel_name", "Number of videos"])
    st.write(df2) 
    
elif queries == "3. Top 10 most viewed videos and their respective channels":
    query3 = '''select views as views, channel_name as channelname, title as videotitle from videos
                where views is not null order by views desc limit 10'''
    cursor.execute(query3)
    db1.commit()
    #fetch all fun
    t3 = cursor.fetchall()
    df3 = pd.DataFrame(t3, columns = ["Most views", "channelname", "videotitle"])
    st.write(df3)
    
elif queries == "4. How many comments in each video, and their corresponding video names":
    query4 = '''select comments as no_comments, title as videotitle from videos where comments is not null'''
    cursor.execute(query4)
    db1.commit()
    #fetch all fun
    t4 = cursor.fetchall()
    df4 = pd.DataFrame(t4, columns = ["No of comments", "videotitle"])
    st.write(df4)

elif queries ==  "5. Which videos with highest number of likes, and their corresponding channel names":
    query5 = '''select title as videotitle, channel_name as channelname, likes as likecount 
                from videos where likes is not null order by likes desc'''
    cursor.execute(query5)
    db1.commit()
    #fetch all fun
    t5 = cursor.fetchall()
    df5 = pd.DataFrame(t5, columns = ["videotitle", "channelname", "likecount"])
    st.write(df5) 
    

elif queries ==  "6. Total number of likes and dislikes for each video, and their corresponding video names":
    query6 = '''select likes as likecount, title as videotitle from videos'''
    cursor.execute(query6)
    db1.commit()
    #fetch all fun
    t6 = cursor.fetchall()
    df6 = pd.DataFrame(t6, columns = ["likecount","videotitle"])
    st.write(df6) 
    
elif queries ==  "7. Total number of views for each channel, and their corresponding channel names":
    query7 = '''select channel_name as channelname, views as totalviews from channels'''
    cursor.execute(query7)
    db1.commit()
    #fetch all fun
    t7 = cursor.fetchall()
    df7 = pd.DataFrame(t7, columns = ["channelname","totalviews"])
    st.write(df7) 
 
elif queries ==  "8. Names of all the channels that have published videos in the year 2022":
    query8 = '''select title as video_title, published_date as videoreleased, channel_name as channelname
                from videos where extract(year from published_date)=2022'''
    cursor.execute(query8)
    db1.commit()
    #fetch all fun
    t8 = cursor.fetchall()
    df8 = pd.DataFrame(t8, columns = ["video_title", "published_date", "channelname"])
    st.write(df8) 

elif queries ==  "9. Average duration of all videos in each channel, and their corresponding channel names":
    query9 = '''select channel_name as channelname, AVG(duration) as averageduration from videos group by channel_name '''
    cursor.execute(query9)
    db1.commit()
    #fetch all fun
    t9 = cursor.fetchall()
    df9 = pd.DataFrame(t9, columns = ["channelname", "averageduration"])

    T9 = []
    for index,row in df9.iterrows():
        channel_title = row["channelname"]
        average_duration = row["averageduration"]
        average_duration_str = str(average_duration)
        T9.append(dict(channeltitle = channel_title, avgduration = average_duration_str)) 
    df = pd.DataFrame(T9)
    st.write(df) 
    
elif queries ==   "10. Which videos with highest number of comments, and their corresponding channel names":
    query10 = '''select title as videotitle, channel_name as channelname, comments as comments from videos
                where comments is not null order by comments desc'''
    cursor.execute(query10)
    db1.commit()
    #fetch all fun
    t10 = cursor.fetchall()
    df10 = pd.DataFrame(t10, columns = ["video title", "channel name", "comments"])
    st.write(df10) 
    
    