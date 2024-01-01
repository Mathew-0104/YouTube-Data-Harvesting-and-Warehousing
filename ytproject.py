import pymongo
import mysql.connector
import psycopg2
import pandas as pd
import streamlit as st
from googleapiclient.discovery import build


def connection():
    API_KEY='AIzaSyB-hnMy4RtugtPsKVojVHuX7XXERUk7etY'
    api_service_name = 'youtube'
    api_version = 'v3'
    
    youtube = build(api_service_name, api_version, developerKey=API_KEY)
    
    return youtube

youtube=connection()

def get_channel_data(channel_id):
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=channel_id
    )

    response = request.execute()

    if 'items' in response and len(response['items']) > 0:
        item = response['items'][0]
        data = {
            'Channel_Name': item['snippet']['title'],
            'Channel_Id': item['id'],
            'Subscriber': item['statistics']['subscriberCount'],
            'View_Count': item['statistics']['viewCount'],
            'Total_Video': item['statistics']['videoCount'],
            'Channel_Description': item['snippet']['description'],
            'Playlist_Id': item['contentDetails']['relatedPlaylists']['uploads']
        }
        return data
    else:
        return None  # Or handle the case where no items are found


def get_Video_Ids(channel_id):
    video_ids = []

    response = youtube.channels().list(id=channel_id,
                                       part='contentDetails').execute()

    Playlist_Id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    next_page_token = None

    while True:
        request_ids = youtube.playlistItems().list(part='snippet', playlistId=Playlist_Id,
                                                   maxResults=50, pageToken=next_page_token).execute()

        for i in range(len(request_ids['items'])):
            video_ids.append(request_ids['items'][i]['snippet']['resourceId']['videoId'])

        next_page_token = request_ids.get('nextPageToken')

        if next_page_token is None:
            break

    return video_ids

def get_video_info(videos_ids):
    video_data = []
    for video_id in videos_ids:
        request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=video_id
        )
        response = request.execute()

        for item in response["items"]:
            data={
                'Channel_Name' : item['snippet']['channelTitle'],
                'Channel_Id' : item['snippet']['channelId'],
                'VideoIds' : item['id'],
                'Titles' : item['snippet']['title'],
                'Tags': item['snippet'].get('tags'),
                'Thumbnail': item['snippet']['thumbnails']['default']['url'],
                'Description': item['snippet']['description'],
                'Published_Date': item['snippet']['publishedAt'],
                'Duration': item['contentDetails']['duration'],
                'Views': item['statistics'].get('viewCount', 0),
                'Likes': item['statistics'].get('likeCount', 0),
                'Comments': item['statistics'].get('commentCount', 0),
                'Favorite_Count': item['statistics'].get('favoriteCount', 0),
                'Definition': item['contentDetails']['definition'],
                'Caption_Status': item['contentDetails']['caption']



            }

            video_data.append(data)
            
    return video_data


def get_comments_info(videos_ids):
    Comment_Data=[]
    try:
        for video_id in videos_ids:
            request = youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                maxResults=50
            )
            response = request.execute()

            for item in response['items']:
                data={
                    'Comment_Id' : item['snippet']['topLevelComment']['id'],
                    'Video_id' : item['snippet']['topLevelComment']['snippet']['videoId'],
                    'Comment_text' : item['snippet']['topLevelComment']['snippet']['textDisplay'],
                    'Comment_author' : item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                    'Comment_published' : item['snippet']['topLevelComment']['snippet']['publishedAt']
                }

                Comment_Data.append(data)

    except:
        pass
    return Comment_Data


def get_playlist_info(channel_id):
    next_page_token = None
    Playlist_Data = []

    while True:
        request = youtube.playlists().list(
            part='snippet,contentDetails',
            channelId=channel_id,
            maxResults=50,
            pageToken=next_page_token
        )

        response = request.execute()

        for item in response['items']:
            data = {
                'Playlist_Id': item['id'],
                'Title': item['snippet']['title'],
                'Channnel_id': item['snippet']['channelId'],
                'Channel_Title': item['snippet']['channelTitle'],
                'Published_At': item['snippet']['publishedAt'],
                'Video_Count': item['contentDetails']['itemCount']
            }
            Playlist_Data.append(data)

        next_page_token = response.get('nextPageToken')
        if next_page_token is None:
            break

    return Playlist_Data


mdb_url="mongodb://localhost:27017/"
db_name = "YT3"

client = pymongo.MongoClient(mdb_url)
db=client[db_name]
collection_name="Youtube_Details1"

def channel_details(channel_id):
    channel_details = get_channel_data(channel_id)
    playlist_details = get_playlist_info(channel_id)
    vid_ids = get_Video_Ids(channel_id)
    vid_details = get_video_info(vid_ids)
    comment_details = get_comments_info(vid_ids)

    
    Coll1=db[collection_name]
    Coll1.insert_one({"Channel_Details":channel_details,"Playlist_Details":playlist_details,
                                "Videos_Details":vid_details,"Comments_Details":comment_details})
    
    return "upload successfully in MongoDB"


def channels_table():
    mydb = mysql.connector.connect(host="127.0.0.1", user="root", password="Mattpop", database="YT")

    cursor = mydb.cursor()

    try:
        create_query = '''create table if not exists channels(Channel_Name varchar(100),
                                Channel_Id varchar(80) primary key,
                                Subscriber bigint,
                                View_Count bigint,
                                Total_Video int,
                                Channel_Description text,
                                Playlist_Id varchar(50))'''
        cursor.execute(create_query)
        mydb.commit()

    except:
        ptint("Channels Table alredy created")

    ch_list = []
    db = client["YT3"]
    coll1 = db["Youtube_Details1"]
    for ch_data in coll1.find({},{"_id":0,"Channel_Details":1}):
        ch_list.append(ch_data["Channel_Details"])
    df = pd.DataFrame(ch_list)

    for index, row in df.iterrows():
        insert_query = '''
            INSERT INTO channels (Channel_Name,
                Channel_Id,
                Subscriber,
                View_Count,
                Total_Video,
                Channel_Description,
                Playlist_Id)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        '''
        values = (
            row['Channel_Name'],
            row['Channel_Id'],
            row['Subscriber'],
            row['View_Count'],
            row['Total_Video'],
            row['Channel_Description'],
            row['Playlist_Id']
        )

    try:
        cursor.execute(insert_query, values)
        mydb.commit()

    except:
        print("Channels values already inserted in the table")



def playlists_table():

    mydb = mysql.connector.connect(host="127.0.0.1", user="root", password="Mattpop", database="YT")

    cursor = mydb.cursor()

    drop_query = "drop table if exists playlists"
    cursor.execute(drop_query)
    mydb.commit()

    try:
        create_query = '''create table if not exists playlists(Playlist_Id varchar(100) primary key,
                                Title varchar(80), 
                                Channnel_id varchar(100), 
                                Channel_Title varchar(100),
                                Published_At timestamp,
                                Video_Count int
                                )'''
        cursor.execute(create_query)
        mydb.commit()

    except:
        print("Playlists Table alredy created")

    db = client["YT3"]
    coll1 = db["Youtube_Details1"]
    pl_list = []
    for pl_data in coll1.find({},{"_id":0,"Playlist_Details":1}):
        for i in range(len(pl_data["Playlist_Details"])):
                pl_list.append(pl_data["Playlist_Details"][i])
    df1 = pd.DataFrame(pl_list)

    from datetime import datetime

    for index,row in df1.iterrows():
        insert_query = '''INSERT into playlists(Playlist_Id,
                                                    Title,
                                                    Channnel_id,
                                                    Channel_Title,
                                                    Published_At,
                                                    Video_Count)
                                        VALUES(%s,%s,%s,%s,%s,%s)'''            


        published_at = datetime.strptime(row['Published_At'], '%Y-%m-%dT%H:%M:%SZ')
        values =(
                row['Playlist_Id'],
                row['Title'],
                row['Channnel_id'],
                row['Channel_Title'],
                published_at,
                row['Video_Count'])
        try:

            cursor.execute(insert_query,values)
            mydb.commit()

        except:
            print("Playlists values are already inserted")



def videos_table():

    mydb = mysql.connector.connect(host="127.0.0.1", user="root", password="Mattpop", database="YT")
    cursor = mydb.cursor()

    drop_query = "drop table if exists videos"
    cursor.execute(drop_query)
    mydb.commit()

    try:
        create_query = '''CREATE TABLE IF NOT EXISTS videos (
                            Channel_Name VARCHAR(150),
                            Channel_Id VARCHAR(100),
                            VideoIds VARCHAR(50) PRIMARY KEY, 
                            Titles VARCHAR(150), 
                            Tags TEXT,
                            Thumbnail VARCHAR(225),
                            Description TEXT, 
                            Published_Date TIMESTAMP,
                            Duration VARCHAR(20),  -- Change to VARCHAR or another suitable type
                            Views BIGINT, 
                            Likes BIGINT,
                            Comments INT,
                            Favorite_Count INT, 
                            Definition VARCHAR(10), 
                            Caption_Status VARCHAR(50)
                        )'''

        cursor.execute(create_query)
        mydb.commit()

    except:
        print("Videos Table alrady created")


    vi_list = []
    db = client["YT3"]
    coll1 = db["Youtube_Details1"]
    for vi_data in coll1.find({}, {"_id": 0, "Videos_Details": 1}):
        if vi_data and "Videos_Details" in vi_data:
            for i in range(len(vi_data["Videos_Details"])):
                vi_list.append(vi_data["Videos_Details"][i])

    print(f"Number of videos in vi_list: {len(vi_list)}")

    df2 = pd.DataFrame(vi_list)


    from datetime import datetime

    for index, row in df2.iterrows():
        insert_query = '''
            INSERT INTO videos (Channel_Name,
                Channel_Id,
                VideoIds, 
                Titles, 
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
                Caption_Status 
                )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        '''

        # Check if 'Tags' is not None before applying join
        tags_str = ','.join(map(str, row['Tags'])) if row['Tags'] is not None else None
        published_at = datetime.strptime(row['Published_Date'], '%Y-%m-%dT%H:%M:%SZ')

        # Replace None with an appropriate default value or handle it according to your use case
        tags_str = tags_str if tags_str is not None else 'Default_Tag_Value'

        values = (
            row['Channel_Name'],
            row['Channel_Id'],
            row['VideoIds'],
            row['Titles'],
            tags_str,
            row['Thumbnail'],
            row['Description'],
            published_at,
            row['Duration'],
            row['Views'],
            row['Likes'],
            row['Comments'],
            row['Favorite_Count'],
            row['Definition'],
            row['Caption_Status']
        )

        try:
            cursor.execute(insert_query, values)
            mydb.commit()

        except:
            print("Videos values already inserted in the table")



def comments_table():

    mydb = mysql.connector.connect(host="127.0.0.1", user="root", password="Mattpop", database="YT")
    cursor = mydb.cursor()

    drop_query = "drop table if exists comments"
    cursor.execute(drop_query)
    mydb.commit()

    try:
        create_query = '''CREATE TABLE if not exists comments(Comment_Id varchar(100) primary key,
                       Video_id varchar(80),
                       Comment_text text, 
                       Comment_author varchar(150),
                       Comment_published timestamp)'''
        cursor.execute(create_query)
        mydb.commit()

    except:
        print("Commentsp Table already created")

    com_list = []
    db = client["YT3"]
    coll1 = db["Youtube_Details1"]
    for com_data in coll1.find({},{"_id":0,"Comments_Details":1}):
        for i in range(len(com_data["Comments_Details"])):
            com_list.append(com_data["Comments_Details"][i])
    df3 = pd.DataFrame(com_list)


    for index, row in df3.iterrows():
        insert_query = '''
            INSERT INTO comments (Comment_Id,
                                  Video_id,
                                  Comment_text,
                                  Comment_author,
                                  Comment_published)
            VALUES (%s, %s, %s, %s, %s)

        '''
        published_at = datetime.strptime(row['Comment_published'], '%Y-%m-%dT%H:%M:%SZ')
        values = (
            row['Comment_Id'],
            row['Video_id'],
            row['Comment_text'],
            row['Comment_author'],
            published_at
        )

        try:
            cursor.execute(insert_query,values)
            mydb.commit()

        except:
            print("This comments are already exist in comments table")



from datetime import datetime
import streamlit as st

def tables():
    channels_table()
    playlists_table()
    videos_table()
    comments_table()
    
    return "Tables created"


def show_channels_table():
    ch_list = []
    db = client["YT3"]
    coll1 = db["Youtube_Details1"]
    for ch_data in coll1.find({},{"_id":0,"Channel_Details":1}):
        ch_list.append(ch_data["Channel_Details"])
    df = st.dataframe(ch_list)
    
    return df


def show_playlists_table():
    db = client["YT3"]
    coll1 = db["Youtube_Details1"]
    pl_list = []
    for pl_data in coll1.find({},{"_id":0,"Playlist_Details":1}):
        for i in range(len(pl_data["Playlist_Details"])):
                pl_list.append(pl_data["Playlist_Details"][i])
    df1 = st.dataframe(pl_list)
    
    return df1



def show_videos_table():
    vi_list = []
    db = client["YT3"]
    coll1 = db["Youtube_Details1"]
    for vi_data in coll1.find({}, {"_id": 0, "Videos_Details": 1}):
        if vi_data and "Videos_Details" in vi_data:
            for i in range(len(vi_data["Videos_Details"])):
                vi_list.append(vi_data["Videos_Details"][i])

    print(f"Number of videos in vi_list: {len(vi_list)}")

    df2 = st.dataframe(vi_list)
    
    return df2



def show_comments_table():
    com_list = []
    db = client["YT3"]
    coll1 = db["Youtube_Details1"]
    for com_data in coll1.find({},{"_id":0,"Comments_Details":1}):
        for i in range(len(com_data["Comments_Details"])):
            com_list.append(com_data["Comments_Details"][i])
    df3 = st.dataframe(com_list)
    
    return df3


import streamlit as st
st.title(":red[YouTube Data Harvesting and Warehousing using SQL, MongoDB and Streamlit]")

with st.sidebar:
    st.header("Skill Take Away")
    st.caption("Python Scripting")
    st.caption("Data Collection")
    st.caption("MongoDB")
    st.caption("API Intergation")
    st.caption("Data management using MongoDB and SQL")
    
channel_Id = st.text_input("Enter the Channel ID")

if st.button("Collect and store Data"):
    ch_ids=[]
    db = client["YT3"]
    coll1 = db["Youtube_Details1"]
    for ch_data in coll1.find({},{"_id":0,"Channel_Details":1}):
        ch_ids.append(ch_data["Channel_Details"]["Channel_Id"])
        
    if channel_Id in ch_ids:
        st.success("Channels Details of given channel id already exists")
        
    else:
        insert=channel_details(channel_Id)
        st.success(insert)

if st.button("Migrate to SQL"):
    table01=tables()
    st.success(table01)
    
show_table = st.radio("SELECT THE TABLE FOR VIEW",(":green[channels]",":orange[playlists]",":red[videos]",":blue[comments]"))

if show_table == ":green[channels]":
    show_channels_table()
elif show_table == ":orange[playlists]":
    show_playlists_table()
elif show_table ==":red[videos]":
    show_videos_table()
elif show_table == ":blue[comments]":
    show_comments_table()



mydb = mysql.connector.connect(host="127.0.0.1", user="root", password="Mattpop", database="YT")
cursor = mydb.cursor()

question = st.selectbox(
    'Please Select Your Question',
    ('1. All the videos and the Channel Name',
     '2. Channels with most number of videos',
     '3. 10 most viewed videos',
     '4. Comments in each video',
     '5. Videos with highest likes',
     '6. likes of all videos',
     '7. views of each channel',
     '8. videos published in the year 2022',
     '9. average duration of all videos in each channel',
     '10. videos with highest number of comments'))


if question=='1. All the videos and the Channel Name':

    query1 = '''select Titles as videos, Channel_Name as channelname from videos'''
    cursor.execute(query1)
    t1 = cursor.fetchall()
    df = pd.DataFrame(t1, columns=["Video title", "channelname"])
    st.write(df)
    
    cursor.close()
    mydb.commit()
    mydb.close()

    
    
elif question=='2. Channels with most number of videos':
    
    query2 = '''select Channel_Name as channelname, Total_Video as Total_No_Of_Videos from channels
                    order by Total_Video desc '''
    cursor.execute(query2)
    t2 = cursor.fetchall()
    df2 = pd.DataFrame(t2, columns=["channelname", "No Of Videos"])
    st.write(df2)

    cursor.close()
    mydb.commit()
    mydb.close()
    
    
elif question=='3. 10 most viewed videos':

    query3 = '''select Views as view, Channel_Name as channelname, Titles as video_title from videos
                    where Views is not null order by Views desc limit 10'''
    cursor.execute(query3)
    t3 = cursor.fetchall()
    df3 = pd.DataFrame(t3, columns=[ "views", "channelname", "video_of_title"])
    st.write(df3)

    cursor.close()
    mydb.commit()
    mydb.close()
    
    
elif question=='4. Comments in each video':

    query4 = '''select Comments as comment, Titles as video_title from videos
                    where Comments is not null'''
    cursor.execute(query4)
    t4 = cursor.fetchall()
    df4 = pd.DataFrame(t4, columns=[ "Comments", "video_of_title"])
    st.write(df4)

    cursor.close()
    mydb.commit()
    mydb.close()
    
    
elif question=='5. Videos with highest likes':

    query5 = '''select Titles as Title, Channel_Name as channelname, Likes as likecount from videos
                    where Likes is not null order by Likes desc'''
    cursor.execute(query5)
    t5 = cursor.fetchall()
    df5 = pd.DataFrame(t5, columns=[ "Title", "channelname","likes"])
    st.write(df5)

    cursor.close()
    mydb.commit()
    mydb.close()
    
    
elif question=='6. likes of all videos':

    query6 = '''select Titles as Title, Likes as likecount from videos'''
    cursor.execute(query6)
    t6 = cursor.fetchall()
    df6 = pd.DataFrame(t6, columns=[ "Title","likes"])
    st.write(df6)

    cursor.close()
    mydb.commit()
    mydb.close()
    
    
elif question=='7. views of each channel':

    query7 = '''select Channel_Name as channelname, View_Count as channel_views from channels'''
    cursor.execute(query7)
    t7 = cursor.fetchall()
    df7 = pd.DataFrame(t7, columns=[ "channelname","channelviews"])
    st.write(df7)

    cursor.close()
    mydb.commit()
    mydb.close()
    
elif question=='8. videos published in the year 2022':

    query8 = '''select Titles as Video_Title, Published_Date as VideoRelease, Channel_Name as ChannelName from videos 
                    where extract(year from Published_Date) = 2022;'''
    cursor.execute(query8)
    t8 = cursor.fetchall()
    df8 = pd.DataFrame(t8, columns=[ "Name", "Video Publised On", "ChannelName"])
    st.write(df8)

    cursor.close()
    mydb.commit()
    mydb.close()
    

elif question=='9. average duration of all videos in each channel':

    query9 = '''select Channel_Name as ChannelName, AVG(Duration) as average_duration FROM videos GROUP BY Channel_Name;'''
    cursor.execute(query9)
    t9 = cursor.fetchall()
    df9 = pd.DataFrame(t9, columns=['ChannelTitle', 'Average Duration'])

    cursor.close()
    mydb.commit()
    mydb.close()
    
    
    T9=[]
    for index, row in df9.iterrows():
        channel_title = row['ChannelTitle']
        average_duration = row['Average Duration']
        average_duration_str = str(average_duration)
        T9.append({"Channel Title": channel_title ,  "Average Duration": average_duration_str})
    df1=pd.DataFrame(T9)
    st.write(df1)
    
elif question=='10. videos with highest number of comments':

    query10 = '''select Titles as VideoTitle, Channel_Name as ChannelName, Comments as Comment from videos 
                           where Comments is not null order by Comments desc;'''
    cursor.execute(query10)
    t10 = cursor.fetchall()
    df10 = pd.DataFrame(t10, columns=['Video Title', 'Channel Name', 'NO Of Comments'])
    st.write(df10)

    cursor.close()
    mydb.commit()
    mydb.close()