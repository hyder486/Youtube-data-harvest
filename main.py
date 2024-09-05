#importing the necessary libraries
import pandas as pd
import plotly.express as px
import streamlit as st
from streamlit_option_menu import option_menu
import mysql.connector as sql
import pymongo
from datetime import datetime, timedelta
from googleapiclient.discovery import build
from PIL import Image
import re



# SETTING PAGE CONFIGURATIONS
icon = Image.open("youtube_logo.jpg")
st.set_page_config(page_title= "Youtube Data Harvesting and Warehousing",
                   page_icon= icon,
                   layout= "wide",                   
                   initial_sidebar_state= "collapsed",
                   menu_items={'About': """Created by *Ahmed Hyder*"""})

# CREATING OPTION MENU
selected = option_menu(None, ["Home","Extract and Transform","View"], 
                           icons=["house","wrench","card-image"],
                           default_index=0,
                           styles={"nav-link": {"font-size": "25px", "text-align": "left", "margin": "10px", 
                                                "--hover-color": "#566573 "},
                                   "icon": {"font-size": "20px"},
                                   "container" : {"max-width": "6000px"},
                                   "nav-link-selected": {"background-color": "#566573"}})

# Bridging a connection with MongoDB and Creating a new database(youtube_data)
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client['capstone_project']




# CONNECTING WITH MYSQL DATABASE
mydb = sql.connect(host="localhost",
                   user="root",
                   password="ahs894",
                   database= "youtube_warehouse"
                  )
mycursor = mydb.cursor(buffered=True)

# BUILDING CONNECTION WITH YOUTUBE API
api_key = "your API key"
youtube = build('youtube','v3',developerKey=api_key)


def init_mysql_tables():
   
    
    query = """
                
                CREATE TABLE IF NOT EXISTS channels (
                        id INT PRIMARY KEY AUTO_INCREMENT,
                        Channel_id VARCHAR(255),
                        Channel_name VARCHAR(255),
                        Playlist_id VARCHAR(255),
                        Subscribers INT,
                        Views BIGINT,
                        Total_videos INT,
                        Description TEXT,
                        Country VARCHAR(100)
                    );
            """
    mycursor.execute(query)


    
    
    query = """
                CREATE TABLE IF NOT EXISTS videos (
                        id INT PRIMARY KEY AUTO_INCREMENT,
                        Video_id VARCHAR(255),
                        Channel_name VARCHAR(255),
                        Channel_id VARCHAR(255),
                        Title VARCHAR(255),
                        Tags TEXT,
                        Thumbnail VARCHAR(255),
                        Description TEXT,
                        Published_date DATE,
                        Duration TIME,
                        Views BIGINT,
                        Likes INT,
                        Comments INT,
                        Favorite_count INT,
                        Definition VARCHAR(50),     
                        Caption_status VARCHAR(50)
                    );
            """
    mycursor.execute(query)

    

    query = """
                CREATE TABLE IF NOT EXISTS comments (
                        id INT PRIMARY KEY AUTO_INCREMENT,
                        Comment_id VARCHAR(255) ,
                        Video_id VARCHAR(255),
                        Comment_text TEXT,
                        Comment_author VARCHAR(255),
                        Comment_posted_date DATETIME,
                        Like_count INT,
                        Reply_count INT
                    );

            """
    mycursor.execute(query)

def iso8601_to_mysql_time(iso_duration):
    pattern = re.compile(r'P(?:(?P<days>\d+)D)?T?(?:(?P<hours>\d+)H)?(?:(?P<minutes>\d+)M)?(?:(?P<seconds>\d+)S)?')
    match = pattern.match(iso_duration)
    
    if not match:
        return "00:00:00"  # Default if no match found
    
    duration = match.groupdict()
    
    hours = int(duration['hours']) if duration['hours'] else 0
    minutes = int(duration['minutes']) if duration['minutes'] else 0
    seconds = int(duration['seconds']) if duration['seconds'] else 0
    
    # Convert to MySQL TIME format (HH:MM:SS)
    mysql_time = f"{hours:02}:{minutes:02}:{seconds:02}"
    return mysql_time


def convert_date_string(date_string): 
    date_object = datetime.strptime(date_string, '%Y-%m-%dT%H:%M:%SZ') 
    mysql_date_string = date_object.strftime('%Y-%m-%d %H:%M:%S')
    return mysql_date_string
            


# FUNCTION TO GET CHANNEL DETAILS
def get_channel_details(channel_id):
    ch_data = []
    response = youtube.channels().list(part = 'snippet,contentDetails,statistics',
                                     id= channel_id).execute()

    for i in range(len(response['items'])):
        data = dict(Channel_id = channel_id[i],
                    Channel_name = response['items'][i]['snippet']['title'],
                    Playlist_id = response['items'][i]['contentDetails']['relatedPlaylists']['uploads'],
                    Subscribers = response['items'][i]['statistics']['subscriberCount'],
                    Views = response['items'][i]['statistics']['viewCount'],
                    Total_videos = response['items'][i]['statistics']['videoCount'],
                    Description = response['items'][i]['snippet']['description'],
                    Country = response['items'][i]['snippet'].get('country')
                    )
        ch_data.append(data)
    return ch_data

# FUNCTION TO GET VIDEO IDS
def get_channel_videos(channel_id):
    video_ids = []
    # get Uploads playlist id
    res = youtube.channels().list(id=channel_id, 
                                  part='contentDetails').execute()
    playlist_id = res['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    next_page_token = None
    
    while True:
        res = youtube.playlistItems().list(playlistId=playlist_id, 
                                           part='snippet', 
                                           maxResults=50,
                                           pageToken=next_page_token).execute()
        
        for i in range(len(res['items'])):
            video_ids.append(res['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token = res.get('nextPageToken')
        
        if next_page_token is None:
            break
    return video_ids


# FUNCTION TO GET VIDEO DETAILS
def get_video_details(v_ids):
    video_stats = []
    
    for i in range(0, len(v_ids), 50):
        response = youtube.videos().list(
                    part="snippet,contentDetails,statistics",
                    id=','.join(v_ids[i:i+50])).execute()
        for video in response['items']:
            video_details = dict(Channel_name = video['snippet']['channelTitle'],
                                Channel_id = video['snippet']['channelId'],
                                Video_id = video['id'],
                                Title = video['snippet']['title'],
                                Tags = video['snippet'].get('tags'),
                                Thumbnail = video['snippet']['thumbnails']['default']['url'],
                                Description = video['snippet']['description'],
                                Published_date = video['snippet']['publishedAt'],
                                Duration = video['contentDetails']['duration'],
                                Views = video['statistics'].get('viewCount'),
                                Likes = video['statistics'].get('likeCount'),
                                Comments = video['statistics'].get('commentCount'),
                                Favorite_count = video['statistics']['favoriteCount'],
                                Definition = video['contentDetails']['definition'],
                                Caption_status = video['contentDetails']['caption']
                               )
            video_stats.append(video_details)
    return video_stats


# FUNCTION TO GET COMMENT DETAILS
def get_comments_details(v_id):
    comment_data = []
    try:
        next_page_token = None
        while True:
            response = youtube.commentThreads().list(part="snippet,replies",
                                                    videoId=v_id,
                                                    maxResults=100,
                                                    pageToken=next_page_token).execute()
            for cmt in response['items']:
                data = dict(Comment_id = cmt['id'],
                            Video_id = cmt['snippet']['videoId'],
                            Comment_text = cmt['snippet']['topLevelComment']['snippet']['textDisplay'],
                            Comment_author = cmt['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            Comment_posted_date = cmt['snippet']['topLevelComment']['snippet']['publishedAt'],
                            Like_count = cmt['snippet']['topLevelComment']['snippet']['likeCount'],
                            Reply_count = cmt['snippet']['totalReplyCount']
                           )
                comment_data.append(data)
            next_page_token = response.get('nextPageToken')
            if next_page_token is None:
                break
    except:
        pass
    return comment_data


# FUNCTION TO GET CHANNEL NAMES FROM MONGODB
def channel_names():   
    ch_name = []
    for i in db.channel_details.find():
        ch_name.append(i['Channel_name'])
    return ch_name


# create the mysql tables to store the channels, videos, and comments
init_mysql_tables()

# HOME PAGE
if selected == "Home":
    # Title Image
    
    col1,col2 = st.columns(2,gap= 'medium')
    col1.markdown("## :green[Domain] : Social Media")
    col1.markdown("## :green[Technologies used] : Python,MongoDB, Youtube Data API, MySql, Streamlit")
    col1.markdown("## :green[Overview] : Retrieving the Youtube channels data from the Google API, storing it in a MongoDB as data lake, migrating and transforming data into a SQL database,then querying the data and displaying it in the Streamlit app.")
    col2.markdown("#   ")
    col2.markdown("#   ")
    col2.markdown("#   ")

# EXTRACT and TRANSFORM PAGE
if selected == "Extract and Transform":
    tab1,tab2 = st.tabs(["$\huge EXTRACT $", "$\huge TRANSFORM $"])
    
    # EXTRACT TAB
    with tab1:
        st.markdown("#    ")
        st.write("### Enter YouTube Channel_ID below :")
        ch_id = st.text_input("Hint : Goto channel's home page > Right click > View page source > Find channel_id").split(',')

        if ch_id and st.button("Extract Data"):
            ch_details = get_channel_details(ch_id)
            st.write(f'#### Extracted data from :green["{ch_details[0]["Channel_name"]}"] channel')
            st.table(ch_details)

        if st.button("Upload to MongoDB"):
            with st.spinner('Upload in Progress. Please Wait...'):
                progress_bar = st.progress(0)             
                ch_details = get_channel_details(ch_id)
                v_ids = get_channel_videos(ch_id)
                vid_details = get_video_details(v_ids)
                progress_bar.progress(5)

                def comments():
                    com_d = []
                    for i in v_ids:
                        com_d+= get_comments_details(i)
                    return com_d
                comm_details = comments()
                progress_bar.progress(10)

                collections1 = db.channel_details
                collections1.insert_many(ch_details)
                progress_bar.progress(30)

                collections2 = db.video_details
                collections2.insert_many(vid_details)
                progress_bar.progress(60)
   
                collections3 = db.comments_details
                collections3.insert_many(comm_details)
                progress_bar.progress(100)
                st.success("Upload to MongoDB successful !!")
      
    # TRANSFORM TAB
   # TRANSFORM TAB
    with tab2:     
        st.markdown("#   ")
        st.markdown("### Select a channel to begin Transformation to SQL")
    
        ch_names = channel_names()
        user_inp = st.selectbox("Select channel", options=ch_names)
    
    def insert_into_channels():
        collections = db.channel_details.find()
        print("#" * 80)
        print("================== Inserting Channel Details ===============================")
        for i in collections:
            
            channel_id = i['Channel_id']  
            channel_name = i['Channel_name']
            playlist_id = i['Playlist_id']
            subscribers = i.get('Subscribers', 0)
            views = i.get('Views', 0)
            total_videos = i.get('Total_videos', 0)
            description = i.get('Description', '')  # Handling potential missing fields
            country = i.get('Country', 'Unknown')   # Default to 'Unknown' if country is missing

            query = """INSERT INTO channels (Channel_id, Channel_name, Playlist_id, Subscribers, Views, Total_videos, Description, Country) 
                           VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"""
            values = (channel_id, channel_name, playlist_id, subscribers, views, total_videos, description, country)

            print("Executing query:", query % values)
            mycursor.execute(query, values)
           
        mydb.commit()
        

  
    def insert_into_videos():
        print("#" * 80)
        print("================== Inserting Video Details ===============================")
        collectionss = db.video_details.find({}, no_cursor_timeout = True)
        
        for i in collectionss:
            
            channel_name = i['Channel_name']
            channel_id = i['Channel_id']
            video_id = i['Video_id']
            title = i['Title']
            tags = ', '.join(i['Tags']) if i.get('Tags') else None
            thumbnail = i['Thumbnail']
            description = i.get('Description', '')
            published_date = convert_date_string(i['Published_date'])
            duration = iso8601_to_mysql_time(i['Duration'])
            views = i.get('Views', 0)
            likes = i.get('Likes', 0)
            comments = i.get('Comments', 0)
            favorite_count = i.get('Favorite_count', 0)
            definition = i['Definition']
            caption_status = i['Caption_status']

            query1 = """INSERT INTO videos (Channel_name, Channel_id, Video_id, Title, Tags, Thumbnail, Description, Published_date, Duration, Views, Likes, Comments, Favorite_count, Definition, Caption_status) 
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
            values = (channel_name, channel_id, video_id, title, tags, thumbnail, description, published_date, duration, views, likes, comments, favorite_count, definition, caption_status)

            print("Executing query:", query1 % values)
            mycursor.execute(query1, values)
            
            
        mydb.commit()
        

    def insert_into_comments():
        print("#" * 80)
        print("================== Inserting Comments ===============================")
        collections4 = db.comments_details.find({}, no_cursor_timeout = True)
        for i in collections4:
            try:
                comment_id = i['Comment_id']
                video_id = i['Video_id']
                comment_text = i['Comment_text']
                comment_author = i['Comment_author']
                comment_posted_date = convert_date_string(i['Comment_posted_date'])
                like_count = i.get('Like_count', 0)
                reply_count = i.get('Reply_count', 0)

                query2 = """INSERT INTO comments (Comment_id, Video_id, Comment_text, Comment_author, Comment_posted_date, Like_count, Reply_count) 
                            VALUES (%s, %s, %s, %s, %s, %s, %s)"""
                values = (comment_id, video_id, comment_text, comment_author, comment_posted_date, like_count, reply_count)

                print("Executing query:", query2 % values)
                mycursor.execute(query2, values)
            except Exception as e:
                print(f"Error inserting comment {comment_id}: {e}")
        mydb.commit()
        

    if st.button("Apply"):
        try:   
            progress_bar = st.progress(0) 
            insert_into_channels()
            progress_bar.progress(20)
            insert_into_videos()
            progress_bar.progress(40)
            insert_into_comments()
            progress_bar.progress(100)
            st.success("Transformation to MySQL Successful!!!")
        except Exception as e:
            st.error(f"An error occurred: {e}")

            
                
# VIEW PAGE
if selected == "View":
    
    st.write("## :orange[Select any question to get Insights]")
    questions = st.selectbox('Questions',
    ['Click the question that you would like to query',
    '1. What are the names of all the videos and their corresponding channels?',
    '2. Which channels have the most number of videos, and how many videos do they have?',
    '3. What are the top 10 most viewed videos and their respective channels?',
    '4. How many comments were made on each video, and what are their corresponding video names?',
    '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
    '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
    '7. What is the total number of views for each channel, and what are their corresponding channel names?',
    '8. What are the names of all the channels that have published videos in the year 2022?',
    '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
    '10. Which videos have the highest number of comments, and what are their corresponding channel names?'])
    
    if questions == '1. What are the names of all the videos and their corresponding channels?':
        mycursor.execute("""SELECT DISTINCT title AS Video_Title, channel_name AS Channel_Name FROM videos ORDER BY channel_name""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
        
    elif questions == '2. Which channels have the most number of videos, and how many videos do they have?':
        mycursor.execute("""SELECT DISTINCT channel_name 
        AS Channel_Name, total_videos AS Total_Videos
                            FROM channels
                            ORDER BY total_videos DESC""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
        st.write("### :green[Number of videos in each channel :]")
        #st.bar_chart(df,x= mycursor.column_names[0],y= mycursor.column_names[1])
        fig = px.bar(df,
                     x=mycursor.column_names[0],
                     y=mycursor.column_names[1],
                     orientation='v',
                     color=mycursor.column_names[0]
                    )
        st.plotly_chart(fig,use_container_width=True)
        
    elif questions == '3. What are the top 10 most viewed videos and their respective channels?':
        mycursor.execute("""SELECT DISTINCT channel_name AS Channel_Name, title AS Video_Title, views AS Views 
                            FROM videos
                            ORDER BY views DESC
                            LIMIT 10""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
        st.write("### :green[Top 10 most viewed videos :]")
        fig = px.bar(df,
                     x=mycursor.column_names[2],
                     y=mycursor.column_names[1],
                     orientation='h',
                     color=mycursor.column_names[0]
                    )
        st.plotly_chart(fig,use_container_width=True)
        
    elif questions == '4. How many comments were made on each video, and what are their corresponding video names?':
        mycursor.execute("""SELECT DISTINCT a.video_id AS Video_id, a.title AS Video_Title, b.Total_Comments
                            FROM videos AS a
                            LEFT JOIN (SELECT DISTINCT video_id,COUNT(comment_id) AS Total_Comments
                            FROM comments GROUP BY video_id) AS b
                            ON a.video_id = b.video_id
                            ORDER BY b.Total_Comments DESC""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
          
    elif questions == '5. Which videos have the highest number of likes, and what are their corresponding channel names?':
        mycursor.execute("""SELECT DISTINCT channel_name AS Channel_Name,title AS Title,likes AS Likes_Count 
                            FROM videos
                            ORDER BY likes DESC
                            LIMIT 10""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
        st.write("### :green[Top 10 most liked videos :]")
        fig = px.bar(df,
                     x=mycursor.column_names[2],
                     y=mycursor.column_names[1],
                     orientation='h',
                     color=mycursor.column_names[0]
                    )
        st.plotly_chart(fig,use_container_width=True)
        
    elif questions == '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
        mycursor.execute("""SELECT DISTINCT title AS Title, likes AS Likes_Count
                            FROM videos
                            ORDER BY likes DESC""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
         
    elif questions == '7. What is the total number of views for each channel, and what are their corresponding channel names?':
        mycursor.execute("""SELECT DISTINCT channel_name AS Channel_Name, views AS Views
                            FROM channels
                            ORDER BY views DESC""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
        st.write("### :green[Channels vs Views :]")
        fig = px.bar(df,
                     x=mycursor.column_names[0],
                     y=mycursor.column_names[1],
                     orientation='v',
                     color=mycursor.column_names[0]
                    )
        st.plotly_chart(fig,use_container_width=True)
        
    elif questions == '8. What are the names of all the channels that have published videos in the year 2022?':
        mycursor.execute("""SELECT DISTINCT channel_name AS Channel_Name
                            FROM videos
                            WHERE published_date LIKE '2022%'
                            GROUP BY channel_name
                            ORDER BY channel_name""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
        
    elif questions == '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':
        mycursor.execute("""SELECT DISTINCT channel_name, 
                        AVG(Duration) AS average_duration
                        From videos
                        GROUP BY channel_name""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names
                          )
        st.write(df)
        
        

        
    elif questions == '10. Which videos have the highest number of comments, and what are their corresponding channel names?':
        mycursor.execute("""SELECT DISTINCT channel_name AS Channel_Name,video_id AS Video_ID,comments AS Comments
                            FROM videos
                            ORDER BY comments DESC
                            LIMIT 10""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
        st.write("### :green[Videos with most comments :]")
        fig = px.bar(df,
                     x=mycursor.column_names[1],
                     y=mycursor.column_names[2],
                     orientation='v',
                     color=mycursor.column_names[0]
                    )
        st.plotly_chart(fig,use_container_width=True) 
      
