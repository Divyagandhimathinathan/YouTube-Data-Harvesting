import streamlit as st
from streamlit_option_menu import option_menu
from googleapiclient.discovery  import build
import pandas as pd
import mysql.connector  
import pymongo
import numpy as np
import plotly.graph_objects as go
from streamlit_extras.let_it_rain import rain

st.set_page_config(
    page_title="YouTube Data Harvesting and Warehousing Project",
    layout="wide",
    initial_sidebar_state="expanded",
)

def add_bg_from_url():
    st.markdown(
         f"""
         <style>
         .stApp {{
             background-image: url("https://img.freepik.com/free-vector/technology-background_23-2148119855.jpg?size=626&ext=jpg&ga=GA1.2.226878040.1688464285&semt=ais");
             background-attachment: fixed;
             background-size: cover
         }}
         </style>
         """,
         unsafe_allow_html=True
     )

add_bg_from_url() 


# Replace 'YOUR_API_KEY' with your actual API key
API_KEY = ""
youtube = build('youtube','v3',developerKey=API_KEY)

#Homepage
selected = option_menu(None, ["Home", "Upload",'Questions','Analysis'], 
    icons=['house', 'upload', 'question','bar-chart'], 
    menu_icon="cast",default_index=0, orientation="horizontal")
#selected

rain(
    emoji="❄️",
    font_size=54,
    falling_speed=5,
    animation_length="100-500ms",
)

# HOME PAGE
if selected == "Home":
  

# Custom CSS for styling
    st.markdown("""
    <style>
    .header-title {
        text-align: center;
        font-size: 36px;
        color: #FFFFFF;
        padding-bottom: 20px;
    }
    .section-header {
        font-size: 24px;
        color: #FFFFFF;
        padding-top: 20px;
        padding-bottom: 10px;
    }
    .section-text {
        font-size: 18px;
        color: #FFFFFF;
        padding-bottom: 10px;
    }
    .overview-header {
        font-size: 24px;
        color: #FFFFFF;
        padding-top: 20px;
        padding-bottom: 10px;
    }
    .overview-text {
        font-size: 18px;
        color: #FFFFFF;
    }
    </style>
""", unsafe_allow_html=True)

# Title section
    st.markdown("<h1 class='header-title'>YouTube Data Harvesting and Warehousing Project</h1>", unsafe_allow_html=True)

# Domain section
    st.markdown("<h2 class='section-header'>Domain: Social Media</h2>", unsafe_allow_html=True)
    st.markdown("<p class='section-text'>This project is focused on analyzing YouTube channel data to gain insights into video views, likes, and other metrics.</p>", unsafe_allow_html=True)

# Applications used section
    st.markdown("<h2 class='section-header'>Applications Used</h2>", unsafe_allow_html=True)
    st.markdown("<p class='section-text'>Python, MongoDB, YouTube Data API, MySQL, Streamlit</p>", unsafe_allow_html=True)

# Project Overview section
    st.markdown("<h2 class='overview-header'>Project Overview</h2>", unsafe_allow_html=True)
    st.markdown("<p class='overview-text'>The YouTube Data Harvesting and Warehousing Project aims to extract video and channel data from the YouTube Data API, store it in a MongoDB data lake, perform data transformations, and migrate it to a SQL database for structured storage. The project includes a user-friendly Streamlit web app for interactive data analysis and visualization.</p>", unsafe_allow_html=True)
    st.markdown("<ul class='overview-text'>"
            "<li>Data Retrieval: Utilize the YouTube Data API to fetch video and channel data, including video titles, views, likes, and other relevant metrics.</li>"
            "<li>Data Lake (MongoDB): Establish a MongoDB database as a data lake to store the raw data retrieved from the YouTube API. MongoDB's NoSQL nature allows for flexible storage of semi-structured data.</li>"
            "<li>Data Migration & Transformation: Transfer data from MongoDB to a SQL database for structured storage. Perform necessary data transformations and cleaning to ensure data quality and consistency.</li>"
            "<li>Streamlit App: Develop a Streamlit web application to connect to the SQL database and display interactive data visualizations. The app will enable users to explore and analyze YouTube channel data.</li>"
            "<li>Deployment & Security: Deploy the Streamlit app securely on a server or cloud platform. Implement necessary security measures to protect API credentials and database access.</li>"
            "<li>Continuous Improvement: Gather user feedback and continuously enhance the app based on user requirements. Regularly update dependencies and maintain the application for optimal performance.</li>"
            "</ul>",
            unsafe_allow_html=True)

# Bridging a connection with MongoDB Atlas and Creating a new database(youtube_data)
client = pymongo.MongoClient("mongodb://localhost:27017")
db = ''

# CONNECTING WITH MYSQL DATABASE
mydb = mysql.connector.connect(host="127.0.0.1",
                   user="root",
                   password="",
                   database= ""
                   )
mycursor = mydb.cursor(buffered=True)

 #Get the channelid by giving the channel name
def get_channel_id_by_name(channel_name):
    #youtube = googleapiclient.discovery.build('youtube', 'v3', developerKey=API_KEY)

    # Send a search query for the channel name
    search_response = youtube.search().list(
        q=channel_name,
        type='channel',
        part='id,snippet',
        maxResults=1
    ).execute()

    # Extract the channel ID from the response
    if 'items' in search_response:
        items = search_response['items']
        if len(items) > 0:
            channel_id = items[0]['id']['channelId']
            return channel_id

    # If the channel name is not found or there was an error, return None
    return None


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
                                #Tags = video['snippet'].get('tags'),
                                Thumbnail = video['snippet']['thumbnails']['default']['url'],
                                Description = video['snippet']['description'],
                                Published_date = video['snippet']['publishedAt'],
                                Duration = video['contentDetails']['duration'],
                                Views = video['statistics']['viewCount'],
                                Likes = video['statistics'].get('likeCount'),
                                Comments = video['statistics'].get('commentCount'),
                                Favorite_count = video['statistics']['favoriteCount'],
                                Definition = video['contentDetails']['definition'],
                                Caption_status = video['contentDetails']['caption']
                               )
            video_stats.append(video_details)
    return video_stats


# FUNCTION TO GET COMMENT DETAILS
def get_comments_details(v_id, comment_limit=100):
    comment_data = []
    try:
        next_page_token = None
        comments_fetched = 0  # Initialize the counter for fetched comments
    
        while comments_fetched < comment_limit:
            response = youtube.commentThreads().list(part="snippet,replies",
                                                     videoId=v_id,
                                                     maxResults=10,
                                                     pageToken=next_page_token).execute()

            for cmt in response['items']:
                data = dict(Comment_id=cmt['id'],
                            Video_id=cmt['snippet']['videoId'],
                            Comment_text=cmt['snippet']['topLevelComment']['snippet']['textDisplay'],
                            Comment_author=cmt['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            Comment_posted_date=cmt['snippet']['topLevelComment']['snippet']['publishedAt'],
                            Like_count=cmt['snippet']['topLevelComment']['snippet']['likeCount'],
                            Reply_count=cmt['snippet']['totalReplyCount']
                            )
                comment_data.append(data)
                comments_fetched += 1  # Increment the fetched comments counter

                # Check if the desired comment limit is reached
                if comments_fetched >= comment_limit:
                    break

            next_page_token = response.get('nextPageToken')

            # If there are no more comments or the desired limit is reached, break out of the loop
            if next_page_token is None or comments_fetched >= comment_limit:
                break

    except Exception as e:
        print(f"An error occurred: {e}")

    return comment_data

# FUNCTION TO GET CHANNEL NAMES FROM MONGODB
def channel_names():   
    ch_name = []
    for i in db.channel_info.find():
        ch_name.append(i['Channel_name'])
    return ch_name


# EXTRACT AND Migrate
if selected == "Upload":
    tab1,tab2 = st.tabs(["Extract","Migrate"])
    # EXTRACT 
    with tab1:
        st.markdown("<h2 style='text-align:left; color: white;'>YouTube Channel ID Finder</h1>", unsafe_allow_html=True)
        channel_name = st.text_input("Enter Channel name:")
        if st.button("Find Channel ID"):
            if channel_name:
               channel_id = get_channel_id_by_name(channel_name)
               if channel_id:
                st.markdown(f'<p style="color: white;">The channel ID for \'{channel_name}\' is: {channel_id}</p>', unsafe_allow_html=True)
                st.snow()
            else:
                st.error(f"Channel '{channel_name}' not found or an error occurred.")
        #st.markdown("#    ")
        st.markdown("<h2 style='text-align:left; color: white;'>Enter YouTube Channel_ID</h1>", unsafe_allow_html=True)
        hint_markup = '<p style="color: white;">Hint: Copy and Paste the Channel_id from the above Success message</p>'
        ch_id_input = st.markdown(hint_markup, unsafe_allow_html=True)
        ch_id = st.text_input("").split(',')

        if ch_id and st.button("🧠 Extract Data"):
            ch_details = get_channel_details(ch_id)
            st.write(f'<h4 style="color: white;">Extracted data from {ch_details[0]["Channel_name"]} channel</h4>', unsafe_allow_html=True)
            st.table(ch_details)
            st.success("Extracted successfully !!",icon="✅")
            st.balloons()
            st.markdown("""
                    <style>
                    /* Existing CSS styles */

                    /* Customize the background color of st.table */
                    div[data-testid="stTable"] {
                    background-color: #E5E4E2; /* Change this color to your desired background color */
                    border-radius: 10px; /* Add rounded corners */
                    padding: 10px; /* Add padding for better spacing */
                    }

                    /* Existing CSS styles */
                    </style>
                    """, unsafe_allow_html=True)
            

        if st.button("🍃 Upload to MongoDB"):
            with st.spinner('Please Wait a sec...'):
                ch_details = get_channel_details(ch_id)
                v_ids = get_channel_videos(ch_id)
                vid_details = get_video_details(v_ids)
                
                def comments():
                    com_d = []
                    for i in v_ids:
                        com_d+= get_comments_details(i)
                    return com_d
                comm_details = comments()

                collections1 = db.channel_info
                collections1.insert_many(ch_details)

                collections2 = db.video_info
                collections2.insert_many(vid_details)

                collections3 = db.comments_info
                collections3.insert_many(comm_details)
                st.success("Upload to MongoDB successful !!")
                st.balloons()

# Migrate
    with tab2:     
        st.markdown("#   ")
        st.markdown("<h2 style='text-align:left; color: white;'> Select a channel from drop down to Migrate to SQL</h1>", unsafe_allow_html=True)
        
        ch_names = channel_names()
        user_inp = st.selectbox("Select channel",options= ch_names)
        def insert_into_channels():
                collections = db.channel_info
                query = """INSERT INTO channels VALUES(%s,%s,%s,%s,%s,%s,%s,%s)"""
                
                for i in collections.find({"Channel_name" : user_inp},{'_id' : 0}):
                    mycursor.execute(query,tuple(i.values()))
                    mydb.commit()
                
        def insert_into_videos():
            collections1 = db.video_info
            query1 = """INSERT INTO videos VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""

            for i in collections1.find({"Channel_name" : user_inp},{'_id' : 0}):
                mycursor.execute(query1,tuple(i.values()))
                mydb.commit()

        def insert_into_comments():
            collections1 = db.video_info
            collections2 = db.comments_info
            query2 = """INSERT INTO comments VALUES(%s,%s,%s,%s,%s,%s,%s)"""

            for vid in collections1.find({"Channel_name" : user_inp},{'_id' : 0}):
                for i in collections2.find({'Video_id': vid['Video_id']},{'_id' : 0}):
                    mycursor.execute(query2,tuple(i.values()))
                    mydb.commit()

        if st.button("🏃‍♂️ Run"):
            try:
                
                insert_into_channels()
                insert_into_videos()
                insert_into_comments()
                st.success("Migration to MySQL Successful !!",icon="✅")
                st.balloons()
            except:
                st.error("Channel details already transformed !!",icon="🚨")


# Questions
if selected == "Questions":
    
    st.write("## :blue[Select any question ]")
    questions = st.selectbox('Questions',
    ['1.What are the names of all the videos and their corresponding channels?',
    '2. Which channels have the most number of videos, and how many videos do they have?',
    '3. What are the top 10 most viewed videos and their respective channels?',
    '4. How many comments were made on each video, and what are their corresponding video names?',
    '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
    '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
    '7. What is the total number of views for each channel, and what are their corresponding channel names?',
    '8. What are the names of all the channels that have published videos in the year 2022?',
    '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
    '10. Which videos have the highest number of comments, and what are their corresponding channel names?',
    '11.What are the names of all the videos and their corresponding channels?'])

    
    
    if questions == '1. What are the names of all the videos and their corresponding channels?':
        mycursor.execute("""SELECT title AS Video_Title
                            FROM videos
                            """)
        df = pd.DataFrame(mycursor.fetchall(), columns=["Video_Title", "Channel_Name"])
        st.write(df)

         
    elif questions == '2. Which channels have the most number of videos, and how many videos do they have?':
        mycursor.execute("""SELECT channel_name AS Channel_Name, total_videos AS Total_Videos
                            FROM channels
                            ORDER BY total_videos DESC""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
        
        
    elif questions == '3. What are the top 10 most viewed videos and their respective channels?':
        mycursor.execute("""SELECT channel_name AS Channel_Name, title AS Video_Title, views AS Views 
                            FROM videos
                            ORDER BY views DESC
                            LIMIT 10""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
        
        
    elif questions == '4. How many comments were made on each video, and what are their corresponding video names?':
        mycursor.execute("""SELECT a.video_id AS Video_id, a.title AS Video_Title, b.Total_Comments
                            FROM videos AS a
                            LEFT JOIN (SELECT video_id,COUNT(comment_id) AS Total_Comments
                            FROM comments GROUP BY video_id) AS b
                            ON a.video_id = b.video_id
                            ORDER BY b.Total_Comments DESC""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
          
    elif questions == '5. Which videos have the highest number of likes, and what are their corresponding channel names?':
        mycursor.execute("""SELECT channel_name AS Channel_Name,title AS Title,likes AS Likes_Count 
                            FROM videos
                            ORDER BY likes DESC
                            LIMIT 10""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
        
        
    elif questions == '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
        mycursor.execute("""SELECT title AS Title, likes AS Likes_Count
                            FROM videos
                            ORDER BY likes DESC""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
         
    elif questions == '7. What is the total number of views for each channel, and what are their corresponding channel names?':
        mycursor.execute("""SELECT channel_name AS Channel_Name, views AS Views
                            FROM channels
                            ORDER BY views DESC""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
       
        
    elif questions == '8. What are the names of all the channels that have published videos in the year 2022?':
        mycursor.execute("""SELECT channel_name AS Channel_Name
                            FROM videos
                            WHERE published_date LIKE '2022%'
                            GROUP BY channel_name
                            ORDER BY channel_name""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
        
    elif questions == '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':
        mycursor.execute("""SELECT channel_name AS Channel_Name,
                            AVG(duration)/60 AS "Average_Video_Duration (mins)"
                            FROM videos
                            GROUP BY channel_name
                            ORDER BY AVG(duration)/60 DESC""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
       
        
    elif questions == '10. Which videos have the highest number of comments, and what are their corresponding channel names?':
        mycursor.execute("""SELECT channel_name AS Channel_Name,video_id AS Video_ID,comments AS Comments
                            FROM videos
                            ORDER BY comments DESC
                            LIMIT 10""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)

    elif questions == '11. What are the names of all the videos and their corresponding channels?':
        mycursor.execute("""SELECT title AS Video_Title
                            FROM videos
                            """)
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
        
   
       
#Analysis
if selected == "Analysis":
    
    def get_video_data():
        st.markdown("<h3 style='text-align:left; color: white;'>Video Likes</h3>",unsafe_allow_html=True)
        mycursor.execute("""SELECT channel_name AS Channel_Name,title as Video_Title,comments AS Comments,likes
                            FROM videos
                            ORDER BY comments DESC
                            """)
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        return df

    # Get data from the SQL
    video_data = get_video_data()
  
    # Create a bar chart using Plotly
    fig = go.Figure()

    # Add trace for the bar chart
    fig.add_trace(go.Bar(
        x=video_data['Channel_Name'],
        y=video_data['likes'],
        marker=dict(color='dark blue'),  # Customize the bar color
    ))

    # Update the layout
    fig.update_layout(
        title='Video likes by Channel',
        xaxis_title='Channel Name',
        yaxis_title='Likes',
    )

    # Show the Plotly bar chart using Streamlit
    st.plotly_chart(fig)



    def get_videoline_data():
        st.markdown("<h3 style='text-align:left; color: white;'>Video View count</h3>",unsafe_allow_html=True)
        mycursor.execute("""SELECT channel_name AS Channel_Name, views AS Views
                            FROM channels
                            ORDER BY views DESC""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        return df

    # Get data from the SQL
    video_data = get_videoline_data()
  
    # Create a bar chart using Plotly
    fig = go.Figure()

    # Add trace for the bar chart
    fig.add_trace(go.Line(
        x=video_data['Channel_Name'],
        y=video_data['Views'],
        marker=dict(color='red'),  # Customize the bar color
    ))

    # Update the layout
    fig.update_layout(
        title='Video viewcount by Channel',
        xaxis_title='Channel Name',
        yaxis_title='views',
    )

    # Show the Plotly line chart using Streamlit
    st.plotly_chart(fig)




