from googleapiclient.discovery import build
import psycopg2
import pandas as pd
import streamlit as st

def api_connect():
    api_id = "AIzaSyA5VOrgRQfLImCJUxNfHI_WYkBrJIBHYWI"
    api_service_name = "youtube"
    api_version = "v3"
    youtube = build(api_service_name, api_version, developerKey=api_id)
    return youtube
youtube=api_connect()

#get channel infrmation
def get_channel_info(channel_id):
    request=youtube.channels().list(
                part="snippet,ContentDetails,statistics",
                id=channel_id
    )
    response=request.execute()

    for i in response['items']:
        data={
            "Channel_Name":i["snippet"]['title'],
            "Channel_Id":i["id"],
            "Channel_subscriber":i["statistics"]["subscriberCount"],
            "Total_views":i["statistics"]["viewCount"],
            "Total_videos":i["statistics"]["videoCount"],
            "Channel_description":i["snippet"]["description"],
            "playlist_id":i["contentDetails"]["relatedPlaylists"]["uploads"]
    
        }
    return data

#Get video ids

def get_video_Ids(channel_id):
    video_ids=[]
    response=youtube.channels().list(
                    part="ContentDetails",
                    id=channel_id
    ).execute()
    playlist_Id=response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]

    next_page_token=None
    while True:
        response1=youtube.playlistItems().list(
                        part="snippet",
                        playlistId=playlist_Id,
                        maxResults=50,
                        pageToken=next_page_token

        ).execute()
        for i in range(len(response1['items'])):
            video_ids.append(response1["items"][i]["snippet"]["resourceId"]["videoId"])
        next_page_token=response1.get("nextPageToken")

        if next_page_token is None:
            break
    return video_ids

#Get video details

def get_video_info(video_ids):
    video_data=[]
    for video_id in video_ids:
        request = youtube.videos().list(
                            part="snippet,contentDetails,statistics",
                            id=video_id
            )
        response2=request.execute()
        for i in response2['items']:
            data={
                "channelname":i["snippet"]["channelTitle"],
                "channelID":i["snippet"]["channelId"],
                "videoID":i['id'],
                "Title":i["snippet"]["title"],
                "Thumbnails":i["snippet"]["thumbnails"]["default"]["url"],
                "Tags":i.get("tags"),
                "Description":i["snippet"].get("description"),
                "PublishedDate":i["snippet"]["publishedAt"],
                "Duration":i["contentDetails"]["duration"],
                "Views":i["statistics"].get("viewCount"),
                "Likes":i["statistics"].get("likeCount"),
                "Comments":i["statistics"].get("commentCount"),
                "Favouritecomment":i["statistics"]["favoriteCount"],
                "Definition":i["contentDetails"]["definition"],
                "captionstatus":i["contentDetails"]["caption"]
            }
        video_data.append(data)
    return video_data


#Get playlist details
def get_playlist_info(channel_id):
    next_page_token=None
    playlist_data=[]
    while True:
        request=youtube.playlists().list(
                        part='snippet,contentDetails',
                        channelId=channel_id,
                        maxResults=50,
                        pageToken=None
                        
                        
        )
        response=request.execute()
        for i in response["items"]:
            data={
                "playlist_id":i["id"],
                "Title":i["snippet"]["title"],
                "channel_id":i["snippet"]["channelId"],
                "channelName":i["snippet"]["channelTitle"],
                "publishDate":i["snippet"]["publishedAt"],
                "videocount":i["contentDetails"]["itemCount"]
            }
            playlist_data.append(data)
        next_page_token=response.get("nextPageToken")

        if next_page_token is None:
            break
    return playlist_data    

#Get comment details
def get_comment_info(video_ids):
    next_page_token=None
    comment_data=[]
    while True:
        for video_id in video_ids:
            request=youtube.commentThreads().list(
                            part='snippet',
                            videoId=video_id,
                            maxResults=50,
                            pageToken=None
                        
            )

        response=request.execute()
        for i in  response["items"]:
            data={
                "comment_id":i["snippet"]["topLevelComment"]["id"],
                "video_id":i["snippet"]["topLevelComment"]["snippet"]["videoId"],
                "comment_Text":i["snippet"]["topLevelComment"]["snippet"]["textDisplay"],
                "comment_Author":i["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],
                "comment_Published":i["snippet"]["topLevelComment"]["snippet"]["publishedAt"]
            }
            comment_data.append(data)

        next_page_token=response.get("nextPageToken")

        if next_page_token is None:
            break 
        
        return comment_data
    

#Create Tables in the sqlite3
def create_table():
      mydb= psycopg2.connect(host="localhost",
                              user="postgres",
                              password="yoga0129",
                              dbname="youtube_data",
                              port="5432")
      cursor=mydb.cursor()
      

      try:
            create_query='''create table if not exists channels(Channel_Name varchar(100),
                                                                Channel_Id varchar(80) primary key,
                                                                Channel_subscriber bigint,
                                                                Total_views bigint,
                                                                Total_videos int,
                                                                Channel_description text,
                                                                playlist_id varchar(80))'''
            cursor.execute(create_query)
            mydb.commit()
      except:
            print("channels table already created")


      try:
            create_query2='''create table if not exists playlists(playlist_ID varchar(100) primary key,
                                                            Title varchar(100),
                                                            Channel_ID varchar(100),
                                                            Channel_Name varchar(100),
                                                            PublishedAt timestamp,
                                                            video_count int )'''
            cursor.execute(create_query2)
            mydb.commit()
      except:
            print("playlist table already created")

      

      try:
            create_query3='''create table if not exists videos(channelname varchar(100),
                                                            channelID varchar(100) ,
                                                            videoID varchar(30) primary key,
                                                            Title varchar(150),
                                                            Tags text,
                                                            Thumbnail varchar(200),
                                                            Description text,
                                                            Published_Date timestamp,
                                                            Duration interval,
                                                            Views bigint,
                                                            Likes bigint,
                                                            Comments int,
                                                            Favourite_Count int,
                                                            Definition varchar(10),
                                                            Caption_Status varchar(50)
                                                            )'''
            cursor.execute(create_query3)
            mydb.commit()
      except:
            print("video tables are alredy created")

    
      try:
            create_query4='''create table if not exists comments(comment_id varchar(100) primary key,
                                                            video_id varchar(50),
                                                            comment_Text text,
                                                            comment_Author varchar(150),
                                                            comment_Published timestamp)'''
            cursor.execute(create_query4)
            mydb.commit()
      except:
            print("comment table are already excited")
      return None




def insert_channel_details(channel_id):
    mydb = psycopg2.connect(
        host="localhost",
        user="postgres",
        password="yoga0129",
        dbname="youtube_data",
        port="5432"
    )
    cursor = mydb.cursor()
    
    ch_details = get_channel_info(channel_id)
    df = pd.DataFrame([ch_details]) 

    for index,row in df.iterrows():
        insert_query = '''
        INSERT INTO channels (
            Channel_Name,
            Channel_Id,
            Channel_subscriber,                
            Total_views,
            Total_videos,
            Channel_description,
            playlist_id
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (Channel_Id)
        DO NOTHING;
        '''
        values = (
            row["Channel_Name"],
            row["Channel_Id"],
            row["Channel_subscriber"],
            row["Total_views"],
            row["Total_videos"],
            row["Channel_description"],
            row["playlist_id"]
        )
        try:
            cursor.execute(insert_query, values)
            mydb.commit()
        except Exception as e:
            print(f"Error: {e}")


def insert_playlist_details(channel_id):
    mydb = psycopg2.connect(
            host="localhost",
            user="postgres",
            password="yoga0129",
            dbname="youtube_data",
            port="5432"
        )
    cursor = mydb.cursor()
        
    pl_details=get_playlist_info(channel_id)
    df = pd.DataFrame(pl_details)

        
    data_to_insert = [
            (
                row["playlist_id"],
                row["Title"],
                row["channel_id"],
                row["channelName"],
                row["publishDate"],
                row["videocount"]
            )
            for _, row in df.iterrows()
        ]

        
    insert_query = '''
            INSERT INTO playlists (
                playlist_ID,
                Title,
                Channel_ID,
                Channel_Name,
                PublishedAt,
                video_count
            ) VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (playlist_ID)
            DO NOTHING;
        '''
    try:
            cursor.executemany(insert_query, data_to_insert)
            mydb.commit() 
            
    except Exception as e:
            print(f"Error inserting rows: {e}")



def insert_comment_details(channel_id):
    mydb = psycopg2.connect(
                host="localhost",
                user="postgres",
                password="yoga0129",
                dbname="youtube_data",
                port="5432"
            )
    cursor = mydb.cursor()
            
    vi_ids=get_video_Ids(channel_id)
    com_details=get_comment_info(vi_ids)
    df=pd.DataFrame(com_details)


            
    data_to_insert = [
                (
                    row["comment_id"],
                    row["video_id"],
                    row["comment_Text"],
                    row["comment_Author"],
                    row["comment_Published"]
                    
                )
                for _, row in df.iterrows()
            ]

            
    insert_query = '''
                INSERT INTO comments (
                    comment_id,
                    video_id,
                    comment_Text,
                    comment_Author,
                    comment_Published) VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (comment_id)
                DO NOTHING;
            '''
    try:
        cursor.executemany(insert_query, data_to_insert)
        mydb.commit() 
                
    except Exception as e:
        print(f"Error inserting rows: {e}")



             
def insert_video_details(channel_id):
    mydb = psycopg2.connect(
                    host="localhost",
                    user="postgres",
                    password="yoga0129",
                    dbname="youtube_data",
                    port="5432"
                )
    cursor = mydb.cursor()
                
    vi_ids=get_video_Ids(channel_id)
    vi_details=get_video_info(vi_ids)
    df=pd.DataFrame(vi_details)


                
    data_to_insert = [
                    (
                        row["channelname"],
                        row["channelID"],
                        row["videoID"],
                        row["Title"],
                        row["Thumbnails"],
                        row["Tags"],
                        row["Description"],
                        row["PublishedDate"],
                        row["Duration"],
                        row["Views"],
                        row["Likes"],
                        row["Comments"],
                        row["Favouritecomment"],
                        row["Definition"],
                        row["captionstatus"]
                        
                    )
                    for _, row in df.iterrows()
                ]

            
    insert_query='''INSERT INTO videos (
                        channelname,
                        channelID,
                        videoID,
                        Title,
                        Tags,
                        Thumbnail,
                        Description,
                        Published_Date,
                        Duration,
                        Views,
                        Likes,
                        Comments,
                        Favourite_Count,
                        Definition,
                        Caption_Status
                        ) VALUES (%s, %s, %s, %s, %s,%s, %s, %s, %s, %s,%s, %s, %s, %s, %s)
                    ON CONFLICT (videoID)
                    DO NOTHING;
                '''
    try:
            cursor.executemany(insert_query, data_to_insert)
            mydb.commit() 
                    
    except Exception as e:
            print(f"Error inserting rows: {e}")

def insert_values(channel_id):
    insert_channel_details(channel_id)
    insert_playlist_details(channel_id)
    insert_comment_details(channel_id)
    insert_video_details(channel_id)

    return "the values are successfully inserted"



def tables():
    create_table()

    return "Tables created successfully"

def show_channels_table(conn):
    try:                
        cursor=conn.cursor()
        cursor.execute("SELECT*FROM channels")
        ch_list=cursor.fetchall()
        if ch_list:
             df1=st.dataframe(ch_list)
        else:
            st.write("No data found in channels table")
        return df1
    except Exception as e:
        st.write(f'Error fetching data from channels table: {e}')
        return None


def show_playlists_table(conn):
    try:                
        cursor=conn.cursor()
        cursor.execute("SELECT*FROM playlists")
        pl_list=cursor.fetchall()
        if pl_list:
             df2=st.dataframe(pl_list)
        else:
            st.write("No data found in playlist table")
        return df2
    except Exception as e:
        st.write(f'Error fetching data from playlist table: {e}')
        return None
def show_videos_table(conn):
    try:                
        cursor=conn.cursor()
        cursor.execute("SELECT*FROM videos")
        vi_list=cursor.fetchall()
        if vi_list:
             df3=st.dataframe(vi_list)
        else:
            st.write("No data found in video table")
        return df3
    except Exception as e:
        st.write(f'Error fetching data from video table: {e}')
        return None

def show_comments_table(conn):
    try:                
        cursor=conn.cursor()
        cursor.execute("SELECT*FROM comments")
        com_list=cursor.fetchall()
        if com_list:
             df4=st.dataframe(com_list)
        else:
            st.write("No data found in comments table")
        return df4
    except Exception as e:
        st.write(f'Error fetching data from comments table: {e}')
        return None

with st.sidebar:
    st.title(":red[YOUTUBE DATA HARVESTING AND WAREHOUSING]")
    st.header("Skill Take Away")
    st.caption("Python Developer")
    st.caption("Data Collection")
    st.caption("API Integration")
    st.caption("Data Management using SQL")

channel_id=st.text_input("Enter the Channel ID")

   

if st.button("Migrate to Sql"):
    Table=tables()
    st.success(Table)

if st.button("collect and store the data in sql"):
     insert=insert_values(channel_id)
     st.success(insert)

conn = psycopg2.connect(
                    host="localhost",
                    user="postgres",
                    password="yoga0129",
                    dbname="youtube_data",
                    port="5432")
if conn:
    show_table=st.radio("SELECT THE TABLE FOR VIEW",("CHANNELS","PLAYLISTS","VIDEOS","COMMENTS"))
    if show_table=="CHANNELS":
        show_channels_table(conn)
    if show_table=="PLAYLISTS":
        show_playlists_table(conn)
    if show_table=="VIDEOS":
        show_videos_table(conn)
    if show_table=="COMMENTS":
        show_comments_table(conn)



mydb = psycopg2.connect(
                    host="localhost",
                    user="postgres",
                    password="yoga0129",
                    dbname="youtube_data",
                    port="5432")
cursor=mydb.cursor()
question=st.selectbox("Select your question",("1. All the videos and the channel name",
                                              "2. channels with most numberof videos",
                                              "3. 10 most viewed videos",
                                              "4. comments in each videos",
                                              "5. videos with highest likes",
                                              "6. likes of all videos",
                                              "7. views of each channel",
                                              "8. videos published in the year 2022",
                                              "9. Average duration of all videos in each channels",
                                              "10. videos with highest number of comments"))

if question=="1. All the videos and the channel name":
    query1='''select title as videos,channelname as channelname from videos'''
    cursor.execute(query1)
    mydb.commit()
    t1=cursor.fetchall()
    if t1:
        df=pd.DataFrame(t1,columns=["video title","channel name"])
        st.write(df)
    else:
        st.write("The table is empty")

elif question=="2. channels with most numberof videos":
    query2='''select channel_name as channelname,total_videos as no_videos from channels
                order by total_videos desc'''
    cursor.execute(query2)
    mydb.commit()
    t2=cursor.fetchall()
    if t2:
        df2=pd.DataFrame(t2,columns=["channel name","no of videos"])
        st.write(df2)
    else:
        st.write("The table is empty")

elif question=="3. 10 most viewed videos":
    query3='''select views as views,channelname as channelname,title as video_title from videos
            where views is not null order by views desc limit 10'''
    cursor.execute(query3)
    mydb.commit()
    t3=cursor.fetchall()
    if t3:
        df3=pd.DataFrame(t3,columns=["views","channel name","videotitle"])
        st.write(df3)
    else:
        st.write("The table is empty")

elif question=="4. comments in each videos":
    query4='''select Comments as no_comments,title as videotitle from videos where Comments is not null'''
    cursor.execute(query4)
    mydb.commit()
    t4=cursor.fetchall()
    if t4:
        df4=pd.DataFrame(t4,columns=["no of comments","video title",])
        st.write(df4)
    else:
        st.write("The table is empty")

elif question=="5. videos with highest likes":
    query5='''select title as videotitle,channelname as channelname,likes as likecount from videos where likes is not null 
            order by likes desc'''
    cursor.execute(query5)
    mydb.commit()
    t5=cursor.fetchall()
    if t5:
        df5=pd.DataFrame(t5,columns=["video title","channel name","likecount"])
        st.write(df5)
    else:
        st.write("The table is empty")

elif question=="6. likes of all videos":
    query6='''select likes as likecount,title as videotitle from videos'''
    cursor.execute(query6)
    mydb.commit()
    t6=cursor.fetchall()
    if t6:
        df6=pd.DataFrame(t6,columns=["likecount","video title"])
        st.write(df6)
    else:
        st.write("The table is empty")

elif question=="7. views of each channel":
    query7='''select channel_name as channelname,Total_views as totalviews from channels'''
    cursor.execute(query7)
    mydb.commit()
    t7=cursor.fetchall()
    if t7:
        df7=pd.DataFrame(t7,columns=["channel name","totla views"])
        st.write(df7)
    else:
        st.write("The table is empty")

elif question=="8. videos published in the year 2022":
    query8='''select title as videotitle,Published_Date as videorelease,channelname as channelname from videos
                where extract(year from Published_Date)=2022'''
    cursor.execute(query8)
    mydb.commit()
    t8=cursor.fetchall()
    if t8:
        df8=pd.DataFrame(t8,columns=["video title","video reease date","channel name"])
        st.write(df8)
    else:
        st.write("The table is empty")

elif question=="9. Average duration of all videos in each channels":
    query9='''select channelname as channelname,AVG(Duration) as averageduration from videos group by channelname'''
    cursor.execute(query9)
    mydb.commit()
    t9=cursor.fetchall()
    if t9:

        df9=pd.DataFrame(t9,columns=["channel name","Averageduration"])
        T9=[]
        for index,row in df9.iterrows():
            channel_title=row["channel name"]
            average_duration=row["Averageduration"]
            average_duration_str=str(average_duration)
            T9.append(dict(channeltitle=channel_title,avgduration=average_duration_str))
        df1=pd.DataFrame(T9)
        st.write(df1)

    else:
        st.write("The table is empty")


elif question=="10. videos with highest number of comments":
    query10='''select title as videotitle,channelname as channelname,Comments as comments from videos where Comments
                is not null order by comments desc'''
    cursor.execute(query10)
    mydb.commit()
    t10=cursor.fetchall()
    if t10:
        df10=pd.DataFrame(t10,columns=["video title","channelname","comments"])
        st.write(df10)
    else:
        st.write("The table is empty")

    
                 
