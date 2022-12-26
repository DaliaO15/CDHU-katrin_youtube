from pytube import YouTube
from pytube import Channel
from pytube import extract
import pandas as pd
from collections import defaultdict
import os


def create_folder(folder):
    try: # if it is possible to create the folder
        os.mkdir(folder)
    except: # if the folder was not created successfuly, raise a warning
        print('There is an error with the folder')

def video_download(youtube_video, path, name):
    itag = youtube_video.streams.filter(only_video=True).asc().first().itag # get the highest resolution possible 
    stream = youtube_video.streams.get_by_itag(itag)
    stream.download(output_path = path, filename = name)

def video_metadata(df): # df must contain a column called url where the urls to the videos are stored
    # To store the information
    info = defaultdict(list)

    for idx in range(0,df.shape[0]):

        # Access the video url from data frame
        video_url = df['url'].iloc[idx]
        print(f'Video_url: {video_url}')
        channel_name = df['channel'].iloc[idx]
        print(f'Channel: {channel_name}')

        # Create a youtube object to use the API
        yt = YouTube(video_url)

        # ---------- Collect metadata 

        # Collect information from the video and store
        info['video_title'].append(yt.title)
        video_id = extract.video_id(video_url)
        info['video_id'].append(video_id)
        info['videos_length'].append(yt.length)
        info['videos_views'].append(yt.views)
        info['videos_description'].append(yt.description)
        info['videos_keywords'].append(yt.keywords)
        info['videos_age_resticted'].append(yt.age_restricted)
        info['videos_author'].append(yt.author)
        info['videos_pub_date'].append(yt.publish_date)
        info['videos_rating'].append(yt.rating)

        # ---------- Download videos 

        # Create a folder for that channel
        channel_folder = os.getcwd() + '/Videos/' + channel_name
        #print(f'Folder: {channel_folder}')

        if os.path.exists(channel_folder):
            # Download the video here
            try:
                video_download(yt, channel_folder, video_id+'.mp4')
            except:
                print('Video not downloaded')
        else:
            create_folder(channel_folder)
            # Then download the viedeo here
            try:
                video_download(yt, channel_folder, video_id+'.mp4')
            except:
                print('Video not downloaded')


        if idx % 1000 == 0:
            print(f'We are in video {idx} in channel {yt.author}')

    return info

if __name__ == "__main__":
    df = pd.read_csv('video_urls.csv')
    df = df.drop(columns='Unnamed: 0')  
    df = df[255:265].reset_index(drop= True)
    # Getting all the metadata 
    info = video_metadata(df)