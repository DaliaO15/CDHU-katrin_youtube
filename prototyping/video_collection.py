from pytube import YouTube
from pytube import Channel
import pandas as pd
import youtube_dl
import os
from pytube import extract
import os
import logging

logging.basicConfig(level=logging.DEBUG, filename='../Data_collection/data/logs/video_download.log', 
                    format='%(asctime)s : %(levelname)s : %(message)s',
                    datefmt='%d-%b-%y %H:%M:%S')


def create_folder(folder):
    try: # if it is possible to create the folder
        os.mkdir(folder)
    except: # if the folder was not created successfuly, raise a warning
        print('There is an error with the folder')

def video_download_tube(youtube_video, url, path):
    # Create a youtube object to use the API
    #youtube_video = YouTube(url)
    video_id = extract.video_id(url)
    itag = youtube_video.streams.filter(only_video=True).asc().first().itag # get the highest resolution possible 
    stream = youtube_video.streams.get_by_itag(itag)
    # Download video
    stream.download(output_path = path, filename = video_id + '-' + str(itag) + '.mp4')

def video_download_dl(url, path):
    ydl_opts = {'outtmpl': path + '%(id)s-%(format_id)s.%(ext)s'}
    with youtube_dl.YoutubeDL(ydl_opts) as ydl:
        ydl.download(['https://www.'+ url]) # Downloads the best format by default 
 
def video_download(df): # df must contain a column called url where the urls to the videos are stored

    for idx in range(0,df.shape[0]):
        
        # Access the video url from data frame
        video_url = df['url'].iloc[idx]
        #print(f'Video_url: {video_url}')
        channel_name = df['channel'].iloc[idx]

        # Create a folder for that channel
        channel_folder = os.getcwd() + '/Videos/' + channel_name + '/'
        #print(f'Folder: {channel_folder}')
  
        yt = YouTube(video_url)
        len = yt.length
        #video_id = extract.video_id(video_url)
        
        # Create a new folder for the channel
        if os.path.exists(channel_folder):
            pass
        else:
            create_folder(channel_folder)

        # Get videos with less than 10 min length
        if len <= 600: 
            # Download the video here
            try:
                video_download_tube(yt, video_url, channel_folder)
            except:
                video_download_dl(video_url, channel_folder)
                #print('Video not downloaded')
        else:
            logging.info(f'The video {video_url} exceeded the time constrain. Length {len} seconds.')
       

def main():
    file_test = 'video_urls_FULL.csv'
    df = pd.read_csv(file_test)
    df = df.drop(columns = 'Unnamed: 0')
    df = df[255:265].reset_index(drop= True)
    video_metadata = video_download(df) 

if __name__ == "__main__":
    main()