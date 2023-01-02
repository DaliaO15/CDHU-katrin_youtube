from pytube import YouTube
from pytube import extract
import pandas as pd
import youtube_dl
import os
import logging

logging.basicConfig(level=logging.DEBUG, filename='../Data_collection/data/logs/video_download.log', 
                    format='%(asctime)s : %(levelname)s : %(message)s',
                    datefmt='%d-%b-%y %H:%M:%S')


def create_folder(folder):
    try: # if it is possible to create the folder
        os.mkdir(folder)
    except: # if the folder was not created successfuly, raise a warning
        logging.info('There is an error with the folder')

def video_download_tube(youtube_video, url, path):
    # Create a youtube object to use the API
    video_id = extract.video_id(url)
    itag = youtube_video.streams.filter(only_video=True).asc().first().itag # get the highest resolution possible 
    stream = youtube_video.streams.get_by_itag(itag)
    # Download video
    stream.download(output_path = path, filename = video_id + '-' + str(itag) + '.mp4')

def video_download_dl(url, path):
    ydl_opts = {'outtmpl': path + '%(id)s-%(format_id)s.%(ext)s'}
    with youtube_dl.YoutubeDL(ydl_opts) as ydl:
        ydl.download(['https://www.'+ url]) # Downloads the best format by default 
 
def video_download(df,root_folder): # df must contain a column called url where the urls to the videos are stored

    for idx in range(0,df.shape[0]):
        
        # Access the video url from data frame
        video_url = df['url'].iloc[idx]
        channel_name = df['channel'].iloc[idx]

        # Create a folder for that channel
        channel_folder = root_folder + channel_name + '/'
  
        yt = YouTube(video_url)
        len = yt.length
        
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
                #video_download_dl(video_url, channel_folder)
                pass
        else:
            logging.info(f'The video {video_url} exceeded the time constrain. Length {len} seconds.')
       

def main():
    file_test = 'video_urls_FULL.csv'
    root = '../Data_collection/data/videos/'
    df = pd.read_csv(file_test)
    df = df.drop(columns = 'Unnamed: 0')
    df = df[255:265].reset_index(drop= True)
    video_download(df,root) 

if __name__ == "__main__":
    main()