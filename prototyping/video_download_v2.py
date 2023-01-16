import pandas as pd
import youtube_dl
import os
import logging
from http.client import IncompleteRead
from youtube_dl.utils import ExtractorError 

logging.basicConfig(level=logging.DEBUG, filename='video_download.log', 
                    format='%(asctime)s : %(levelname)s : %(message)s',
                    datefmt='%d-%b-%y %H:%M:%S')

def create_folder(folder):
    try: # if it is possible to create the folder
        os.mkdir(folder)
    except: # if the folder was not created successfuly, raise a warning
        logging.info('There is an error with the folder')


def video_download_dl(url, format, path):
    ydl_opts = {'outtmpl': path + '%(id)s-%(format_id)s.%(ext)s', 'format':str(format)}
    with youtube_dl.YoutubeDL(ydl_opts) as ydl:
        ydl.download(['https://www.'+ url]) # Downloads the best format by default 


def video_download(df, root_folder:str, col1:str, col2:str): # df must contain a column called url where the urls to the videos are stored
                                    # and two more columns that stores the itag for the best and second best 
                                    # resolutions provided by pytube

    for idx in range(0,df.shape[0]):
        
        # Access the video url from data frame
        video_url = df['url'].iloc[idx]
        channel_name = df['channel'].iloc[idx].replace(" ", "_")
        len = df['length_in_secs'].iloc[idx]

        itag = df[col1].iloc[idx]# df[col2].iloc[idx][0] column with lists 
        if itag == None: 
            itag = int(df[col2].iloc[idx])

        # Create a folder for that channel
        channel_folder = root_folder + channel_name + '/'
        
        # Create a new folder for the channel
        if os.path.exists(channel_folder):
            pass
        else:
            create_folder(channel_folder)

        # Get videos with less than 10 min length
        if len <= 600: 
            # Download the video here
            try:
                video_download_dl(video_url, int(itag), channel_folder)
            except (IncompleteRead, ExtractorError) as e:
                logging.error(
                    f"Video {video_url} from channel {channel_name} could not be downloaded"
                )
                logging.error(f"Error: {e}")
        else:
            logging.info(f'Video {video_url} from channel {channel_name} exceeded the time constrain. Length {len} seconds.')
       
        if idx > 3:
            break      

def main():
    file_test = 'videos_metadata_TEST.csv'
    root = 'Videos/'
    df = pd.read_csv(file_test)
    video_download(df, root, 'best_itag', 'sec_itag') 

if __name__ == "__main__":
    main()