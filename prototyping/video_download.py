import pandas as pd
import os
from yt_dlp import YoutubeDL
import logging

logging.basicConfig(level=logging.DEBUG, filename='../Data_collection/data/logs/video_download.log', 
                    format='%(asctime)s : %(levelname)s : %(message)s',
                    datefmt='%d-%b-%y %H:%M:%S')


def create_folder(folder):
    try: # if it is possible to create the folder
        os.mkdir(folder)
    except: # if the folder was not created successfuly, raise a warning
        logging.info('There is an error with the folder')

logging.basicConfig(level=logging.DEBUG, filename='video_download.log', 
                    format='%(asctime)s : %(levelname)s : %(message)s',
                    datefmt='%d-%b-%y %H:%M:%S')

def create_folder(folder):
    try: # if it is possible to create the folder
        os.mkdir(folder)
    except: # if the folder was not created successfuly, raise a warning
        logging.info('There is an error with the folder')

def video_download(df, root_folder:str): 
    # df: metadata. df must contain a column called url where the urls to the videos are stored
    # root_folder: folder where videos are stored

    for idx in range(0,df.shape[0]):

        # Access the video url from data frame
        video_url = 'https://www.' + df['url'].iloc[idx]
        channel_name = df['channel'].iloc[idx].replace(" ", "_")
        len = df['length'].iloc[idx]

        # Create a folder for that channel
        channel_folder = root_folder + channel_name + '/'
        
        # Create a new folder for the channel
        if not os.path.exists(channel_folder):
            create_folder(channel_folder)

        ydl_opts = {'outtmpl': channel_folder + '%(id)s-%(format_id)s.%(ext)s',
                    'format': 'bestvideo[ext=mp4]/mp4',
                    'overwrites': False,
                    'retries': 15,
                    'concurrent-fragments':8}
        
        if len <= 600:
            try:
                with YoutubeDL(ydl_opts) as ydl:
                    ydl.download([video_url]) # Downloads the best format by default 
            #except:
            #    logging.info(f'Error when downloading video {video_url}')

            except Exception as e:
                logging.error(f'Error when downloading video {video_url} in channel {channel_name}')
                logging.error(f"Error: {e}")
        else:
            logging.info(f'The video {video_url} exceeded the time constrain. Length {len} seconds.')

def main():
    file_test = '../Data_collection/data/videos_metadata.csv'
    df = pd.read_csv(file_test,lineterminator='\n')
    df = df.sample(frac = 0.01, random_state=495)
    video_download(df, root_folder='Videos/') 

if __name__ == "__main__":
    main()