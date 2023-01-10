from pytube import YouTube
from pytube import Channel
from pytube import extract
import pandas as pd
from collections import defaultdict
import os
import logging

logging.basicConfig(level=logging.DEBUG, filename='../Data_collection/data/logs/video_metadata.log', 
                    format='%(asctime)s : %(levelname)s : %(message)s',
                    datefmt='%d-%b-%y %H:%M:%S')

def video_metadata_v2(df):
    keys = ['channel', 'url', 'id', 'title', 'author', 'description', 'keywords', 'length_in_secs', 
            'nbr_views', 'age_resticted', 'publication_date', 'rating']

    metadata = defaultdict(list)

    for idx in range(0,df.shape[0]):

        # Access the video url from data frame
        video_url = df['url'].iloc[idx]
        channel_name = df['channel'].iloc[idx]

        try:
            # Create a youtube object to use the API
            yt = YouTube(video_url)

            # To track the video
            logging.info( yt.title)

            # Extract metadata
            info = [channel_name, video_url, extract.video_id(video_url), yt.title, yt.author, yt.description,
            yt.keywords, yt.length, yt.views, yt.age_restricted, yt.publish_date, yt.rating]
        except:
            print('Video could not be accessed')
            info = [channel_name, video_url]+[None]*10
        
        # Store in dictionary 
        for k, v in zip(keys,info):
            metadata[k].append(v)
                    
        # Track 
        if idx % 200 == 0:
            print(f'We are in video {idx} in channel {yt.author}')

    video_metadata = pd.DataFrame.from_dict(metadata)

    return video_metadata

def main():
    file_test = 'video_urls_FULL.csv'
    df = pd.read_csv(file_test)
    df = df.drop(columns = 'Unnamed: 0')
    #df = df[255:265].reset_index(drop= True)
    video_metadata = video_metadata_v2(df) 
    video_metadata.to_csv(index=False, path_or_buf='../Data_collection/data/videos_metadata_FULL.csv') 

if __name__ == "__main__":
    main()