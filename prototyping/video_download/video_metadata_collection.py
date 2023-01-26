from pytube import YouTube
from pytube import Channel
from pytube import extract
import pandas as pd
from collections import defaultdict
import os
import logging

logging.basicConfig(level=logging.DEBUG, filename='video_metadata.log', 
                    format='%(asctime)s : %(levelname)s : %(message)s',
                    datefmt='%d-%b-%y %H:%M:%S')

def itags(youtube_video, url, file):

    try: 
        itag_lst = youtube_video.streams.filter(only_video=True).asc() # get the highest resolution possible

        if len(itag_lst) >= 2:
            bests = [int(itag_lst[i].itag) for i in range(0,2)]
        elif len(itag_lst) == 1:
            bests = [int(itag_lst.itag), None]
        else:
            logging.info(f'The lenght of itag_list is zero for video {url}')
            bests = [None]*2
            file.write(f'Url with no itags: {url} \n')
    except:
        logging.info(f'Itags not retrieved. Video: {url}')
        file.write(f'Itags not retrieved. Video: {url} \n')
        bests = [None]*2
        
    return bests


def video_metadata(df):
    keys = ['channel', 'url', 'id', 'title', 'author', 'description', 'keywords', 'length_in_secs', 
            'nbr_views', 'age_resticted', 'publication_date', 'rating','best_itag','sec_itag']

    metadata = defaultdict(list)
    #destino = '../Data_collection/data/'
    catched_urls = open('catched_urls_v2.txt', 'w')

    for idx in range(0,df.shape[0]):

        # Access the video url from data frame
        video_url = df['url'].iloc[idx]
        channel_name = df['channel'].iloc[idx]

        # Create a youtube object to use the API
        yt = YouTube(video_url)

        # Get itags
        itag_lst = itags(yt,video_url,catched_urls)
        
        # Extract metadata
        info = [channel_name, video_url, extract.video_id(video_url), yt.title, yt.author, yt.description,
        yt.keywords, yt.length, yt.views, yt.age_restricted, yt.publish_date, yt.rating, itag_lst[0],itag_lst[1]]

        # Store in dictionary 
        for k, v in zip(keys,info):
            metadata[k].append(v)
                    
        # Track 
        if idx % 100 == 0:
            print(f'We are in video {idx} in channel {yt.author}')

    return metadata


def main():
    file_test = 'video_urls_FULL.csv'
    df = pd.read_csv(file_test)
    df = df.sample(n= 500, random_state=495).drop(columns='Unnamed: 0')
    info = video_metadata(df) 
    info = pd.DataFrame.from_dict(info)
    info.to_csv(index=False, path_or_buf='videos_metadata_TEST.csv') 

if __name__ == "__main__":
    main()