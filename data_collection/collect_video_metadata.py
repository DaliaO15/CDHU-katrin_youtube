import contextlib
import logging
import time
from dataclasses import dataclass
from datetime import datetime
from typing import List, Union

import pandas as pd
from bs4 import BeautifulSoup
from pytube import YouTube, extract
from pytube.exceptions import PytubeError
from selenium import webdriver
from selenium.common import NoSuchElementException
from selenium.webdriver.firefox.options import Options
from tqdm import tqdm

# Setup logging
logging.basicConfig(
    handlers=[
        logging.FileHandler(filename="logs/video_metadata.log", mode="a"),
        logging.StreamHandler(),
    ],
    format="%(asctime)s | %(levelname)s: %(message)s",
    level=logging.INFO,
)


def setup_driver():

    options = Options()
    options.headless = True
    return webdriver.Firefox(options=options)


driver = setup_driver()


def parse_urls(channel_url: str) -> List[str]:
    # Go to video page
    driver.get(channel_url)
    time.sleep(1)

    with contextlib.suppress(NoSuchElementException):
        driver.find_element(
            "xpath",
            "/html/body/c-wiz/div/div/div/div[2]/div[1]/div[3]/div[1]/form[1]/div/div/button/span",
        ).click()

    lastHeight = driver.execute_script("return document.documentElement.scrollHeight")

    while True:

        driver.execute_script(f"window.scrollTo(0, {lastHeight});")
        time.sleep(1)
        newHeight = driver.execute_script(
            "return document.documentElement.scrollHeight"
        )

        if newHeight == lastHeight:
            break

        lastHeight = newHeight

    soup = BeautifulSoup(driver.page_source, "html.parser")
    return [
        f"youtube.com{video.get('href')}"
        for video in soup.find_all(
            "a",
            {
                "class": "yt-simple-endpoint focus-on-expand style-scope ytd-rich-grid-media"
            },
        )
    ]


@dataclass
class VideoMeta:
    title: str
    id: str
    length: int
    views: int
    description: str
    keywords: List[str]
    age_restricted: bool
    author: str
    pub_date: Union[datetime, None]
    raiting: float
    channel: str
    url: str
    best_itag: Union[int, None]


def parse_metadata() -> None:

    channels_df = pd.read_csv("data/channel_metadata.csv")

    pbar = tqdm(list(channels_df[["channel", "channel_url"]].itertuples()))

    for i, channel in enumerate(pbar):

        pbar.set_description(f"Processing {channel.channel}")
        pbar2 = tqdm(parse_urls(f"{channel.channel_url}/videos"))

        video_metadata = []

        for url in pbar2:

            try:
                yt = YouTube(url)
                pbar2.set_description(f"Processing video {yt.title}")

                video_metadata.append(
                    VideoMeta(
                        title=yt.title,
                        id=extract.video_id(url),
                        length=yt.length,
                        views=yt.views,
                        description=yt.description,
                        keywords=yt.keywords,
                        age_restricted=yt.age_restricted,
                        author=yt.author,
                        pub_date=yt.publish_date,
                        raiting=yt.rating,
                        channel=channel.channel,
                        url=url,
                        best_itag=yt.streams.filter(
                            progressive=False
                        ).get_highest_resolution(),
                    )
                )
            except PytubeError as e:
                logging.error(
                    f"Video with url: {url} from channel {channel.channel} could not be parsed"
                )
                logging.error(e)

        if i == 0:
            pd.DataFrame(video_metadata).to_csv("data/videos_metadata.csv", index=False)
        else:
            pd.DataFrame(video_metadata).to_csv(
                "data/videos_metadata.csv", mode="a", index=False, header=False
            )

        video_metadata = []


if __name__ == "__main__":
    metadata_df = parse_metadata()
