import contextlib
import logging
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from http.client import IncompleteRead
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
    # Allow properly quitting driver
    profile = webdriver.FirefoxProfile()
    profile.set_preference("dom.disable_beforeunload", True)

    options = Options()
    options.headless = True
    options.add_argument("start-maximized")
    options.add_argument("disable-infobars")
    options.add_argument("--disable-extensions")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-application-cache")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-dev-shm-usage")
    return webdriver.Firefox(options=options, firefox_profile=profile)


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

    last_height = driver.execute_script("return document.documentElement.scrollHeight")

    while True:

        driver.execute_script(f"window.scrollTo(0, {last_height});")
        time.sleep(1)
        new_height = driver.execute_script(
            "return document.documentElement.scrollHeight"
        )

        if new_height == last_height:
            break

        last_height = new_height

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
    video_only_itags: Union[List[int], None]


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

                best_itag = yt.streams.filter().get_highest_resolution()
                video_only_itags = yt.streams.filter(
                    only_video=True, file_extension="mp4"
                )

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
                        best_itag=best_itag.itag if best_itag else None,
                        video_only_itags=(
                            [t.itag for t in video_only_itags]
                            if video_only_itags
                            else None
                        ),
                    )
                )
            except (IncompleteRead, PytubeError) as e:
                logging.error(
                    f"Video with url: {url} from channel {channel.channel} could not be parsed"
                )
                logging.error(f"Error: {e}")

        if i == 0:
            pd.DataFrame(video_metadata).to_csv("data/videos_metadata.csv", index=False)
        else:
            pd.DataFrame(video_metadata).to_csv(
                "data/videos_metadata.csv", mode="a", index=False, header=False
            )

        video_metadata = []


if __name__ == "__main__":
    try:
        parse_metadata()
        driver.close()
    except KeyboardInterrupt:
        driver.close()
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
