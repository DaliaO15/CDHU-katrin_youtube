import contextlib
import logging
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from http.client import IncompleteRead
from pathlib import Path
from typing import List, Union

import pandas as pd
from bs4 import BeautifulSoup
from pytube import YouTube, extract
from pytube.exceptions import ExtractError, HTMLParseError
from selenium import webdriver
from selenium.common import NoSuchElementException
from selenium.webdriver.firefox.options import Options
from tqdm import tqdm

# import ssl
# ssl._create_default_https_context = ssl._create_unverified_context

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
    options = Options()
    options.headless = True
    options.set_preference("dom.disable_beforeunload", True)
    options.add_argument("start-maximized")
    options.add_argument("disable-infobars")
    options.add_argument("--disable-extensions")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-application-cache")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-dev-shm-usage")
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


def parse_metadata() -> None:

    channels_df = pd.read_csv("data/channel_metadata.csv")

    pbar = tqdm(list(channels_df[["channel", "channel_url"]].itertuples()))

    for channel in pbar:

        pbar.set_description(f"Processing {channel.channel}")
        pbar2 = tqdm(parse_urls(f"{channel.channel_url}/videos"))

        video_metadata = []

        for url in pbar2:

            # rety 5 times
            for i in range(5):
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
                        )
                    )

                except (IncompleteRead, ExtractError, HTMLParseError):
                    pbar2.set_description(f"Collection failed. Retry attempt {i + 1}.")
                else:
                    break
            else:
                logging.error(f"Failed to get metadata for {url} after 5 retries")

        p = Path("data/videos_metadata.parquet")

        if not p.is_file():
            pd.DataFrame(video_metadata).to_parquet(p, engine="fastparquet")
        else:
            pd.DataFrame(video_metadata).to_parquet(
                p, engine="fastparquet", append=True
            )


def tear_down():
    driver.stop_client()
    driver.close()
    driver.quit()


if __name__ == "__main__":
    try:
        parse_metadata()
        tear_down()
    except KeyboardInterrupt:
        tear_down()
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
