import contextlib
import logging
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Union

import pandas as pd
from bs4 import BeautifulSoup
from dateutil import parser
from googleapiclient.discovery import build
from selenium import webdriver
from selenium.common import NoSuchElementException
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from tqdm import tqdm

# Setup logging
logging.basicConfig(
    handlers=[
        logging.FileHandler(filename="logs/metadata.log", mode="a"),
        logging.StreamHandler(),
    ],
    format="%(asctime)s | %(levelname)s: %(message)s",
    level=logging.INFO,
)


def setup_driver():

    options = Options()
    options.headless = True
    return webdriver.Firefox(options=options)


# Setup selenium driver
driver = setup_driver()

# Setup Google API
GOOGLE_API_KEY = "AIzaSyDf5gV7kqHmTsG5qnvG3cg6ZoPHuLD9xTY"
youtube = build("youtube", "v3", developerKey=GOOGLE_API_KEY)


@dataclass
class ChannelMeta:
    channel: str
    channel_id: str
    channel_url: str
    created_at: datetime
    description: str
    videos_count: int
    subs_count: int
    views_count: int


def get_channel_id(channel: str) -> Union[str, None]:
    """
    Parses the id of a channel based on channel name.
    """
    driver.get(f"https://www.youtube.com/@{channel}")
    time.sleep(1)

    with contextlib.suppress(NoSuchElementException):
        driver.find_element(
            "xpath",
            "/html/body/c-wiz/div/div/div/div[2]/div[1]/div[3]/div[1]/form[1]/div/div/button/span",
        ).click()
        time.sleep(1)

    try:
        channel_id = WebDriverWait(driver, 3).until(
            EC.presence_of_element_located((By.XPATH, "/html/body/link[1]"))
        )
        return channel_id.get_attribute("href").split("/")[-1]
    except TimeoutException:
        logging.error(f"ID of channel {channel} could not be parsed")


def parse_meta(channel: str) -> Union[ChannelMeta, None]:
    """
    Uses Google API to extract metadata from channel
    """
    channel_id = get_channel_id(channel)

    if channel_id is not None:

        response = (
            youtube.channels()
            .list(
                part=["statistics", "snippet"],
                id=channel_id,
            )
            .execute()
        )

        items = response.get("items")[0]
        snippet, stats = items.get("snippet"), items.get("statistics")

        return ChannelMeta(
            channel=channel,
            channel_id=channel_id,
            channel_url=f"https://www.youtube.com/channel/{channel_id}",
            created_at=parser.parse(snippet.get("publishedAt")),
            description=snippet.get("description"),
            videos_count=int(stats.get("videoCount")),
            subs_count=int(stats.get("subscriberCount")),
            views_count=int(stats.get("viewCount")),
        )


def main():

    channels_df = pd.read_csv("data/raw/channels_katrin.csv")
    pbar = tqdm(channels_df["channel"].tolist())

    metadata_df = []
    for channel in pbar:
        pbar.set_description(f"Processing channel {channel}: ")
        metadata_df.append(parse_meta(channel))

    metadata_df = pd.DataFrame([c for c in metadata_df if c is not None])
    channel_df = channels_df.merge(metadata_df, on="channel")
    fname = "data/channel_metadata.csv"
    logging.info(f"Saving metadata of size {channel_df.shape} to {fname}")

    channel_df.to_csv(fname, index=False)


if __name__ == "__main__":
    main()
