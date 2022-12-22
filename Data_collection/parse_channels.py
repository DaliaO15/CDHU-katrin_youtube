import contextlib
import time
from dataclasses import dataclass
from typing import List

import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.common import NoSuchElementException
from selenium.webdriver.firefox.options import Options
from tqdm import tqdm
from webdriver_manager.firefox import GeckoDriverManager


def setup_driver():

    options = Options()
    options.headless = True
    try:
        driver = webdriver.Firefox(
            executable_path=GeckoDriverManager(), options=options
        )
    except Exception as e:
        # logging.exception(e)
        # logging.info("Installing geckodriver")
        driver = webdriver.Firefox(
            executable_path=GeckoDriverManager().install(), options=options
        )
    return driver


def parse_urls(channel_url: str, driver) -> List[str]:
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
class Video:
    channel: str
    url: str


def get_video_urls():

    driver = setup_driver()

    channels_df = pd.read_csv("channels.csv")

    results = []
    pbar = tqdm(list(channels_df[["channel", "channel_videos_url"]].itertuples()))

    for channel in pbar:
        pbar.set_description(f"Processing {channel.channel}")

        results.extend(
            Video(channel=channel.channel, url=url)
            for url in parse_urls(channel.channel_videos_url, driver)
        )

    urls_df = pd.DataFrame(results)

    urls_df.to_csv("video_urls.csv")


if __name__ == "__main__":
    get_video_urls()
