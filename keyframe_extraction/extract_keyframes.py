import pandas as pd
from Katna.video import Video
from Katna.writer import KeyFrameDiskWriter
from collections import defaultdict
import os
from pathlib import Path
from tqdm import tqdm
from multiprocessing import Pool
import math
import logging
import argparse
from sklearn.exceptions import FutureWarning
import warnings

# Ignore FutureWarnings from sklearn
warnings.filters(action = "ignore", category = FutureWarning)

# Read metadata
meta_df = pd.read_csv(
    "/zpool/beast-mirror/labour-movements-mobilisation-via-visual-means/CDHU-katrin_youtube_dalia/data_collection/data/final_videos_metadata.csv",
    lineterminator="\n",
    index_col=0,
)
channel_df = pd.read_csv("/zpool/beast-mirror/labour-movements-mobilisation-via-visual-means/CDHU-katrin_youtube_dalia/data_collection/data/channel_metadata.csv")

errors = defaultdict(list)

# Setup logging
logging.basicConfig(
    handlers=[
        logging.FileHandler(filename="keyframes.log", mode="a"),
        logging.StreamHandler(),
    ],
    format="%(asctime)s | %(levelname)s: %(message)s",
    level=logging.INFO,
)

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-v", "--videos_dir", type=str, required=True)
    parser.add_argument("-o", "--output_dir", type=str, required=True)
    parser.add_argument("-c", "--country", type=str, default=None)
    return parser.parse_args()


def extract_keyframes(video_path: str, output_dir: str) -> None:

    vd = Video()

    Path(output_dir).mkdir(parents=True, exist_ok=True)

    diskwriter = KeyFrameDiskWriter(location=output_dir)

    # Calculate number of frames based on length of video
    # 10 frames per minute
    video_id = output_dir.split("/")[-1]
    l = meta_df.loc[meta_df["id"] == video_id, "length"]
    n_frames = max(10, math.ceil((l / 60) * 10))
    try:
        vd.extract_video_keyframes(
            no_of_frames=n_frames, file_path=str(video_path), writer=diskwriter
        )
    except Exception as e:
        logging.error(f"Error extracting keyframes for {video_path}: {e}")
        errors["video_id"].append(video_id)
        errors["error"].append(e)


def extract_channel(
    channel_path: Path, output_dir:str, channel: str, country: str
) -> None:
    """
    Extracts keyframes from all videos in a channel
    """

    pbar = tqdm(
        zip(
            channel_path.glob("*.mp4"),
            [
                f"{output_dir}/{country}/{channel}/{video.name[:-8]}"
                for video in channel_path.glob("*.mp4")
            ],
        )
    )

    # Non parallel version
    for video_path, video_output_dir in pbar:
        extract_keyframes(video_path, video_output_dir)
        pbar.set_description(f"Extracting keyframes for video {video_output_dir}")


def extract_all(videos_dir:str, output_dir:str, country: str = None) -> None:
    """
    Extracts keyframes from all videos in all channels
    """


    channel_dirs = [p for p in Path(videos_dir).glob("*") if p.is_dir()]
    #print(channel_dirs)
    if country is not None:
        country_channels = channel_df.query(f"country == '{country}'")["channel"].tolist()
        channel_dirs = [p for p in channel_dirs if p.name in country_channels]

    pbar = tqdm(channel_dirs)
    for channel_dir in pbar:
        pbar.set_description(f"Extracting keyframes from channel {channel_dir.name}")
        extract_channel(channel_dir, output_dir, channel_dir.name, country)


if __name__ == "__main__":
    args = parse_args()
    extract_all(args.videos_dir, args.output_dir, args.country)
    pd.DataFrame(errors).to_csv("keyframe_errors.csv")