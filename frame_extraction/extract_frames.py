import pathlib as pl
from tqdm import tqdm
from multiprocessing import Pool
import cv2
import logging

VIDEOS_DIR = "/zpool/beast-mirror/labour-movements-mobilisation-via-visual-means/CDHU-katrin_youtube_dalia/CDHU-katrin_youtube/prototyping/Videos"
OUTPUT_DIR = "frames_out"


# Setup logging
logging.basicConfig(
    handlers=[
        logging.FileHandler(filename="frames.log", mode="a"),
        logging.StreamHandler(),
    ],
    format="%(asctime)s | %(levelname)s: %(message)s",
    level=logging.INFO,
)


class FrameExtractor:
    def __init__(
        self,
        videos_dir: str,
        output_dir: str,
        every_n_sec: int = 10,
        n_workers: int = 4,
    ):
        self.videos_dir = videos_dir
        self.output_dir = output_dir
        self.every_n_sec = every_n_sec
        self.n_workers = n_workers

    def extract_frames(self, video_path, channel):

        cap = cv2.VideoCapture(video_path)
        fps = int(cap.get(cv2.CAP_PROP_FPS))
        count = 0
        while cap.isOpened():
            sucess, frame = cap.read()
            if sucess:
                try:
                    fname = video_path.split("/")[-1][:11]
                    # Check if output directory exists
                    pl.Path(f"{self.output_dir}/{channel}/{fname}").mkdir(
                        parents=True, exist_ok=True
                    )

                    cv2.imwrite(
                        f"{self.output_dir}/{channel}/{fname}/{fname}_{count}.jpg",
                        frame,
                    )
                    count += fps * self.every_n_sec
                    cap.set(cv2.CAP_PROP_POS_FRAMES, count)
                except Exception as e:
                    logging.error(f"Error extracting frames for {video_path}: {e}")
            else:
                cap.release()
                break

    def channel_worker(self, channel):
        channel_path = pl.Path(f"{self.videos_dir}/{channel}")
        videos = [str(p) for p in channel_path.glob("*.mp4")]
        with Pool(self.n_workers) as p:
            p.starmap(
                self.extract_frames,
                zip(videos, [channel] * len(videos))
            )

    def run(self):

        channel_dirs = [
            p.name for p in pl.Path(self.videos_dir).iterdir() if p.is_dir()
        ]
        pbar = tqdm(channel_dirs)
        logging.info(
            f"Extracting frames from {len(channel_dirs)} channels in {self.videos_dir}"
        )
        logging.info(f"Using {self.n_workers} workers")

        for channel in pbar:
            self.channel_worker(channel)
            pbar.set_description(f"Extracting frames for {channel}")


if __name__ == "__main__":
    fe = FrameExtractor(VIDEOS_DIR, OUTPUT_DIR, every_n_sec=10, n_workers=20)
    fe.run()
