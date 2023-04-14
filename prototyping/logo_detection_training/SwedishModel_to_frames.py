#%cd /zpool/beast-mirror/labour-movements-mobilisation-via-visual-means/CDHU-katrin_youtube_dalia/yolov5 

import torch
import utils
from IPython import display
from IPython.display import clear_output
from pathlib import Path
import matplotlib.pyplot as plt
import matplotlib.image as mpimg
import pandas as pd
import yaml
import os
import glob as glob
import requests
import cv2
import subprocess

#%matplotlib inline


def run_detection(model_path:str, test_data_path:str, project_name:str, test_name:str):
    # Create comand to run 
    comand = ['python', 'detect.py', '--weights', model_path, '--source', test_data_path, '--project', project_name, '--name', test_name, '--save-txt', '--save-conf', '--nosave']
    # Run
    subprocess.run(comand)

    
def run_detection_directory(model_path:str, test_data_dir:str):
    # Access the channel folder 
    channel_dirs = [filename for filename in os.listdir(test_data_dir) if not filename.startswith('.')]
    for channel_d in channel_dirs:
        sub_folder_path = os.path.join(test_data_dir,channel_d)
        # Access the video's frames folder 
        videos_dir = [filename for filename in os.listdir(sub_folder_path) if not filename.startswith('.')]
        for video_d in videos_dir:
            video_path = os.path.join(sub_folder_path,video_d)
            
            # Apply the model to each video's frame
            name = channel_d + '/' + video_d
            run_detection(model_path, video_path, 'Sweden_analysis', name)
            
            
def main():
    # Set relevant paths
    test_swe_path = '/zpool/beast-mirror/labour-movements-mobilisation-via-visual-means/youtube_video_frames/sweden/'
    #test_swe_path = '/zpool/beast-mirror/labour-movements-mobilisation-via-visual-means/swedish_test_data/sweden'
    swe_model_path = 'Sverige/lr0.01_b32_e100_f12_V5M6/weights/best.pt'

    # Run detection 
    print('Running detections')
    run_detection_directory(swe_model_path, test_swe_path)
    
if __name__ == "__main__":
    main()
    run_detection_directory(swe_model_path, test_swe_path)
