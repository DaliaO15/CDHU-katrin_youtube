import torch
import torch.nn as nn
import torchvision.transforms as transforms
from torchvision.models import resnet50, ResNet50_Weights

import numpy as np
import pandas as pd
from fastparquet import write

import os
import glob as glob

import cv2
from PIL import Image



class FinalLayer(nn.Module):
    """Modified last layer for resnet50 for your dataset"""
    def __init__(self):
        super(FinalLayer, self).__init__()
        self.fc = nn.Linear(2048, 12)  # Assuming you have 12 output classes
        self.sigmoid = nn.Sigmoid()

    def forward(self, x):
        out = self.fc(x)
        out = self.sigmoid(out)
        return out

def modified_resnet50():
    # Load pretrained resnet50 with a modified last fully connected layer
    model = resnet50(weights=ResNet50_Weights.DEFAULT)
    model.fc = FinalLayer()
    return model

def model_load():
    # Load the modified ResNet-50 model
    model = modified_resnet50()

    # Load the protest prediction model
    model_checkpoint = torch.load('../../protest-detection-violence-estimation/model_best.pth.tar')
    model.load_state_dict(model_checkpoint['state_dict'])
    model.eval()

    if torch.cuda.is_available():
        model = model.to('cuda')
    
    return model

def image_transform(image):
    # Define image transformations
    preprocess = transforms.Compose([
        transforms.Resize((224, 224)),  # Resize to match the model's input size
        transforms.ToTensor(),           # Convert to tensor
    ])
    return preprocess(image)
    
def protest_inference(model, path_to_img:str):
    # Load your input image
    image = Image.open(path_to_img)

    # Preprocess the image
    input_tensor = image_transform(image)

    # Add a batch dimension (1 image)
    input_tensor = input_tensor.unsqueeze(0)

    # Move all to cuda 
    if torch.cuda.is_available():
        input_tensor = input_tensor.to('cuda')

    # Perform inference
    with torch.no_grad():
        output = model(input_tensor)

    output = output.to('cpu')

    # Convert the output tensor to list
    output_list = output[0].tolist()
    
    return output_list

def run_detection_directory(path_to_res_file:str, country:str, test_data_dir: str):
    
    # Set up model 
    model = model_load()
    
    # Set frame counter
    frame_counter = 0
    
    # File to store the results (parqur file)
    file_path = path_to_res_file+f'results_{country}.parquet'
    
    # Access the channel folder 
    channel_dirs = [filename for filename in os.listdir(test_data_dir) if not filename.startswith('.')]
    
    
    for channel_d in channel_dirs: # Get each channel 
        channel_name = channel_d

        sub_folder_path = os.path.join(test_data_dir, channel_d)

        # Access the video's frames folder 
        videos_dir = [filename for filename in os.listdir(sub_folder_path) if not filename.startswith('.')]

        for video_d in videos_dir:
            video_name = video_d

            video_path = os.path.join(sub_folder_path, video_d)

            frames = [f for f in os.listdir(video_path) if not f.startswith('.')] # Get the frames 

            for frame in frames:
                frame_path = os.path.join(video_path, frame)

                output_list = protest_inference(model, frame_path)

                # Extract country, channel name, video, and frame from the file path
                parts = frame_path.split('/')
                channel_name = parts[-3]
                video_name = parts[-2]
                frame_name = parts[-1]
                frame_counter += 1

                # Write the labels to a data frame
                data = [channel_name, video_name, frame_name] + output_list
                df = pd.DataFrame([data], columns=['Channel', 'Video', 'Frame', "protest", "violence", "sign", "photo", "fire", "police", "children", "group_20", "group_100", "flag", "night", "shouting"])

                # If the file does not exists, create it
                if not os.path.isfile(file_path): 
                    write(file_path, df)
                else: # Otherwise, write on it
                    write(file_path, df, append=True)
                        
                    

            
def main():
    # Set relevant paths
    country = 'italy'
    test_swe_path = f'/zpool/beast-mirror/labour-movements-mobilisation-via-visual-means/youtube_video_frames/{country}/'
    res_path = '/zpool/beast-mirror/labour-movements-mobilisation-via-visual-means/protest_derection_results/'
    
    # Call the function and store the resulting DataFrame
    print('Running detections')
    run_detection_directory(res_path, country, test_swe_path)
    
if __name__ == "__main__":
    main()
