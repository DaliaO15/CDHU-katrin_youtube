import os
import pandas as pd
from fastparquet import write

def result_processing(path_to_results_dir:str, path_to_res_file:str, country:str):
    ### This will return a parque file 
    
    # File to store the results
    file_path = path_to_res_file+f'results_{country}.parquet'
    
    # Channel names 
    channel_dirs = [filename for filename in os.listdir(path_to_results_dir) if not filename.startswith('.')]
    for channel_d in channel_dirs:
        sub_folder_path = os.path.join(path_to_results_dir,channel_d)
        # Access the video's frames folder 
        videos_dir = [filename for filename in os.listdir(sub_folder_path) if not filename.startswith('.')]
        for video_d in videos_dir:
            video_path = os.path.join(sub_folder_path,video_d)
            labels_dir_path = os.path.join(video_path,'labels/')
            
            if os.listdir(labels_dir_path): # Labels dir is not empty:
                
                # Access labels information. This are txt files
                labels_files = [filename for filename in os.listdir(labels_dir_path) if not filename.startswith('.')]
                for label_f in labels_files:
                    
                    with open(os.path.join(labels_dir_path,label_f), 'r') as f:
                        lines = f.readlines()
                        # Write the labels to a data frame
                        df = pd.DataFrame([l.split() for l in lines],columns=['class_label',
                                'x_center','y_center','width','height','confidence_level'])
                        df.insert(0,'Frame', label_f.split('.')[0])
                        df.insert(0, 'Video_id', video_d)
                        df.insert(0,'Channel', channel_d)
       
                        # If the file does not exists, create it
                        if not os.path.isfile(file_path): 
                            write(file_path, df)
                        else: # Otherwise, write on it
                            write(file_path, df, append=True)
            else: 
                pass
            
    return file_path 

    
def transf_parque(parque_file_path:str, class_0_name:str, class_1_name:str):
    res = pd.read_parquet(parque_file_path, engine='fastparquet')
    mapping = {'1': class_1_name, '0': class_0_name}
    res = res.replace({'class_label': mapping})
    # Converting to CSV
    res.to_csv(parque_file_path.split('.par')[0]+'.csv', index = False)
    
    
def main():
    
    # Set folders 
    main_folder = '../../../yolov5/Spain_analysis'
    end_results_folder = '../../data_collection/data/'
    country = 'spain'
    
    # Process results 
    res = result_processing(main_folder, end_results_folder, country)
    transf_parque(res,'e_iu', 'e_pp')
    print('*****DONE*****')

if __name__ == "__main__":
    main()