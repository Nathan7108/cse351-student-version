"""
Course: CSE 351
Assignment: 06
Author: [Your Name]

Instructions:

- see instructions in the assignment description in Canvas

""" 

import multiprocessing as mp
import os
import cv2
import numpy as np

from cse351 import *

# Folders
INPUT_FOLDER = "faces"
STEP1_OUTPUT_FOLDER = "step1_smoothed"
STEP2_OUTPUT_FOLDER = "step2_grayscale"
STEP3_OUTPUT_FOLDER = "step3_edges"

# Parameters for image processing
GAUSSIAN_BLUR_KERNEL_SIZE = (5, 5)
CANNY_THRESHOLD1 = 75
CANNY_THRESHOLD2 = 155

# Allowed image extensions
ALLOWED_EXTENSIONS = ['.jpg']

# ---------------------------------------------------------------------------
def create_folder_if_not_exists(folder_path):
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
        print(f"Created folder: {folder_path}")

# ---------------------------------------------------------------------------
def task_convert_to_grayscale(image):
    if len(image.shape) == 2 or (len(image.shape) == 3 and image.shape[2] == 1):
        return image # Already grayscale
    return cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)

# ---------------------------------------------------------------------------
def task_smooth_image(image, kernel_size):
    return cv2.GaussianBlur(image, kernel_size, 0)

# ---------------------------------------------------------------------------
def task_detect_edges(image, threshold1, threshold2):
    if len(image.shape) == 3 and image.shape[2] == 3:
        print("Warning: Applying Canny to a 3-channel image. Converting to grayscale first for Canny.")
        image = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
    elif len(image.shape) == 3 and image.shape[2] != 1 : # Should not happen with typical images
        print(f"Warning: Input image for Canny has an unexpected number of channels: {image.shape[2]}")
        return image # Or raise error
    return cv2.Canny(image, threshold1, threshold2)

# ---------------------------------------------------------------------------
def process_images_in_folder(input_folder,              # input folder with images
                             output_folder,             # output folder for processed images
                             processing_function,       # function to process the image (ie., task_...())
                             load_args=None,            # Optional args for cv2.imread
                             processing_args=None):     # Optional args for processing function

    create_folder_if_not_exists(output_folder)
    print(f"\nProcessing images from '{input_folder}' to '{output_folder}'...")

    processed_count = 0
    for filename in os.listdir(input_folder):
        file_ext = os.path.splitext(filename)[1].lower()
        if file_ext not in ALLOWED_EXTENSIONS:
            continue

        input_image_path = os.path.join(input_folder, filename)
        output_image_path = os.path.join(output_folder, filename) # Keep original filename

        try:
            # Read the image
            if load_args is not None:
                img = cv2.imread(input_image_path, load_args)
            else:
                img = cv2.imread(input_image_path)

            if img is None:
                print(f"Warning: Could not read image '{input_image_path}'. Skipping.")
                continue

            # Apply the processing function
            if processing_args:
                processed_img = processing_function(img, *processing_args)
            else:
                processed_img = processing_function(img)

            # Save the processed image
            cv2.imwrite(output_image_path, processed_img)

            processed_count += 1
        except Exception as e:
            print(f"Error processing file '{input_image_path}': {e}")

    print(f"Finished processing. {processed_count} images processed into '{output_folder}'.")

# ---------------------------------------------------------------------------
def smoothing_worker(input_queue, output_queue, worker_id):
    while True:
        task = input_queue.get()
        if task is None:
            break
        
        input_file, output_file = task
        try:
            image = cv2.imread(input_file)
            if image is None:
                continue
            
            smoothed = task_smooth_image(image, GAUSSIAN_BLUR_KERNEL_SIZE)
            output_queue.put((smoothed, output_file))
        except Exception as e:
            print(f"Error in smoothing worker: {e}")

def grayscale_worker(input_queue, output_queue, worker_id):
    while True:
        task = input_queue.get()
        if task is None:
            break
        
        image, output_file = task
        try:
            gray_image = task_convert_to_grayscale(image)
            output_queue.put((gray_image, output_file))
        except Exception as e:
            print(f"Error in grayscale worker: {e}")

def edge_worker(input_queue, worker_id):
    while True:
        task = input_queue.get()
        if task is None:
            break
        
        image, output_file = task
        try:
            edges = task_detect_edges(image, CANNY_THRESHOLD1, CANNY_THRESHOLD2)
            cv2.imwrite(output_file, edges)
        except Exception as e:
            print(f"Error in edge worker: {e}")

# ---------------------------------------------------------------------------
def run_image_processing_pipeline():
    print("Starting image processing pipeline...")

    create_folder_if_not_exists(STEP3_OUTPUT_FOLDER)
    
    total_images = 0
    for filename in os.listdir(INPUT_FOLDER):
        if filename.lower().endswith('.jpg'):
            total_images += 1
    
    num_smooth_workers = 4
    num_gray_workers = 4
    num_edge_workers = 4
    
    print(f"Processing {total_images} images using {num_smooth_workers} smoothing workers, {num_gray_workers} grayscale workers, and {num_edge_workers} edge workers")
    
    smooth_queue = mp.Queue()
    gray_queue = mp.Queue()
    edge_queue = mp.Queue()
    
    smooth_procs = []
    gray_procs = []
    edge_procs = []
    
    for i in range(num_smooth_workers):
        proc = mp.Process(target=smoothing_worker, args=(smooth_queue, gray_queue, i))
        proc.start()
        smooth_procs.append(proc)
    
    for i in range(num_gray_workers):
        proc = mp.Process(target=grayscale_worker, args=(gray_queue, edge_queue, i))
        proc.start()
        gray_procs.append(proc)
    
    for i in range(num_edge_workers):
        proc = mp.Process(target=edge_worker, args=(edge_queue, i))
        proc.start()
        edge_procs.append(proc)
    
    print(f"Processing images from '{INPUT_FOLDER}' to '{STEP3_OUTPUT_FOLDER}'...")
    images_loaded = 0
    
    for filename in os.listdir(INPUT_FOLDER):
        if filename.lower().endswith('.jpg'):
            input_file = os.path.join(INPUT_FOLDER, filename)
            output_file = os.path.join(STEP3_OUTPUT_FOLDER, filename)
            smooth_queue.put((input_file, output_file))
            images_loaded += 1
    
    for i in range(num_smooth_workers):
        smooth_queue.put(None)
    
    for proc in smooth_procs:
        proc.join()
    
    for i in range(num_gray_workers):
        gray_queue.put(None)
    
    for proc in gray_procs:
        proc.join()
    
    for i in range(num_edge_workers):
        edge_queue.put(None)
    
    for proc in edge_procs:
        proc.join()
    
    print(f"Finished processing. {images_loaded} images processed into '{STEP3_OUTPUT_FOLDER}'.")

    print("\nImage processing pipeline finished!")
    print(f"Original images are in: '{INPUT_FOLDER}'")
    print(f"Grayscale images are in: '{STEP1_OUTPUT_FOLDER}'")
    print(f"Smoothed images are in: '{STEP2_OUTPUT_FOLDER}'")
    print(f"Edge images are in: '{STEP3_OUTPUT_FOLDER}'")


# ---------------------------------------------------------------------------
if __name__ == "__main__":
    log = Log(show_terminal=True)
    log.start_timer('Processing Images')

    # check for input folder
    if not os.path.isdir(INPUT_FOLDER):
        print(f"Error: The input folder '{INPUT_FOLDER}' was not found.")
        print(f"Create it and place your face images inside it.")
        print('Link to faces.zip:')
        print('   https://drive.google.com/file/d/1eebhLE51axpLZoU6s_Shtw1QNcXqtyHM/view?usp=sharing')
    else:
        run_image_processing_pipeline()

    log.write()
    log.stop_timer('Total Time To complete')