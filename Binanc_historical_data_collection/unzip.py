import os
import zipfile

intervals = ["1d", "4h", "1h", "15m", "1m"]

def unzip_files():
    for interval in intervals:
        if os.path.exists(interval):
            for filename in os.listdir(interval):
                if filename.endswith(".zip"):
                    zip_file_path = os.path.join(interval, filename)
                    extract_folder = os.path.join(interval, filename.replace(".zip", ""))
                    
                    if not os.path.exists(extract_folder):
                        os.makedirs(extract_folder)
                
                    try:
                        with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
                            zip_ref.extractall(extract_folder)
                        print(f"Successfully extracted {zip_file_path} to {extract_folder}")
                    except Exception as e:
                        print(f"Failed to extract {zip_file_path}: {e}")
        else:
            print(f"Folder {interval} does not exist.")

if __name__ == "__main__":
    unzip_files()
