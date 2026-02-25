import os
import sys
from pathlib import Path
import kaggle


def download_dataset():
    """Download weather dataset from Kaggle."""
    
    # Configuration
    dataset = "ramima/weather-dataset-in-antwerp-belgium"
    download_path = "/app/data"
    
    print(f"Starting download of dataset: {dataset}")
    print(f"Download path: {download_path}")
    
    # Ensure download directory exists
    Path(download_path).mkdir(parents=True, exist_ok=True)
    
    # Check if files already exist
    csv_files = list(Path(download_path).glob("*.csv"))
    if csv_files:
        print(f"Found {len(csv_files)} CSV files already downloaded:")
        for file in csv_files:
            print(f"  - {file.name}")
        print("Skipping download.")
        return
    
    try:
        # Download dataset using Kaggle API
        print("Downloading dataset from Kaggle...")
        kaggle.api.dataset_download_files(
            dataset,
            path=download_path,
            unzip=True,
            quiet=False
        )
        
        # Verify download
        csv_files = list(Path(download_path).glob("*.csv"))
        if csv_files:
            print(f"\n✓ Successfully downloaded {len(csv_files)} file(s):")
            for file in csv_files:
                size_mb = file.stat().st_size / (1024 * 1024)
                print(f"  - {file.name} ({size_mb:.2f} MB)")
        else:
            print("\n✗ Error: No CSV files found after download")
            sys.exit(1)
            
    except Exception as e:
        print(f"\n✗ Error downloading dataset: {e}")
        print("\nTroubleshooting:")
        print("1. Make sure you have a Kaggle account")
        print("2. Create an API key at https://www.kaggle.com/settings")
        print("3. Set environment variables KAGGLE_USERNAME and KAGGLE_KEY")
        print("4. Or mount kaggle.json file to /root/.kaggle/kaggle.json")
        sys.exit(1)


if __name__ == "__main__":
    download_dataset()
