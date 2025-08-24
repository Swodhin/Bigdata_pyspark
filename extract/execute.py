import os, sys
from zipfile import ZipFile

def extract_zip_file(zip_filename, output_dir):
    """Extracts a local Netflix dataset zip file."""
    with ZipFile(zip_filename, 'r') as zip_file:
        zip_file.extractall(output_dir)
    print(f"Extracted zip file to: {output_dir}")

    print("Removing the zip file...")
    os.remove(zip_filename)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Extraction path is required")
        print("Example usage:")
        print("python3 extract.py /home/bigdata/data/extract")
    else:
        try:
            print("Starting Netflix Extraction Engine...")
            EXTRACT_PATH = sys.argv[1]

            # Adjust this filename to match the actual file you put in EXTRACT_PATH
            zip_file = os.path.join(EXTRACT_PATH, "archive.zip")

            extract_zip_file(zip_file, EXTRACT_PATH)

            print("Extraction Successfully Completed!!!")
        except Exception as e:
            print(f"Error: {e}")
