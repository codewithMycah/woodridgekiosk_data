import pandas as pd
from pymongo import MongoClient
import gridfs
from PIL import Image
import io
import os

# Connect to MongoDB Atlas
atlas_connection_string = "mongodb+srv://db_admin:dQERttM5UIXM41cy@cluster0.ofqfm.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
client = MongoClient(atlas_connection_string)

# Specify your database and collection
db = client['woodridgekiosk']  # Database name
collection = db['businesses']  # Collection name
fs = gridfs.GridFS(db)  # GridFS for storing images

# Read Excel file
excel_file_path = 'data/businesses.xlsm'
df = pd.read_excel(excel_file_path)

# Function to store image in GridFS and return its ID
def store_image_in_gridfs(image_path):
    try:
        with open(image_path, 'rb') as img_file:
            img_data = img_file.read()
            # Store image in GridFS
            img_file = fs.put(img_data, filename=os.path.basename(image_path))
            return img_file
    except Exception as e:
        print(f"Error storing image: {e}")
        return None
    
def clear_collection():
    print('Emptying collection...')
    return collection.delete_many({})
    
# Function to process each business and insert it into MongoDB
def insert_business_data(df):
    for _, row in df.iterrows():
        business_data = {
            "business_name": row['business_name'],
            "address": row['address'],
            "contact_number": row['contact_number'],
            "website_url": row['website_url'],
            "business_category": row['business_category'],
            "hours_mon": row['hours_mon'],
            "hours_tue": row['hours_tue'],
            "hours_wed": row['hours_wed'],
            "hours_thu": row['hours_thu'],
            "hours_fri": row['hours_fri'],
            "hours_sat": row['hours_sat'],
            "hours_sun": row['hours_sun'],
            "loc_lat": row['loc_lat'],
            "loc_long": row['loc_long'],
            "coordinates": row['coordinates'],
            "image": None  # Placeholder for image
        }

        # Get image path from the Excel data
        image_path = row['path']
        if os.path.exists(image_path):
            image_file_id = store_image_in_gridfs(image_path)
            business_data['image'] = image_file_id

        # Insert into MongoDB
        try:
            collection.insert_one(business_data)
            print(f"Inserted {row['business_name']} into the database.")
        except Exception as e:
            print(f"Error inserting {row['business_name']}: {e}")

# Clear dollection
clear_collection()
# Run the script to insert data
insert_business_data(df)