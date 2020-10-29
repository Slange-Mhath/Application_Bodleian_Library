import requests
import json
import logging
import os
from pprint import pprint
from pathlib import Path
from csv import DictReader
from itertools import groupby
from operator import itemgetter
from collections import ChainMap


# Specify the log file
logging.basicConfig(filename='tad_migration.log',level=logging.DEBUG)

# Specify the Path to the images
image_folder = Path("/Users/sebastianlange/Documents/Edinburgh/projects/TaD_data/Tad_Images")

# Specify the Path to the csv
csv_path = "/Users/sebastianlange/Documents/Edinburgh/projects/TaD_data/Photos_Rights_Statements.csv"


# DSPACE Credentials

api_base_url = "https://digitalpreservation.is.ed.ac.uk"
endpoint_path = "/rest/login"
endpoint = "{}{}".format(api_base_url,endpoint_path)
ds_collection = "377e93ba-c66f-46a1-930b-553323f98398"
ds_user = input("Enter your email for DSpace:")
ds_password = input("Enter your password for DSpace:")

login_data = {
    "email": ds_user,
    "password": ds_password
}

headers = {
    'Content-Type': 'application/json', 'Accept': 'application/json'
}


# Login to ArchivesSpace and return SessionID

as_base_url = "http://lac-archivesspace-live2.is.ed.ac.uk"
as_user = input("Enter your ArchivesSpace username:")
as_password = input("Enter your ArchivesSpace password:")
as_archival_repo = "2"
as_url_port = "8089"





def load_image_info():
    with open(csv_path) as read_obj:
        image_info = DictReader(read_obj)
        image_info_list = list(image_info)
        for info in image_info_list:
            info['id'] = info.pop('\ufeffContributor ID')
            info['id'] = int(info['id'])
        return image_info_list


def create_image():
    tad_images = []
    images = os.listdir(image_folder)
    for image in images:
        tad_image = {}
        tad_image['id'] = image.split(" - ")[0]
        if tad_image['id'].isnumeric():
            tad_image['id'] = int(tad_image['id'])
            tad_image['name'] = image
            if len(os.listdir(image_folder / image)) > 1:
                pprint(f"Warning: More than one Image {os.listdir(image_folder / image)}")
            elif len(os.listdir(image_folder / image)) == 0:
                pprint(f"The folder {tad_image['name']} is empty")
            else:
                pprint(Path(f"{image_folder}/{image}/{os.listdir(image_folder / image)}")) # Throws an error cause Hector has no image LOL
                tad_image['path'] = Path(f"{image_folder}/{image}/{os.listdir(image_folder / image)[0]}") #   TODO: For Windows use backslash instead of /
                tad_images.append(tad_image)
    return tad_images




def login_to_dspace():
    response = requests.post(endpoint, data=login_data)
    set_cookie = response.headers["Set-Cookie"].split(";")[0]
    session_id = set_cookie[set_cookie.find("=") + 1:]
    return session_id

def format_metadata(key, value, lang="en"):
    """Reformats the metadata for the REST API."""
    return {'key': key, 'value': value, 'language': lang}

# create a dspace record with metadata
def create_dspace_record(metadata, ds_collection, session_id):
    item = {"type": "item", "metadata": metadata} #
    collection_url = "{}/rest/collections/{}/items".format(api_base_url, ds_collection)
    response = requests.post(collection_url,
                             cookies={"JSESSIONID": session_id},
                             data=json.dumps(item),
                             headers=headers
                             )
    response.raise_for_status()
    response_object = response.json()
    return response_object


# Uploads the image into the DSPACE object
def upload_image(ds_object_link, image_to_upload, ds_object_tag):
    headers = {'Content-Type': 'application/json', 'Accept': 'application/json'}
    with open(image_to_upload, 'rb') as content:
        ds_object_tag = str(ds_object_tag) + ".jpg"
        pprint(ds_object_tag)
        requests.post("{}/{}/bitstreams?name={}".format(api_base_url,ds_object_link,ds_object_tag),
                                  data=content,
                                  headers=headers,
                                  cookies={"JSESSIONID": login_to_dspace()})
        print(f"image with the id: {ds_object_tag} successfully uploaded")


def as_login():
    files = {
        'password': (None, as_password),
    }
    response = requests.post(f"{as_base_url}:{as_url_port}/users/{as_user}/login", files=files)
    return response.json()['session']


# Todo: has to be changed to link image to the agent in AS which has the same number as the folder number of the image
def get_as_agent(as_base_url, as_url_port, as_session_id, agent_id): #use image_id as agent_id as they are the same
    link_to_agent = f'{as_base_url}:{as_url_port}/agents/people/{agent_id}'
    as_headers = {
        'X-ArchivesSpace-Session': as_session_id
    }
    response = requests.get(link_to_agent, as_headers)
    as_agent = response.json()
    return as_agent

def update_as_agent(as_base_url, as_url_port, as_session_id, agent_id, as_agent, link_to_image, rights_statement):
    link_to_agent = f'{as_base_url}:{as_url_port}/agents/people/{agent_id}'
    as_headers = {
        'X-ArchivesSpace-Session': as_session_id
    }
    as_agent['notes'][0]['label'] = "Image"
    as_agent['notes'][0]['subnotes'][0]['content'] = "<img src='{}.jpg'/>".format(link_to_image)
    as_agent['notes'].append({'jsonmodel_type': 'note_bioghist', 'label': 'Image Rights', 'publish':False, 'subnotes': [{'content': rights_statement, 'jsonmodel_type': 'note_text', 'publish': False}]})
    response = requests.post(link_to_agent, headers=as_headers, data=json.dumps(as_agent))
    print(response.status_code)



def main():
    image_info = load_image_info()
    image_list = create_image()
    ds_session_id = login_to_dspace()
    as_session_id = as_login()
    # merge the list of dicts in image_info with the one in image_list
    grouped_images = groupby(sorted(image_info + image_list, key=itemgetter("id")), itemgetter("id"))
    images = [dict(ChainMap(*g)) for k, g in grouped_images]
    # iterate over the images reformat the information to apply to DSpace
    for image in images:
         if "name" in image and "id" in image and "Title of Photo" in image and "Rights Statement" in image:
             image_formatted = [format_metadata("dc.identifier", image['id']),
                                format_metadata("dc.title", image['name']),
                                format_metadata("dc.description", image['Title of Photo']),
                                format_metadata("dc.rights", image['Rights Statement'])
                                ]
             ds_object = create_dspace_record(image_formatted, ds_collection, ds_session_id)
             upload_image(ds_object['link'], image['path'], image['id'])
             # create the link to the image stored in Dspace
             link_to_image = "{}/bitstream/handle/{}/{}".format(api_base_url, ds_object['handle'], image['id'])
             # get the as agent which needs to be updated
             as_agent = get_as_agent(as_base_url, as_url_port, as_session_id, image['id'])
             # update the as agent with the desired link info
             if "notes" in as_agent and as_agent['notes']:
                 update_as_agent(as_base_url, as_url_port, as_session_id, image['id'], as_agent, link_to_image, image['Rights Statement'])
                 pprint(f" the image with the id: {image['id']} the name: {image['name']} and the title: {image['Title of Photo']} with the rights: {image['Rights Statement']} has been uploaded to {link_to_image}")
             else:
                 pprint(f"The Agent with the id {image['id']} is not in ArchivesSpace or {as_agent} has no label")

         else:
             pprint(f"{image} has not all necessary parameter")


main()