import requests
import json
import logging
from pprint import pprint
from pathlib import Path
from csv import DictReader
from agentarchives import archivesspace
import re

# This file is used for migrating media files, creating digital objects and preserving them with their metadata in
# DSpace and linking them to the respective Archival Object in ArchivesSpace using the component_id and
# track_id from the data dump in a CSV file as unique identifier.
# This script is written to be run on Python 3.8 and applies to the PEP8 standard.

# Specify the log file

logging.basicConfig(filename='tad_migration.log', level=logging.DEBUG)

# Provide the Path to the data_dump in this case a CSV

path_to_csv = "/Users/sebastianlange/Documents/Edinburgh/projects/TaD/catalogue_tracks.csv"

# Provide the path to the media files which are supposed to be created as Digital object and migrated

path_to_files = "/Users/sebastianlange/Documents/Edinburgh/projects/TaD_data/"

# For the creation of the track as digital object which should be preserved in DSPace a DSpace Login is needed and asked
# for when running the script. In addition to that you have to provide a baseurl leading to the DSpace instance and a
# collection where the objects should be stored

# DSPACE credentials

ds_api_base_url = "https://test.digitalpreservation.is.ed.ac.uk"
ds_endpoint_path = "/rest/login"
ds_endpoint = f"{ds_api_base_url}{ds_endpoint_path}"
ds_collection = "b8ef34ee-1b49-460b-8fe4-00a39d9a737d"
ds_user = input("Enter your email for DSpace:")
ds_password = input("Enter your password for DSpace:")
headers = {
    'Content-Type': 'application/json', 'Accept': 'application/json'
}

login_data = {
    "email": ds_user,
    "password": ds_password
}

# To link the digital object created in DSpace to the respective Archival object in ArchivesSpace you have to provide
# a login for ArchivesSpave as well as a base URL and a repository where the archival objects live.

# Login to ArchivesSpace and return the Session-ID

as_base_url = "http://lac-archives-test.is.ed.ac.uk"
as_archival_repo = "18"
as_url_port = "8089"
as_user = input("Enter your username for ArchivesSpace:")
as_password = input("Enter your password for ArchivesSpace:")


def as_login():
    files = {
        'password': (None, as_password),
    }
    response = requests.post(f"{as_base_url}:{as_url_port}/users/{as_user}/login", files=files)
    return response.json()['session']


# Get the Archival Objects from the respective Repo in the EAD standard and return it as json

def get_as_data(as_session_id):
    as_headers = {
        'X-ArchivesSpace-Session': as_session_id
    }
    link_to_ao_in_ead = f'{as_base_url}:{as_url_port}/repositories/{as_archival_repo}/resources/86851/tree'
    response = requests.get(link_to_ao_in_ead,
                            headers=as_headers)
    print(response.status_code)
    print(response.content)
    return response.json()


# Read the CSV File with the information about the tracks, which are supposed to be migrated

def get_catalogue_tracks():
    with open(path_to_csv,
              'r') as read_obj:
        tracks_as_dict = DictReader(read_obj)
        tracks_as_list = list(tracks_as_dict)
        return tracks_as_list


def create_track_list():
    track_list = get_catalogue_tracks()
    tracks_with_paths_list = []
    for track in track_list:
        data_folder = Path(path_to_files)
        if len(track['mp3']) > 0:  # check if there is a file specified to the entry in the csv
            if len(track['title']) < 0:  # check if the file has a title otherwise name it "untitled"
                track['title'] = "Untitled"
            path_to_track = data_folder / track['mp3']  # create the path to the file
            track.update({'track_path': path_to_track})  # update the track dict. with the path of the track
            track_tag = re.sub(r"/([0-9_]+)/\1\.", "/\\1.", track['mp3']).replace("/", "_")  # replace the "/" with "_"
            track.update({'track_tag': track_tag})  # update the track dict. with the tag of the track
            tracks_with_paths_list.append(track)  # add the track as dict. (with the keys: id, mp3, title, track_path,
            # track_tag) to the tracks_with_paths_list.
    return tracks_with_paths_list


def format_metadata(key, value, lang="en"):
    """Reformats the metadata for the REST API."""
    return {'key': key, 'value': value, 'language': lang}


# returns the path to the file on the storage (not needed but handy if you have to switch the path of the file storage)

def path_to_stored_file(track):
    data_folder = Path(path_to_files)
    if len(track['mp3']) > 0:
        local_path = data_folder / track['mp3']
        return local_path


# DSpace Login

def login_to_dspace():
    response = requests.post(ds_endpoint, data=login_data)
    set_cookie = response.headers["Set-Cookie"].split(";")[0]
    session_id = set_cookie[set_cookie.find("=") + 1:]
    return session_id


# create a dspace record with the formatted track metadata

def create_dspace_record(metadata, ds_collection, session_id):
    item = {"type": "item", "metadata": metadata}
    collection_url = f"{ds_api_base_url}/rest/collections/{ds_collection}/items"
    response = requests.post(collection_url,
                             cookies={"JSESSIONID": session_id},
                             data=json.dumps(item),
                             headers=headers
                             )
    response_object = response.json()
    return response_object


# Uploads the track into the DSPACE object

def upload_track(ds_object_link, track_to_open, ds_object_tag):
    track_list = create_track_list()
    test_object = track_list[0]
    headers = {'Content-Type': 'application/json', 'Accept': 'application/json'}
    with open(track_to_open, 'rb') as content:
        requests.post(f"{ds_api_base_url}/{ds_object_link}/bitstreams?name={ds_object_tag}",
                      data=content,
                      headers=headers,
                      cookies={"JSESSIONID": login_to_dspace()})
        pprint("track uploaded")


# Use the ArchivesSpaceClient from Agentarchive to add a digital object (with the track) to the Archival Object

def link_dip_to_as(as_base_url, as_user, as_password, as_url_port, archival_obj_id, track_id, track_title,
                   link_to_bitstream):
    client = archivesspace.ArchivesSpaceClient(as_base_url, as_user, as_password, as_url_port)
    client.add_digital_object(
        parent_archival_object=f"/repositories/{as_archival_repo}/archival_objects/{archival_obj_id}",
        identifier=track_id, title=track_title, uri=link_to_bitstream, object_type="sound_recording")
    pprint("Linked to AS")


def main():
    as_login()
    as_ead_data = get_as_data(as_login())
    ds_tracks = {}
    for ds_track in create_track_list():  # for ds_track (which is a dict with info about the track) do:
        ds_tracks[ds_track['id']] = ds_track  # add to the dict ds_tracks a key which is the ds_track['id] and as the
        # value the respective ds_track
    fonds = as_ead_data['children']  # start iterating over the EAD-XML structure (now json) to get to the AO item
    for fond in fonds:
        subfonds = fond['children']
        for subfond in subfonds:
            items = subfond['children']
            for item in items:
                track_comp_id = item['component_id']  # save the id of the archival_object as track_comp_id
                if track_comp_id in ds_tracks:
                    matching_track = ds_tracks[track_comp_id]  # put the ds_track with the
                    # matching track_comp_id in matching_track
                    if len(matching_track['title']) == 0:  # if the file from the csv has no title use the one from AS
                        matching_track['title'] = item['title']
                    track_formatted = [format_metadata('dc.identifier', matching_track['id']),
                                       format_metadata('dc.title', matching_track['title'])]
                    if Path.exists(path_to_stored_file(matching_track)):  # check if there is a file behind the path
                        try:
                            # create the object in Dspace with the MD of the track, in the respective DS collection
                            ds_object = create_dspace_record(track_formatted, ds_collection,
                                                             login_to_dspace())
                            #  upload the track in the created DS object and use the track_tag as the name for the track
                            upload_track(ds_object['link'],
                                         path_to_stored_file(matching_track),
                                         matching_track['track_tag'])
                            # create the link which has to be used in AS later and is created through the handle and tag
                            link_to_bitstream = f"{ds_api_base_url}/bitstream/handle/{ds_object['handle']}/{matching_track['track_tag']}"
                            # create a Digital object with the track and link it to the AO item in ArchivesSpace
                            link_dip_to_as(as_base_url, as_user, as_password, as_url_port, item['id'],
                                           matching_track['id'], matching_track['title'], link_to_bitstream)
                            pprint(f" item id = {item['id']} and {matching_track['id']}")
                        except:
                            logging.warning("Unique Id already exists")
                    else:
                        pprint(f"{path_to_stored_file(matching_track)} is not in the test files")
                        logging.warning(f"{path_to_stored_file(matching_track)} is not in the test files")
                else:
                    pprint(f"Track with id {track_comp_id} is not in the CSV")
                    logging.warning(f"Track with id {track_comp_id} is not in the CSV")


main()
