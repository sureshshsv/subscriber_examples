#!/usr/bin/env python3

######Importing modules for checks
import netrc
import json
from datetime import datetime, timezone, timedelta
import xml.etree.ElementTree as ET
import requests
import sys
data = {}
e=[]
v=[]
wp={}
i=0
######
cmr = "cmr.earthdata.nasa.gov"
shortname=""
################################################################################
def get_cloud_collections():
    search_url="https://cmr.earthdata.nasa.gov/search/collections.json?provider=POCLOUD&page_size=2000"
    response = requests.get(search_url)
    json_response = json.loads(response.text)
    return json_response
################################################################################
def get_variables(coll):
    v=[]
    if coll['feed']['entry'][0]['has_variables']:
        if 'variables' in coll['feed']['entry'][0]['associations']:
                for f in coll['feed']['entry'][0]['associations']['variables']:
                    search_url="https://"+cmr+"/search/variables.json?pretty=true&concept_id="+f
                    response = requests.get(search_url)
                    json_response = json.loads(response.text)
                    if(json_response['hits'] > 0):
                        v.append(json_response['items'][0]['long_name'])
    return v                  
################################################################################
def get_json_collection():
    search_url="https://"+cmr+"/search/collections.json?shortName="+shortname
    response = requests.get(search_url)
    json_response = json.loads(response.text)
    return json_response
################################################################################
def get_json_granules():
    search_url="https://"+cmr+"/search/granules.json?shortName="+shortname+"&sort_key[]=-start_date&provider=POCLOUD"
    response = requests.get(search_url)
    json_response = json.loads(response.text)
    return json_response
################################################################################
def get_cycles_passes():
    search_url="https://"+cmr+"/search/granules.native?shortName="+shortname
    response = requests.get(search_url)
    granule_xml=response.text
    root = ET.fromstring(granule_xml)
    cycle=0
    for gr in root.findall("./result"):
        json_response = json.loads(gr.text)
        if "Track" in json_response['SpatialExtent']['HorizontalSpatialDomain']:
            cycle=json_response['SpatialExtent']['HorizontalSpatialDomain']['Track']['Cycle']
            pss=json_response['SpatialExtent']['HorizontalSpatialDomain']['Track']['Passes'][0]['Pass']
        if(cycle):
            return(cycle,pss)
        else:
            return(0,0) 
################################################################################
collections=get_cloud_collections()
collections=['AQUARIUS_L2_SSS_CAP_V5','OISST_HR_NRT-GOS-L4-BLK-v2.0','AQUARIUS_L3_SSS_SMI_7DAY-RUNNINGMEAN_V5']
#for f in collections['feed']['entry']:
for f in collections:    
    #shortname=f['short_name']
    shortname=f
    download1=""
    download2=""
    download3=""
    download4=""
    download5=""
    download6=""
    granule_details=get_json_granules()
    collection_details=get_json_collection()
    v=get_variables(collection_details)
    time_start=collection_details['feed']['entry'][0]['time_start']
    if(len(granule_details['feed']['entry']) > 0):
        time_end=granule_details['feed']['entry'][0]['time_end']
    else:
        print("No Granlues")
        continue
    bbox=collection_details['feed']['entry'][0]['boxes'][0]
    #Works for data in the cloud
    for f in granule_details['feed']['entry'][0]['links']:
        if f['rel'] == "http://esipfed.org/ns/fedsearch/1.1/s3#" :
            extensions = f['href'].split(".")
            e.append(extensions[-1])
    #Works for data outside the cloud
    if (len(e) < 1):
        for f in granule_details['feed']['entry'][0]['links']:
            if f['rel'] == "http://esipfed.org/ns/fedsearch/1.1/data#" :
                extensions = f['href'].split(".")
                e.append(extensions[-1])
    bbox = bbox.replace(" ", ",")
    time_start=datetime.strptime(time_start, "%Y-%m-%dT%H:%M:%S.%f%z")
    time_end=datetime.strptime(time_end, "%Y-%m-%dT%H:%M:%S.%f%z")
    # difference between dates in timedelta
    delta = time_end - time_start
    if(delta.days > 0):
        time_end=time_start + timedelta(days=1)
    if(delta.days > 7):
        time_end=time_start  + timedelta(days=7)
    current_date = datetime.now(timezone.utc)
    # get difference
    delta = current_date - time_end
    minutes = delta.total_seconds() / 60
    # convert to format expected by the downloader
    time_start = str(time_start.strftime("%Y-%m-%dT%H:%M:%SZ"))
    time_end = str(time_end.strftime("%Y-%m-%dT%H:%M:%SZ"))
    c,p=get_cycles_passes()
    download1="podaac-data-downloader -c "+shortname+" -d ./data --start-date "+time_start+" --end-date "+time_end+" -e \"\""
    download2="podaac-data-downloader -c "+shortname+" -d ./data --start-date "+time_start+" --end-date "+time_end+" -b=\""+bbox+"\""
    #Downloads data files that have been updated within the last 360 minutes
    download5="podaac-data-subscriber -c "+shortname+" -d ./data -m 360"
    #Downloads data files that have been updated within the last 1440 minutes (24 hrs)
    download6="podaac-data-subscriber -c "+shortname+" -d ./data -m 1440"
    cmd =  str(download1)
    cmd = cmd + "<br>"  + str(download1)
    cmd = cmd + "<br>"  + str(download2)
    if (len(e[0]) < 4):
        download3="podaac-data-downloader -c "+shortname+" -d ./data --start-date "+time_start+" --end-date "+time_end+" -e ."+e[0]
        cmd = cmd + "<br>"  + str(download3)
    if(c):
        download4="podaac-data-downloader -c "+shortname+" -d ./data --cycle "+str(c)
        cmd = cmd + "<br>"  + str(download4)
    
    if minutes < 400:
        cmd = cmd + "<br>"  + str(download5)
    if minutes < 1600:
        cmd = cmd + "<br>"  + str(download6)
    data={}
    data['variable'] = list(v)
    data['subscriber_simple'] = download1
    data['subscriber_bbox'] = download2
    data['subscriber_extension'] = download3
    data['subscriber_cycle'] = download4
    data['subscriber_time400'] = download5
    data['subscriber_time1600'] = download6
    wp[shortname]=data
print(json.dumps(wp))




