import datetime
import json
import logging, configparser

import pymongo
import requests

# import uuid

from utils import save_google_campaign, save_google_ad_group, save_google_ad, save_google_ad_search_keyword

# open_file = open("/home/sagar/Desktop/sagar sharma/googleAd/sample_response/googleAd.json", "r")
# json_response = json.load(open_file)
# data_list = json_response[0]['results']

config = configparser.ConfigParser()
config.read('config.ini')
raw_response = {}

try:
    # Connecting with MongoDB
    mongo_client = pymongo.MongoClient(f"mongodb://{config['MONGO']['HOSTNAME']}:{config['MONGO']['PORT']}/",
                                       username=config['MONGO']['USERNAME'], password=config['MONGO']['PASSWORD'])
    mongo_db = mongo_client[config['MONGO']['DBNAME']]  # databasename
    mongo_page_collections = mongo_db["google_campaign_ad_group"]  # collectionname
except Exception as e:
    logging.error(f"Connection error with Mongo:: {str(e)}", exc_info=True)

brand_id = 'e2fd950a-19f7-4722-a78e-79bd5b94a802'
current_date = datetime.datetime.today().date()
until_date = str(current_date)
since_date = str(current_date - datetime.timedelta(days=120))
print(since_date, until_date)

refresh_token = '1//0gqNTH5me-wJeCgYIARAAGBASNwF-L9IrT87kqxbhLdPMR4GdC0gnLWM8ECWHaom0eCXfD-' \
                'WJr914yV3qOM70oEo9KhufJIODbIg'

data = {
    'grant_type': 'refresh_token',
    'client_id': '816729504322-skhfkv0inf7sfku98uei4uspfdktshq5.apps.googleusercontent.com',
    'client_secret': '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',
    'refresh_token': refresh_token
}

response = requests.post('https://www.googleapis.com/oauth2/v3/token', data=data)
info = response.json()
access_token = info['access_token']
customer_id = '6713652640'
# customer_id = '9028554949'
api_url = f"https://googleads.googleapis.com/v10/customers/{customer_id}/googleAds:searchStream"
headers = {
    "Authorization": "Bearer " + access_token,
    "developer-token": "DzD1DKSi5jZKCxSl2xGC-A"
}
ad_query = f"SELECT ad_group_ad.ad.call_ad.headline1, ad_group_ad.ad.call_ad.headline2, " \
           f"ad_group_ad.ad.image_ad.preview_image_url, ad_group_ad.ad.call_ad.path1, " \
           f"ad_group_ad.ad.call_ad.path2, ad_group_ad.ad.call_ad.description1, " \
           f"ad_group_ad.ad.call_ad.description2, customer.time_zone, customer.currency_code," \
           f"ad_group_ad.status, segments.device, metrics.average_cpm, metrics.average_cpv, " \
           f"metrics.clicks, metrics.conversions, metrics.cost_micros, metrics.ctr, " \
           f"metrics.engagement_rate, metrics.engagements, metrics.impressions, " \
           f"metrics.interaction_rate, metrics.interactions, metrics.video_view_rate," \
           f"metrics.video_views,segments.ad_network_type, ad_group.type, " \
           f"campaign.advertising_channel_sub_type, campaign.advertising_channel_type," \
           f"campaign.target_impression_share.location,campaign.start_date, campaign.end_date, " \
           f"campaign.status, ad_group.type, ad_group.status, ad_group.labels, segments.date, " \
           f"ad_group.id, ad_group.name, ad_group_ad.ad.id, campaign.id, campaign.name, " \
           f"metrics.cost_micros, metrics.average_cpc, metrics.average_cpe, metrics.average_cpm, " \
           f"metrics.average_cpv,  metrics.clicks, metrics.conversions, metrics.ctr, " \
           f"metrics.engagement_rate, metrics.engagements, metrics.interactions, " \
           f"metrics.interaction_rate " \
           f"FROM ad_group_ad where segments.date >= '{since_date}' and segments.date <= '{until_date}'"

request_param = {"query": ad_query}
response = requests.post(api_url, headers=headers, data=request_param)
campaign_response = response.json()
data_list = campaign_response[0]['results']

for each_data in data_list:
    customers = each_data.get('customer')
    campaigns = each_data.get('campaign')
    ad_group = each_data.get('adGroup')
    metrics = each_data.get('metrics')
    ad_group_ad = each_data.get('adGroupAd')
    segments = each_data.get('segments')
    ad_group_criterion = each_data.get('adGroupCriterion')
    keyword_view = each_data.get('keywordView')
    segment_date = segments.get('date')

    # save_google_campaign(customers, campaigns, brand_id)
    save_google_ad_group(customers, campaigns, ad_group, brand_id)
    # save_google_ad(customers, campaigns, ad_group, metrics, ad_group_ad, segments, brand_id)
