import datetime
import json

import requests

from googleAds.utils import save_google_video_ad

# open_file = open("/home/sagar/Documents/projects/google/google-ads-puller/sample_response/sampele_video_response.json", "r")
# read_file = open_file.read()
# json_response = json.loads(read_file)

brand_id = 'e2fd950a-19f7-4722-a78e-79bd5b94a802'
current_date = datetime.datetime.today().date()
until_date = str(current_date)
since_date = str(current_date - datetime.timedelta(days=120))

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

video_ad_query = f"SELECT metrics.clicks, metrics.average_cpv, metrics.average_cpm, metrics.average_cpe, " \
                 f"metrics.average_cpc, metrics.all_conversions_value, metrics.conversions, " \
                 f"metrics.conversions_from_interactions_rate, metrics.conversions_value, metrics.cost_micros, " \
                 f"metrics.ctr, metrics.engagement_rate, metrics.engagements, metrics.impressions, " \
                 f"metrics.video_quartile_p100_rate, metrics.video_quartile_p25_rate, " \
                 f"metrics.video_quartile_p50_rate, metrics.video_quartile_p75_rate, metrics.video_view_rate, " \
                 f"metrics.video_views, metrics.view_through_conversions, metrics.value_per_conversion, " \
                 f"metrics.value_per_all_conversions, metrics.cost_per_all_conversions, " \
                 f"metrics.cost_per_conversion, metrics.cross_device_conversions, " \
                 f"metrics.conversions_value_per_cost, metrics.conversions_from_interactions_value_per_interaction, " \
                 f"metrics.all_conversions_from_interactions_rate, metrics.all_conversions, ad_group.campaign, " \
                 f"ad_group.id, ad_group_ad.ad.id, campaign.id, campaign.name, campaign.status, campaign.start_date, " \
                 f"segments.date, segments.device, campaign.end_date, campaign.advertising_channel_type, " \
                 f"campaign.advertising_channel_sub_type, ad_group_ad.ad.name, ad_group.name, customer.id " \
                 f"FROM video where segments.date >= '{since_date}' and segments.date <= '{until_date}'"

request_param = {"query": video_ad_query}
response = requests.post(api_url, headers=headers, data=request_param)
campaign_response = response.json()
# print(campaign_response)
data_list = campaign_response[0]['results']
for each_data in data_list:
    customers = each_data.get('customer')
    campaigns = each_data.get('campaign')
    ad_group = each_data.get('adGroup')
    metrics = each_data.get('metrics')
    ad_group_ad = each_data.get('adGroupAd')
    video = each_data.get('video')
    segments = each_data.get('segments')
    save_google_video_ad(customers, campaigns, ad_group, video, metrics, segments, ad_group_ad, brand_id)

