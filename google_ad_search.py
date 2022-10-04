import datetime
import json

import requests

from utils import save_google_ad_search_keyword

# open_file = open("/home/sagar/Desktop/sagar sharma/googleAd/sample_response/searchAd.json", "r")
# json_response = json.load(open_file)
# data_list = json_response[0]['results']
brand_id = 'e2fd950a-19f7-4722-a78e-79bd5b94a802'

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
ad_query = f"SELECT campaign.status, campaign.end_date, campaign.advertising_channel_sub_type, " \
           f"campaign.advertising_channel_type," \
           f"ad_group_criterion.keyword.text, ad_group_criterion.keyword.match_type, " \
           f"campaign.id, campaign.labels, campaign.name, campaign.start_date, " \
           f"ad_group.id, ad_group.name, ad_group.labels, ad_group.status, metrics.video_views, " \
           f"metrics.video_view_rate, metrics.interaction_rate, metrics.interactions, " \
           f"metrics.ctr, metrics.cost_micros, metrics.clicks,  " \
           f"metrics.conversions, metrics.average_cost, metrics.average_cpc, " \
           f"metrics.average_cpe, metrics.average_cpm, metrics.average_cpv, " \
           f"metrics.engagements, metrics.engagement_rate, metrics.impressions, " \
           f"segments.date, segments.device FROM keyword_view " \
           f"where segments.date >= '{since_date}' and segments.date <= '{until_date}'"

request_param = {"query": ad_query}
response = requests.post(api_url, headers=headers, data=request_param)
campaign_response = response.json()
print(campaign_response)
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

    save_google_ad_search_keyword(customers, campaigns, ad_group, metrics, segments, brand_id,
                                  ad_group_criterion, keyword_view)