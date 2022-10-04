import uuid
from datetime import datetime

import configparser
import logging
import psycopg2
import psycopg2.extras
import uuid
from datetime import datetime

import pymongo

current_date_time = datetime.now()

config = configparser.ConfigParser()
config.read('config.ini')
mongo_collection = ''

logging.basicConfig(filename="google_data.log", format='%(levelname)s:: %(asctime)s - %(message)s',
                    level=logging.INFO)


pg_conn = psycopg2.connect(
    host=config['PGSQL']['HOSTNAME'],
    database=config['PGSQL']['DB_NAME'],
    user=config['PGSQL']['USERNAME'],
    password=config['PGSQL']['PASSWORD'],
    port=config['PGSQL']['PORT'],
)
pg_cur = pg_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)


def execute_query(sql_query):
    pg_cur = pg_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        pg_cur.execute(sql_query)
        pg_conn.commit()
        logging.info(f"Query executed successfully with pgSQL:: {sql_query}")
    except Exception as e:
        logging.error(f"Query Execution Error with pgSQL:: {str(e)}, sql query :: {sql_query}", exc_info=True)


def save_google_campaign(customers, campaigns, brand_id):
    resource_name_customer = ''
    google_customer_id = ''
    if customers:
        resource_name_customer = customers.get('resourceName')
        google_customer_id = resource_name_customer.split("/")[1]

    resource_name_campaign = campaigns.get('resourceName')
    campaign_status = campaigns.get('status')

    advertising_channel_type = campaigns.get('advertisingChannelType')
    if advertising_channel_type:
        advertising_channel_type = advertising_channel_type.replace("'", "''")

    campaign_name = campaigns.get('name')
    if campaign_name:
        campaign_name = campaign_name.replace("'", "''")

    campaign_id = campaigns.get('id')
    start_date = campaigns.get('startDate')
    end_date = campaigns.get('endDate')

    sql_query = f"INSERT INTO google_campaign" \
                f"(created_at, is_active, google_campaign_id_uuid, google_customer_id, " \
                f"google_campaign_id, resource_name, advertising_channel_type, campaign_name, status, " \
                f"start_date, end_date, brand_id)" \
                f"VALUES('{current_date_time}', true, '{uuid.uuid4()}', '{google_customer_id}', " \
                f"'{campaign_id}', '{resource_name_campaign}', '{advertising_channel_type}', " \
                f"'{campaign_name}', '{campaign_status}', '{start_date}', '{end_date}', '{brand_id}')" \
                f"ON CONFLICT (brand_id,google_campaign_id,google_customer_id,advertising_channel_type)" \
                f"DO UPDATE SET resource_name='{resource_name_campaign}',campaign_name='{campaign_name}'," \
                f"status='{campaign_status}' "

    execute_query(sql_query)


def save_google_ad_group(customers, campaigns, ad_group, brand_id):
    resource_name_customer = ''
    google_customer_id = ''
    if customers:
        resource_name_customer = customers.get('resourceName')
        google_customer_id = resource_name_customer.split("/")[1]

    campaign_id = campaigns.get('id')
    ad_group_id = ad_group.get('id')
    resource_name_ad_group = ad_group.get('resourceName')
    ad_group_name = ad_group.get('name')
    if ad_group_name:
        ad_group_name = ad_group_name.replace("'", "''")

    ad_group_type = ad_group.get('type')

    if ad_group_type:
        ad_group_type = ad_group_type.replace("'", "''")

    ad_group_status = ad_group.get('status')

    sql_query = f"INSERT INTO google_ad_group(created_at, is_active, google_campaign_id_uuid, " \
                f"google_customer_id, google_campaign_id, google_ad_group_id, resource_name, " \
                f"ad_name, type, status, brand_id) " \
                f"VALUES('{current_date_time}', 'true', '{uuid.uuid4()}', '{google_customer_id}'," \
                f"'{campaign_id}', '{ad_group_id}', '{resource_name_ad_group}', '{ad_group_name}', " \
                f"'{ad_group_type}', '{ad_group_status}', '{brand_id}') " \
                f"ON CONFLICT (brand_id, google_campaign_id, google_customer_id, google_ad_group_id)" \
                f"DO UPDATE SET resource_name='{resource_name_ad_group}', ad_name='{ad_group_name}', " \
                f"type='{ad_group_type}', status='{ad_group_status}'"
    execute_query(sql_query)


def save_google_ad(customers, campaigns, ad_group, metrics, ad_group_ad, segments, brand_id):
    resource_name_customer = ''
    google_customer_id = ''
    ad_id = ''
    resource_name_ad = ''
    timezone = ''
    currency_code = ''

    if customers:
        resource_name_customer = customers.get('resourceName')
        google_customer_id = resource_name_customer.split("/")[1]
        timezone = customers.get('timeZone')
        currency_code = customers.get('currencyCode')

    campaign_id = campaigns.get('id')
    ad_group_id = ad_group.get('id')

    if ad_group_ad:
        ad_id = ad_group_ad['ad']['id']
        resource_name_ad = ad_group_ad['ad']['resourceName']

    segment_date = segments.get('date')
    resource_name_ad_group = ad_group.get('resourceName')
    if resource_name_ad_group:
        resource_name_ad_group = resource_name_ad_group.replace("'", "''")

    ad_group_name = ad_group.get('name')
    if ad_group_name:
        ad_group_name = ad_group_name.replace("'", "''")

    device = segments.get('device')
    if device:
        device = device.replace("'", "''")

    ad_network_type = segments.get('adNetworkType')

    clicks = metrics.get('clicks', 0)
    engagements = metrics.get('engagements', 0)
    impressions = metrics.get('impressions', 0)
    interactions = metrics.get('interactions', 0)
    average_cpc = metrics.get('averageCpc', 0)
    average_cpm = metrics.get('averageCpm', 0)
    interaction_rate = metrics.get('interactionRate', 0)
    video_views = metrics.get('videoViews', 0)
    conversions = metrics.get('conversions', 0)
    cost_micros = metrics.get('costMicros', 0)
    ctr = metrics.get("ctr", 0)

    spends = 0
    if cost_micros:
        spends = int(cost_micros) / 1000000

    sql_query = f"INSERT INTO google_ad(created_at, is_active, google_campaign_id_uuid, google_customer_id, " \
                f"google_campaign_id, google_ad_group_id, google_ad_id, data_date, ad_date, resource_name, " \
                f"ad_group_resource_name, ad_name, device, ad_network_type, timezone, currency_code, clicks, " \
                f"engagements, impressions, interactions, average_cpc, average_cpm, interaction_rate, video_views, " \
                f"conversions, cost_micros, ctr, spends, brand_id)" \
                f"VALUES('{current_date_time}', 'true', '{uuid.uuid4()}', '{google_customer_id}', '{campaign_id}', " \
                f"'{ad_group_id}', '{ad_id}', '{segment_date}', '{segment_date}', '{resource_name_ad}', " \
                f"'{resource_name_ad_group}', '{ad_group_name}', '{device}', '{ad_network_type}', '{timezone}', " \
                f"'{currency_code}', '{clicks}', '{engagements}', '{impressions}', '{interactions}', " \
                f"'{average_cpc}', " \
                f"'{average_cpm}', '{interaction_rate}', '{video_views}', '{conversions}', '{cost_micros}', " \
                f"'{ctr}', '{spends}', '{brand_id}')" \
                f" ON CONFLICT(brand_id, data_date, device, google_ad_id, google_campaign_id, google_customer_id, " \
                f"google_ad_group_id)" \
                f"DO UPDATE SET google_ad_group_id='{ad_group_id}', ad_date='{segment_date}', resource_name='{resource_name_ad}', " \
                f"ad_group_resource_name='{resource_name_ad_group}', ad_name='{ad_group_name}', device='{device}', " \
                f"ad_network_type='{ad_network_type}', timezone='{timezone}', currency_code='{currency_code}', " \
                f"clicks='{clicks}', engagements='{engagements}', impressions='{impressions}', interactions='{interactions}', " \
                f"average_cpc='{average_cpc}', average_cpm='{average_cpm}', interaction_rate='{interaction_rate}', " \
                f"video_views='{video_views}', conversions='{conversions}', cost_micros='{cost_micros}', ctr='{ctr}', " \
                f"spends='{spends}'"
    execute_query(sql_query)


def save_google_ad_search_keyword(customers, campaigns, ad_group, metrics, segments, brand_id,
                                  ad_group_criterion, keyword_view):
    resource_name_customer = ''
    google_customer_id = ''
    if customers:
        resource_name_customer = customers.get('resourceName')
        google_customer_id = resource_name_customer.split("/")[1]

    campaign_id = campaigns.get('id')
    ad_group_id = ad_group.get('id')
    segment_date = segments.get('date')

    ad_group_criterion_resource_name = ad_group_criterion.get('resourceName')
    keyword_view_resource_name = keyword_view.get('resourceName')

    keywords = ad_group_criterion.get('keyword')
    match_type = keywords.get('matchType')
    if match_type:
        match_type = match_type.replace("'", "''")

    keyword = keywords.get('text')
    if keyword:
        keyword = keyword.replace("'", "''")

    device = segments.get('device')
    if device:
        device = device.replace("'", "''")

    clicks = metrics.get('clicks', 0)
    engagements = metrics.get('engagements', 0)
    impressions = metrics.get('impressions', 0)
    interactions = metrics.get('interactions', 0)
    average_cpc = metrics.get('averageCpc', 0)
    average_cpm = metrics.get('averageCpm', 0)
    interaction_rate = metrics.get('interactionRate', 0)
    video_views = metrics.get('videoViews', 0)
    conversions = metrics.get('conversions', 0)
    cost_micros = metrics.get('costMicros', 0)
    ctr = metrics.get("ctr", 0)

    spends = 0
    if cost_micros:
        spends = int(cost_micros) / 1000000

    sql_query = f"INSERT INTO google_ad_search_keyword(created_at, is_active, google_campaign_id_uuid, " \
                f"google_customer_id, google_campaign_id, google_ad_group_id, data_date, ad_date, " \
                f"ad_group_criterion_resource_name, keyword_view_resource_name, match_type, device, " \
                f"keyword, clicks, engagements, impressions, interactions, average_cpc, average_cpm, " \
                f"interaction_rate, video_views, conversions, cost_micros, ctr, spends, brand_id)" \
                f"VALUES('{current_date_time}', 'true', '{uuid.uuid4()}', '{google_customer_id}', " \
                f"'{campaign_id}', '{ad_group_id}', '{segment_date}', '{segment_date}', " \
                f"'{ad_group_criterion_resource_name}', '{keyword_view_resource_name}', '{match_type}', " \
                f"'{device}', '{keyword}', {clicks}, {engagements}, {impressions}, {interactions}, " \
                f"{average_cpc}, {average_cpm}, {interaction_rate}, {video_views}, {conversions}, " \
                f"'{cost_micros}', '{ctr}', '{spends}', '{brand_id}')" \
                f"ON CONFLICT (brand_id,data_date,device,keyword,match_type," \
                f"google_campaign_id,google_customer_id, google_ad_group_id)" \
                f"DO UPDATE SET ad_group_criterion_resource_name='{ad_group_criterion_resource_name}', " \
                f"keyword_view_resource_name='{keyword_view_resource_name}', " \
                f"match_type='{match_type}', device='{device}', keyword='{keyword}', clicks='{clicks}', " \
                f"engagements={engagements}, impressions={impressions}, interactions={interactions}, " \
                f"average_cpc={average_cpc}, average_cpm={average_cpm}, interaction_rate={interaction_rate}, " \
                f"video_views={video_views}, conversions={conversions}, cost_micros='{cost_micros}', " \
                f"ctr='{ctr}', spends='{spends}'"
    execute_query(sql_query)


def save_google_video_ad(customers, campaigns, ad_group, video, metrics, segments, ad_group_ad, brand_id):
    google_customer_id = ''
    ad_id = ''
    if customers:
        resource_name_customer = customers.get('resourceName')
        google_customer_id = resource_name_customer.split("/")[1]

    campaign_id = campaigns.get('id')
    ad_group_id = ad_group.get('id')
    channel_type = campaigns.get('advertisingChannelType')
    segment_date = segments.get('date')
    ad_group_name = ad_group.get('name')
    if ad_group_name:
        ad_group_name = ad_group_name.replace("'", "''")

    device = segments.get('device')
    if device:
        device = device.replace("'", "''")

    clicks = metrics.get('clicks', 0)
    engagements = metrics.get('engagements', 0)
    impressions = metrics.get('impressions', 0)
    average_cpm = metrics.get('averageCpm', 0)
    video_views = metrics.get('videoViews', 0)
    conversions = metrics.get('conversions', 0)
    cost_micros = metrics.get('costMicros', 0)
    ctr = metrics.get("ctr", 0)
    video_quartile_p100_rate = metrics.get('videoQuartileP100Rate', 0)
    video_quartile_p75_rate = metrics.get('videoQuartileP75Rate', 0)
    video_quartile_p50_rate = metrics.get('videoQuartileP50Rate', 0)
    video_quartile_p25_rate = metrics.get('videoQuartileP25Rate', 0)
    video_view_rate = metrics.get('videoViewRate', 0)
    view_through_conversions = metrics.get('viewThroughConversions', 0)
    conversions_from_interactions_rate = metrics.get('conversionsFromInteractionsRate', 0)
    conversions_value = metrics.get('conversionsValue', 0)
    conversions_from_interactions_value_per_interaction = metrics.get('conversionsFromInteractionsValuePerInteraction', 0)
    conversions_value_per_cost = metrics.get('conversionsValuePerCost', 0)
    cross_device_conversions = metrics.get('crossDeviceConversions', 0)
    engagement_rate = metrics.get('engagementRate', 0)
    all_conversions_from_interactions_rate = metrics.get('allConversionsFromInteractionsRate', 0)
    all_conversions_value = metrics.get('allConversionsValue', 0)
    all_conversions = metrics.get('allConversions', 0)
    average_cpe = metrics.get('averageCpe', 0)
    average_cpv = metrics.get('averageCpv', 0)
    spends = 0
    if cost_micros:
        spends = int(cost_micros) / 1000000
    sql_query = f"INSERT INTO google_ad_video(created_at, is_active, google_video_uuid, " \
                f"google_customer_id, google_campaign_id, google_ad_group_id, google_ad_id, device, " \
                f"channel_type, data_date, ad_date, clicks, video_views, impressions, engagements, spends, " \
                f"video_quartile_p100_rate, video_quartile_p25_rate, video_quartile_p50_rate, " \
                f"video_quartile_p75_rate, video_view_rate, view_through_conversions, " \
                f"conversions_from_interactions_rate, conversions_value, conversions_value_per_cost, " \
                f"conversions_from_interactions_value_per_interaction, conversions, cost_micros, " \
                f"cross_device_conversions, ctr, engagement_rate, all_conversions_from_interactions_rate, " \
                f"all_conversions_value, all_conversions, average_cpe, average_cpm, average_cpv, brand_id)" \
                f"VALUES('{current_date_time}', 'true', '{uuid.uuid4()}', '{google_customer_id}', " \
                f"'{campaign_id}', '{ad_group_id}', '{ad_id}', '{device}', '{channel_type}', '{segment_date}', " \
                f"'{segment_date}', {clicks}, {video_views}, {impressions}, {engagements}, {spends}, " \
                f"{video_quartile_p100_rate}, {video_quartile_p25_rate}, {video_quartile_p50_rate}, " \
                f"{video_quartile_p75_rate}, {video_view_rate}, {view_through_conversions}, " \
                f"{conversions_from_interactions_rate}, {conversions_value}, " \
                f"{conversions_value_per_cost}, {conversions_from_interactions_value_per_interaction}, " \
                f"{conversions}, {cost_micros}, {cross_device_conversions}, {ctr}, {engagement_rate}, " \
                f"{all_conversions_from_interactions_rate}, {all_conversions_value}, {all_conversions}, " \
                f"{average_cpe}, {average_cpm}, {average_cpv}, '{brand_id}')" \
                f"ON CONFLICT (brand_id,data_date,device,google_campaign_id," \
                f"google_customer_id,google_ad_group_id,google_ad_id)" \
                f"DO UPDATE SET channel_type='{channel_type}', clicks={clicks}, video_views={video_views}," \
                f"impressions={impressions}, engagements={engagements}, spends={spends}, " \
                f"video_quartile_p100_rate={video_quartile_p100_rate}, " \
                f"video_quartile_p25_rate={video_quartile_p25_rate}, " \
                f"video_quartile_p50_rate={video_quartile_p50_rate}, " \
                f"video_quartile_p75_rate={video_quartile_p75_rate}, " \
                f"video_view_rate={video_view_rate}, view_through_conversions={view_through_conversions}, " \
                f"conversions_from_interactions_rate={conversions_from_interactions_rate}, " \
                f"conversions_value={conversions_value}, conversions_value_per_cost={conversions_value_per_cost}, " \
                f"conversions_from_interactions_value_per_interaction={conversions_from_interactions_value_per_interaction}, " \
                f"conversions={conversions}, cost_micros={cost_micros}, " \
                f"cross_device_conversions={cross_device_conversions}, ctr={ctr}, engagement_rate={engagement_rate}, " \
                f"all_conversions_from_interactions_rate={all_conversions_from_interactions_rate}, " \
                f"all_conversions_value={all_conversions_value}, all_conversions={all_conversions}, " \
                f"average_cpe={average_cpe}, average_cpm={average_cpm}, average_cpv={average_cpv}"
    execute_query(sql_query)
