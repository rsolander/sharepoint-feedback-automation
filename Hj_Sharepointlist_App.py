import numpy as np
import json
import requests
import pandas as pd
import timeit
import math
import datetime
import os
import time
import csv
from datetime import datetime, timedelta
import pytz
#from pydrive.auth import GoogleAuth
#from pydrive.drive import GoogleDrive
import apscheduler
from apscheduler.schedulers.blocking import BlockingScheduler
from O365 import Account
import spacy

def main():
    parent_path = os.path.dirname(os.getcwd())
    nlpmodel = spacy.load(parent_path + '/ux_rec_model')
    print('NLP model loaded')
    def scheduledtask():
        print('Task begins...')
        loginurl = 'https://insights.hotjar.com/api/v2/users'
        dlurl= 'https://insights.hotjar.com/api/v1/sites/1547206/feedback/256010/responses?fields=browser,content,created_datetime_string,created_epoch_time,country_code,country_name,device,id,index,os,response_url,short_visitor_uuid,window_size&sort=-id&offset=0&amount=30000&format=csv&filter=created__ge__2009-05-11'
        headexp = {
                    'authority': 'insights.hotjar.com',
                    'method': 'GET',
                    'path': '/api/v1/sites/1547206/feedback/256010/responses?fields=browser,content,created_datetime_string,created_epoch_time,country_code,country_name,device,id,index,os,response_url,short_visitor_uuid,window_size&sort=-id&offset=0&amount=30000&format=csv&filter=created__ge__2020-03-24',
                    'scheme': 'https',
                    'accept': '*/*',
                    'accept-encoding': 'gzip, deflate',
                    'accept-language': 'en-US,en;q=0.9',
                    'referer': 'https://insights.hotjar.com/sites/1547206/feedback/responses/256010',
                    'sec-fetch-dest': 'empty',
                    'sec-fetch-mode': 'cors',
                    'sec-fetch-site': 'same-origin',
                    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.113 Safari/537.36'
        }
        postheader = {
                    'authority': 'insights.hotjar.com',
                    'method': 'POST',
                    'path': '/api/v2/users',
                    'scheme': 'https',
                    'accept': 'application/json, text/plain, */*',
                    'accept-encoding': 'gzip, deflate, br',
                    'accept-language': 'en-US,en;q=0.9',
                    'content-length': '77',
                    'content-type': 'application/json;charset=UTF-8',
                    'cookie': 'in_new_website_experiment=variant1; _gcl_au=1.1.1236886742.1636490802; _ga=GA1.2.1087228580.1636490802; _gid=GA1.2.173253070.1636490802; _fbp=fb.1.1636490802791.1155339610; _scid=002b7a58-2ed4-4369-9486-5d306d260336; OptanonConsent=isIABGlobal=false&datestamp=Tue+Nov+09+2021+14:46:43+GMT-0600+(Central+Standard+Time)&version=6.5.0&hosts=&consentId=b0d92f7e-124e-4183-a277-e1d0debe8d15&interactionCount=1&landingPath=https://www.hotjar.com/&groups=0:1,3:1,2:1,1:1; _hjid=caed91f8-cf76-41d9-a6bb-bdfa835ea62f; _hjFirstSeen=1; _hjIncludedInPageviewSample=1; _hjAbsoluteSessionInProgress=1; _hjIncludedInSessionSample=1; __zlcmid=16ykT8ISabKB2SC; _BEAMER_USER_ID_zeKLgqli17986=010b5051-469e-4c24-95c3-3e5e0835faef; _BEAMER_FIRST_VISIT_zeKLgqli17986=2021-11-09T20:47:25.882Z; _BEAMER_FILTER_BY_URL_zeKLgqli17986=false; _hjCachedUserAttributes=eyJhdHRyaWJ1dGVzIjp7ImFjY291bnRfZmVhdHVyZV9mbGFncyI6bnVsbCwiYmVjYW1lX2FfY3VzdG9tZXIiOiIyMDIwLTAxLTI0VDIxOjMzOjQ3LjAwMFoiLCJieHBfYmFzaWNfY2NfZXhwIjoiY29udHJvbCIsImNvdW50cnkiOiJVUyIsImhpZ2hlc3RfcGxhbiI6ImJ1c2luZXNzIiwiaGlnaGVzdF9zYW1wbGVfcmF0ZSI6MTIwMDAwLCJvd25lZF9hY2NvdW50X2JlY2FtZV9hX2N1c3RvbWVyIjpudWxsLCJvd25lZF9hY2NvdW50X2JpbGxpbmdfY3ljbGUiOm51bGwsInJlZmVycmVyX3VybCI6InJlZmVycmFsIiwic2lnbmVkX3VwIjoiMjAyMS0xMS0wOVQyMDoxOTozMS4wMDBaIiwic2l0ZV9pbmR1c3RyeSI6Im90aGVyIiwic2l0ZV9sb3dlc3RfYWxleGFfcmFuayI6Mjg3ODYsInVzZXJfcm9sZSI6Im90aGVyIn0sInVzZXJJZCI6IjI2Mjc0MjYifQ==; _hjUserAttributesHash=6330f04c52d2e92b716f185b015b11c7; _hjMinimizedPolls=744300; _uetsid=252a0bb0419e11eca67e575c194da46b; _uetvid=252a33b0419e11ec8cb0ad8ad0a31d88; _BEAMER_LAST_UPDATE_zeKLgqli17986=1636490878320; _hjKB={"stateHistory":[{"name":"login","timestamp":1636492020492},{"name":"dashboard","timestamp":1636491998262},{"name":"login","timestamp":1636491980774}],"latestSite":"1764089"}; ajs_anonymous_id=d16dbee0-7638-4c8d-96ae-7d0674e1f13a; _dd_s=rum=0&expire=1636492935682; SESSION-ID=98316a4b8e83363624f07b929a6ddc011c05115ad66706203815bf7b; LOGGED-IN=1; XDOMAIN-LOGGED-IN=1; XSRF-TOKEN=IrWEysvBgk4az6EDO0PKwMqhjlR1Fabl8K5wPnr4jyuX9yERfQqRfUBYPQvueCn28yq0RXdAL83mPw9Ay8D8qw==; ajs_user_id=2627426',
                    'origin': 'https://insights.hotjar.com',
                    'referer': 'https://insights.hotjar.com/login',
                    'sec-fetch-dest': 'empty',
                    'sec-fetch-mode': 'cors',
                    'sec-fetch-site': 'same-origin',
                    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.122 Safari/537.36'
        }
        #New header and URL added for grabbing hotjar data from the staging environment
        getstageheader = {
                'authority': 'insights.hotjar.com',
                'method': 'GET',
                'path': '/api/v1/sites/1764089/feedback/305636/responses?fields=browser,content,created_datetime_string,created_epoch_time,country_code,country_name,device,id,index,os,response_url,short_visitor_uuid,window_size&sort=-id&offset=0&amount=30000&format=csv&filter=created__ge__2009-12-19',
                'scheme': 'https',
                'accept': '*/*',
                'accept-encoding': 'gzip, deflate',
                'accept-language': 'en-US,en;q=0.9',
                'referer': 'https://insights.hotjar.com/sites/1764089/feedback/responses/305636',
                'sec-fetch-dest': 'empty',
                'sec-fetch-mode': 'cors',
                'sec-fetch-site': 'same-origin',
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.66 Safari/537.36'
        }
        stageDLurl = 'https://insights.hotjar.com/api/v1/sites/1764089/feedback/305636/responses?fields=browser,content,created_datetime_string,created_epoch_time,country_code,country_name,device,id,index,os,response_url,short_visitor_uuid,window_size&sort=-id&offset=0&amount=30000&format=csv&filter=created__ge__2009-12-19'
        #Needs to be hidden using env variables
        tenantid = os.environ.get('RA_TENANT')
        credentials = (os.environ.get('CLIENT_ID'), os.environ.get('CLIENT_SECRET'))
        account = Account(credentials, auth_flow_type='credentials', tenant_id=tenantid)
        if account.authenticate():
            print('Authenticated!')
        rok_shar=account.sharepoint()
        #searchres=rok_shar.search_site('hotjar')
        #print(searchres)
        site = rok_shar.get_site('rockwellautomation.sharepoint.com','/sites/HotjarComments')
        share_point_list = site.get_list_by_name('Feedback List')
        list_items = share_point_list.get_items()
        # create a list of object_id values for all sharepoint list item rows
        id_list = []
        for item in list_items:
            id_list.append(item.object_id)
        # create list of share point list item field value dictionaries using the ids, only use the title column
        list_row_values = []
        for item_id in id_list:
            list_row = share_point_list.get_item_by_id(item_id, ['Title'])
            list_row_values.append(list_row.fields)
        df = pd.DataFrame(list_row_values)
        print('Issue IDs collected')

        existing_list = df['Title'].tolist()
        #Get the hidden user list
        hiddenUser_list = site.get_list_by_name('User Information List')
        userlist_items = hiddenUser_list.get_items()
        id_list2 = []
        for item2 in userlist_items:
            id_list2.append(item2.object_id)
        # create list of share point list item field value dictionaries using the ids
        list_row_values2 = []
        for item_id2 in id_list2:
            list_row2 = hiddenUser_list.get_item_by_id(item_id2)
            list_row_values2.append(list_row2.fields)
        df2 = pd.DataFrame(list_row_values2)

        poi_dic = {}
        names_list = ['Joseph Harkulich',
                      'Sophia Abdelmawla',
                      'Melanie Gee',
                      'Susan Peirson',
                      'Deborah Hoffman',
                      'Amy Schultz',
                      'Eric Solderitsch',
                      'Brad West',
                      'Matthew Huth',
                      'Marina Sedmak',
                      'Susan Stuebe',
                      'Marcelo Ocampo',
                      'Dave Picou',
                      'Christa Andradewi']
        for name in names_list:
            targetassignID = df2.loc[df2['Title'] == name,'id'].values[0]
            poi_dic[name] = targetassignID

        with requests.Session() as session:
            payload = {"action":"login", "email":"ra.hj.automation@gmail.com", "password": os.environ.get('PW_HJ')}
            rp = session.post(loginurl, data=json.dumps(payload), headers=postheader)
            with session.get(dlurl, headers=headexp, stream=True) as r:
                with open('feedback-256010.csv', 'wb') as fd:
                    for chunk in r.iter_content(chunk_size=None):
                        fd.write(chunk)

        #Load the ra.com hotjar data and start filtering
        hjdf = pd.read_csv('feedback-256010.csv')
        #Convert number field to string
        hjdf['Number'] = hjdf['Number'].astype(str)
        #Compare new data to sharepoint list, only grab new entries
        newsub_df = hjdf[~hjdf['Number'].isin(existing_list)]
        #Drop any entries with no feedback message
        newsub_df = newsub_df.dropna(subset=['Message'])
        #Fill NaN email values with an empty string
        newsub_df['Email'] = newsub_df['Email'].fillna('')
        #Convert emotion datatype to float64
        newsub_df['Emotion (1-5)'] = newsub_df['Emotion (1-5)'].astype(np.float64)
        #Convert source URL field to string
        newsub_df['Source URL'] = newsub_df['Source URL'].astype(str)
        #Only grab entries at the start of the time specififed
        newsub_df = newsub_df[newsub_df['Date Submitted'] > '2021-03-29']

        for row in newsub_df.itertuples():
            print(row.Number+' was sent')
            #---Assignments---
            assignID = poi_dic['Joseph Harkulich']
            if (('//locator.rockwellautomation' or '//partners.rockwellautomation.com') in str(row._5)):
                #Deb
                assignID = poi_dic['Deborah Hoffman']
            if (('//compatibility.rockwellautomation' or '//download.rockwellautomation.com') in str(row._5)):
                #Eric
                assignID = poi_dic['Eric Solderitsch']
            if ('//activate.rockwellautomation.com' in str(row._5)):
                #Dave Picou
                assignID = poi_dic['Dave Picou']
            camp_pages = ['//events.rockwellautomation','//campaigns.rockwellautomation']
            if any(i in row._5 for i in camp_pages):
                assignID = poi_dic['Brad West']
            if ('https://www.rockwellautomation.com' in str(row._5)):
                pages = ['/industry/','/capability','/products/software','industries.html','capabilities.html','products.html','/industries/','/capabilities/','/support/documentation/']
                pages2 = ['/products/hardware','/company/','company.html']
                if ('https://www.rockwellautomation.com/search' in row._5):
                    #Matt gets all search
                    assignID = poi_dic['Matthew Huth']
                elif ('/literature-library.html' in row._5):
                    #Marina gets lit library, tech docs
                    assignID = poi_dic['Marina Sedmak']
                elif ('/ecommerce/' in row._5):
                    #Christa gets ecommerce
                    assignID = poi_dic['Christa Andradewi']
                elif any(i in row._5 for i in pages):
                    #Sue gets software, ind, capa
                    assignID = poi_dic['Susan Peirson']
                elif any(i in row._5 for i in pages2):
                    if ('/company/' in row._5):
                        sue_comp = ['/company/news/presentations','/company/news/demonstrations']
                        if any(i in row._5 for i in sue_comp):
                            assignID = poi_dic['Susan Peirson']
                        else:
                            assignID = poi_dic['Melanie Gee']
                    else:
                        assignID = poi_dic['Melanie Gee']
                elif ('/proposalworks-proposal-builder' in str(row._5)):
                    #Marcelo gets proposalworks
                    assignID = poi_dic['Marcelo Ocampo']
                elif ('rockwellautomation.com/my' in str(row._5)):
                    #Matt gets myrockwell
                    assignID = poi_dic['Matthew Huth']
                else:
                    #Default to Susan Stuebe
                    assignID = poi_dic['Susan Stuebe']
            #Priority 1 - Emotion 1 + Email = Joe
            if (row.Email != '' and row._10 == 1):
                assignID = poi_dic['Joseph Harkulich']
            #---End assignments---
            body = {'Title':row.Number,
                    'Country':row.Country,
                    'Source_x0020_URL':row._5,
                    'Email':row.Email,
                    'Message':row.Message,
                    'Emotion_x0020__x0028_1_x002d_5_x':row._10,
                    'Browser':row.Browser,
                    'OS':row.OS,
                    'Date_x0020_Submitted':row._3,
                    'Device':row.Device,
                    'PrimaryAssigneeLookupId':float(assignID),
                    'LinktoHotJar':row._9}
            #NLP classification
            doc = nlpmodel(str(row.Message))
            if doc.cats['ux_issue'] > 0.85:
                body['n223ed48f5c94e38b4215edbe1a59772'] = '-1;#UX Issue|55e6360a-eb37-4dd6-8f86-06e8d6997e66'
            #---Create the new list items---
            share_point_list.create_list_item(body)
    sched = BlockingScheduler()
    sched.add_job(scheduledtask,'interval', hours=6, id='update_sharepointlist')
    sched.start()
if __name__ == "__main__":
    main()
