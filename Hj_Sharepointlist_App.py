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

def main():
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
                    'cookie': '_ga=GA1.2.408059203.1582125822; _gcl_au=1.1.2121588707.1582125822; _hjid=bcbc175b-2df6-400c-9358-65e7f264f87c; _BEAMER_USER_ID_zeKLgqli17986=dcee6938-859e-4348-bd34-f80d53c958b6; _BEAMER_FIRST_VISIT_zeKLgqli17986=2020-02-19T15:23:42.738Z; hubspotutk=c0b18dd390c94a77301d5605b29e6460; _fbp=fb.1.1582125894901.66546557; __zlcmid=wqivZyfBgbR6w5; _gcl_aw=GCL.1585228516.EAIaIQobChMI4N_Cspy46AIVGWyGCh09Mw6hEAAYASAAEgJOoPD_BwE; _gac_UA-51401671-1=1.1585228516.EAIaIQobChMI4N_Cspy46AIVGWyGCh09Mw6hEAAYASAAEgJOoPD_BwE; _hjDonePolls=481939,481419,156128,491599; _hjMinimizedPolls=481906,156128,491599; __hstc=162211107.c0b18dd390c94a77301d5605b29e6460.1582125822938.1585924808327.1586054543411.39; _BEAMER_DATE_zeKLgqli17986=2020-04-23T15:14:50.954Z; _hjIncludedInSample=1; _BEAMER_LAST_POST_SHOWN_zeKLgqli17986=1138945; _hjUserAttributesHash=ee9ffb91a314801cef0a410822bd5c93; receptiveNotificationCount=6; _gid=GA1.2.1421648835.1588522759; _BEAMER_LAST_UPDATE_zeKLgqli17986=1588522765334; _gat=1; ajs_anonymous_id=%22a15ab479-8323-4d1a-9115-a39c48fee53e%22; _dd_s=rum=0&expire=1588538442658',
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
                      'Dave Picou']
        for name in names_list:
            targetassignID = df2.loc[df2['Title'] == name,'id'].values[0]
            poi_dic[name] = targetassignID

        with requests.Session() as session:
            payload = {"action":"login", "email":"rsolande@ra.rockwell.com", "password": os.environ.get('PW_HJ')}
            rp = session.post(loginurl, data=json.dumps(payload), headers=postheader)
            with session.get(dlurl, headers=headexp, stream=True) as r:
                with open('feedback-256010.csv', 'wb') as fd:
                    for chunk in r.iter_content(chunk_size=None):
                        fd.write(chunk)
        with requests.Session() as session:
            payload = {"action":"login", "email":"rsolande@ra.rockwell.com", "password": os.environ.get('PW_HJ')}
            rp = session.post(loginurl, data=json.dumps(payload), headers=postheader)
            with session.get(stageDLurl, headers=getstageheader, stream=True) as r:
                with open('feedback-305636.csv', 'wb') as fd:
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
            if ('//campaign.rockwellautomation' in str(row._5)):
                assignID = poi_dic['Brad West']
            if ('https://www.rockwellautomation.com/' in str(row._5)):
                pages = ['/industry/','/capability','/products/software','industries.html','capabilities.html','products.html','/industries/','/capabilities/']
                pages2 = ['/products/hardware','/company/','company.html']
                pages3 = ['/literature-library.html','/support/documentation/technical']
                if ('https://www.rockwellautomation.com/search' in row._5):
                    #Matt gets all search
                    assignID = poi_dic['Matthew Huth']
                elif any(i in row._5 for i in pages):
                    #Sue gets software, ind, capa
                    assignID = poi_dic['Susan Peirson']
                elif any(i in row._5 for i in pages2):
                    #Mel gets hardware, company
                    assignID = poi_dic['Melanie Gee']
                elif ('/proposalworks-proposal-builder' in str(row._5)):
                    #Marcelo gets proposalworks
                    assignID = poi_dic['Marcelo Ocampo']
                elif any(i in row._5 for i in pages3):
                    #Marina gets lit library, tech docs
                    assignID = poi_dic['Marina Sedmak']
                elif ('rockwellautomation.com/my' in str(row._5)):
                    #Sophia gets myrockwell
                    assignID = poi_dic['Sophia Abdelmawla']
                else:
                    #Default to Susan Stuebe
                    assignID = poi_dic['Susan Stuebe']
            #Priority 1 - Emotion 1 + Email = Joe
            if (row.Email != '' and row._10 == 1):
                assignID = poi_dic['Joseph Harkulich']
            #---If the source URL exceed 255 chars, shorten it for sharepoint---
            #sourceurl = row._5
            #if len(row._5) > 255:
                #sourceurl = row._5[:len(row._5)-(len(row._5)-255)]
            #---Create the new list items---
            share_point_list.create_list_item({'Title':row.Number,
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
                                               'LinktoHotJar':row._9})
    sched = BlockingScheduler()
    sched.add_job(scheduledtask,'interval', hours=6, id='update_sharepointlist')
    sched.start()
if __name__ == "__main__":
    main()
