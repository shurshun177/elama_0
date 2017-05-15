import requests
import time
from pandas import DataFrame
from urllib.parse import urlparse
import sys
import hashlib
args = sys.argv[1:]
args.sort()
h = hashlib.md5(''.join(args).encode()).hexdigest()
login = args[-1]
def data_getting(campaignID = '24646526', login1 = 'context.elama'):
    u = ('http://elama.ru/integration/account/context.elama/campaign'
        '/24647324/banners-with-prices-direct-api5?verify_authentication=false')
    url_from_task = u
    host_name = 'http://elama.ru'
    url_raw = urlparse(url_from_task)
    url_to_change = url_raw[2].split('/')
    url_to_change[3], url_to_change[5] = login1, str(campaignID)
    url_after_change = '/'.join(url_to_change)
    url = host_name + url_after_change + '?' + url_raw[-2]

    r=requests.get(url,headers = {"Authorization":'**************************'})
    try:
        d=r.json()

    except Exception:
        with open('log.txt','a') as fl:
            print('Status code is {} Server error campaignID = {}, login1 = {}'
                  'Time = {}'.format(r.status_code,campaignID,login1,time.asctime()), file=fl)
    else:
        #print(r.status_code,r.json(),r.content,sep='\n')
        return d

def main():
    def data_processing(login1,campaignID):
        jso=data_getting(login1=login1,campaignID=campaignID)
        if jso is None:
            with open('log.txt', 'a') as fl:
                print('json object is None, time = {}'.format(time.asctime()), file=fl)
        elif len(jso) == 0:
            with open('log.txt', 'a') as fl:
                print('Empty json, login-1= {}, campaignID= {},time = {}'.format(login1, campaignID, time.asctime()), file=fl)
        elif len(jso) == 3:
            with open('log.txt', 'a') as fl:
                print('Error   ===== {}\nMessage ===== {}'
                      '\nlogin-1 = {}, campaignId = {},time = {}'.format(jso['errorCode'],
                                                                         jso['errorMessage'], login1, campaignID,
                                                                         time.asctime()), file=fl)
        else:
            values_list = [i.get('Keywords') for i in jso]  # List of values with key 'Keywords '
            q = (i.values() if i is not None else None for i in values_list)  # Generator of values in values_list
            e = [i for i in list(q)]
            len_e = len(e)
            id_list = [i['Id'] for i in jso]
            banner_id.extend(id_list)
            ad_group_id.extend([i.get('AdGroupId') for i in jso])
            list_dict = []
            bids_list = []
            banner_ch = []
            for j in range(len_e):
                if e[j] is None:
                    banner_ch.append(id_list[j])
                    list_dict.append(None)
                    value.append(None)
                    bids_list.append(None)
                else:
                    for i in e[j]:
                        value.append(i.get('Keyword'))
                        banner_ch.append(id_list[j])
                        list_dict.append(i)
                        bids_list.append(i.get('Bids'))
            banner_id_kw.extend(banner_ch)
            pre_prod = [i.get('Productivity') if i is not None else None for i in list_dict]
            productivity.extend([i.get('Value') if i is not None else None for i in pre_prod])
            curprice.extend([(i['Bid'])/1000000*1.18 if i is not None else None for i in list_dict])# update
            kw_state.extend([i['State'] if i is not None else None for i in list_dict])
            kw_status.extend([i['Status'] if i is not None else None for i in list_dict])
            pre_phrase = [i['Id'] if i is not None else None for i in list_dict]
            phrase_id.extend(pre_phrase)
            ban_f.extend(list(map(lambda x,y:'{0}_{1}'.format(x,y),banner_ch,phrase_id)))
            userparam1.extend([i.get('UserParam1') if i is not None else None for i in list_dict])
            userparam2.extend([i.get('UserParam2') if i is not None else None for i in list_dict])
            ad_group_id_kw.extend([i.get('AdGroupId') if i is not None else None for i in bids_list])
            pre_min=(i.get('MinSearchPrice') if i is not None else None for i in bids_list)
            min_searchprice.extend([(i)/1000000*1.18 if i is not None else None for i in pre_min])
            pre_cur=(i.get('CurrentSearchPrice') if i is not None else None for i in bids_list)
            current_searchprice.extend([i/1000000*1.18 if i is not None else None for i in pre_cur])
            strategy_priority.extend([i.get('StrategyPriority') if i is not None else None for i in bids_list])
            auc_gen = [i.get('AuctionBids') if i is not None else None for i in bids_list]
            pp11=(i[0].get('Price') if i is not None else None for i in auc_gen)
            p11_price.extend([i/1000000*1.18 if i is not None else '00000000000000' for i in pp11])
            pb11=(i[0].get('Bid') if i is not None else None for i in auc_gen)
            p11_bid.extend([i/1000000*1.18 if i is not None else '00000000000000' for i in pb11])
            pp12=(i[1].get('Price') if i is not None else None for i in auc_gen)
            p12_price.extend([i/1000000*1.18 if i is not None else '00000000000000' for i in pp12])
            pb12=(i[1].get('Bid') if i is not None else None for i in auc_gen)
            p12_bid.extend([i/1000000*1.18 if i is not None else '00000000000000' for i in pb12])
            pp13=(i[2].get('Price') if i is not None else None for i in auc_gen)
            p13_price.extend([i/1000000*1.18 if i is not None else '00000000000000' for i in pp13])
            pb13=(i[2].get('Bid') if i is not None else None for i in auc_gen)
            p13_bid.extend([i/1000000*1.18 if i is not None else '00000000000000' for i in pb13])
            pp21=(i[3].get('Price') if i is not None else None for i in auc_gen)
            p21_price.extend([i/1000000*1.18 if i is not None else '00000000000000' for i in pp21])
            pb21=(i[3].get('Bid') if i is not None else None for i in auc_gen)
            p21_bid.extend([i/1000000*1.18 if i is not None else '00000000000000' for i in pb21])
            pp22=(i[4].get('Price') if i is not None else None for i in auc_gen)
            p22_price.extend([i/1000000*1.18 if i is not None else '00000000000000' for i in pp22])
            pb22=(i[4].get('Bid') if i is not None else None for i in auc_gen)
            p22_bid.extend([i/1000000*1.18 if i is not None else '00000000000000' for i in pb22])
            pp23=(i[5].get('Price') if i is not None else None for i in auc_gen)
            p23_price.extend([i/1000000*1.18 if i is not None else '00000000000000' for i in pp23])
            pb23=(i[5].get('Bid') if i is not None else None for i in auc_gen)
            p23_bid.extend([i/1000000*1.18 if i is not None else '00000000000000' for i in pb23])
            pp24=(i[6].get('Price') if i is not None else None for i in auc_gen)
            p24_price.extend([i/1000000*1.18 if i is not None else '00000000000000' for i in pp24])
            pb24=(i[6].get('Bid') if i is not None else None for i in auc_gen)
            p24_bid.extend([i/1000000*1.18 if i is not None else '00000000000000' for i in pb24])
            campaign_id.extend([i['CampaignId'] for i in jso])
            ban_status.extend([i.get('Status') for i in jso])
            ban_state.extend([i.get('State') for i in jso])
            age_label.extend([i.get('AgeLabel') for i in jso])
            type.extend([i.get('Type') for i in jso])
            text_ad = [i.get('TextAd') for i in jso]
            ad_group_list = [i.get('AdGroup') for i in jso]
            '''values in TEXT_AD'''
            dup.extend([i.get('DisplayUrlPath') for i in text_ad])
            domain.extend([i.get('DisplayDomain') for i in text_ad])
            mobile.extend([i.get('Mobile') for i in text_ad])
            ad_image_hash.extend([False if i.get('AdImageHash') else True for i in text_ad])
            text.extend([i.get('Text') for i in text_ad])
            title.extend([i.get('Title') for i in text_ad])
            href.extend([i.get('Href') for i in text_ad])
            vcard_list = [i.get('VCard') for i in text_ad]
            sitelink_list = [i.get('Sitelinks') for i in text_ad]
            sl_titles = []
            for j in range(len(sitelink_list)):
                if sitelink_list[j] is None:
                    sl_titles.append(None)
                else:

                    sl_titles.append([i.get('Title')  for i in sitelink_list[j]])
            sl_title_1.extend([i[0] if i is not None else None for i in sl_titles])
            sl_title_2.extend([i[1] if i is not None else None for i in sl_titles])
            sl_title_3.extend([i[2] if i is not None else None for i in sl_titles])
            sl_title_4.extend([i[3] if i is not None else None for i in sl_titles])
            '''values in VCard'''
            contact_email.extend([i.get('ContactEmail') if i else None for i in vcard_list])
            city.extend([i.get('City') if i else None for i in vcard_list])
            work_time.extend([i.get('WorkTime') if i else None for i in vcard_list])
            company_name.extend([i.get('CompanyName') if i else None for i in vcard_list])
            metrostation_id.extend([i.get('MetrostationId') if i else None for i in vcard_list])
            pre_geo=[i.get('PointOnMap') if i is not None else None for i in vcard_list]
            got_geocoordinates.extend([True if i else False for i in pre_geo])
            number_gen = (i.get('Phone') if i else None for i in vcard_list)
            phone.extend([i.get('PhoneNumber') if i else None for i in number_gen])
            '''values in AD_GROUP'''
            pre_neg_key = (i.get('NegativeKeywords') for i in ad_group_list)
            pre_neg_key_1 = (i.get('Items') for i in pre_neg_key)
            n_k = ((map(lambda x:'-{}'.format(x),i)) if isinstance(i,list) else '-{}'.format(i) if i is not None else None for i in pre_neg_key_1)
            #negative_keywords.extend([i.strip('[]') for i in ('{}'.format(list(j)) for j in n_k)])
            a=[i.strip('[]') for i in (list(j) for j in n_k)]
            q=[i.strip('"') for i in a]
            pre_reg = ('{}'.format(j) for j in [i.get('RegionIds') for i in ad_group_list])
            regions.extend([i.strip('[]') for i in pre_reg])
            ad_group_name.extend([i.get('Name') for i in ad_group_list])
            group_status.extend([i.get('Status') for i in ad_group_list])
            group_type.extend([i.get('Type') for i in ad_group_list])
            track_par.extend([i.get('TrackingParams') for i in ad_group_list])
    def data_frames(h):
        df_0 = DataFrame({'phrase_id': phrase_id, 'value': value,
                          'ad_group_id': ad_group_id_kw, 'banner_id': banner_id_kw,
                          'curPrice': curprice, 'StrategyPriority': strategy_priority,
                          'KW_state': kw_state, 'KW_status': kw_status,
                          'UserParam1': userparam1, 'UserParam2': userparam2,
                          'Productivity': productivity,
                          'MinSearchPrice': min_searchprice,
                          'CurrentSearchPrice': current_searchprice,
                          'P11_bid': p11_bid, 'P12_bid': p12_bid,
                          'P13_bid': p13_bid, 'P21_bid': p21_bid,
                          'P22_bid': p22_bid, 'P23_bid': p23_bid,
                          'P24_bid': p24_bid, 'P11_price': p11_price,
                          'P12_price': p12_price, 'P13_price': p13_price,
                          'P21_price': p21_price, 'P22_price': p22_price,
                          'P23_price': p23_price, 'P24_price': p24_price,'bannerPhrase':ban_f},

                         columns=['banner_id', 'ad_group_id', 'phrase_id',
                                  'value', 'curPrice', 'StrategyPriority',
                                  'KW_state', 'KW_status', 'UserParam1',
                                  'UserParam2', 'Productivity',
                                  'MinSearchPrice', 'CurrentSearchPrice',
                                  'P11_bid', 'P12_bid', 'P13_bid', 'P21_bid',
                                  'P22_bid', 'P23_bid', 'P24_bid', 'P11_price',
                                  'P12_price', 'P13_price', 'P21_price',
                                  'P22_price', 'P23_price', 'P24_price','bannerPhrase'])
        df_3 = DataFrame({'campaign_id' : campaign_id, 'regions' : regions,
                                        'banner_id' : banner_id},
                       columns=['campaign_id', 'banner_id', 'regions'])
        df_2 = DataFrame({'banner_id':banner_id,'campaign_id':campaign_id,'Text':text,
                          'ad_group_id':ad_group_id,'bannerStatus':ban_status,
                          'bannerState':ban_state,'AgeLabel':age_label,
                          'Type':type,'AdImageHash':ad_image_hash,'Geo':regions,
                          'Title':title, 'Href':href, 'Mobile':mobile,
                          'Domain':domain,'DisplayUrlPath':dup,'City':city,
                          'WorkTime':work_time, 'Phone':phone,
                          'CompanyName':company_name,'ContactEmail':contact_email,
                          'MetroStationId':metrostation_id,'SLtitle1':sl_title_1,
                          'gotGEOcoordinates':got_geocoordinates,
                          'NegativeKeywords':negative_keywords,
                          'SLtitle2':sl_title_2, 'SLtitle3':sl_title_3,
                          'SLtitle4':sl_title_4,'groupType':group_type,
                          'AdGroupName':ad_group_name,
                          'TrackingParams':track_par,'groupStatus':group_status},
                          columns=['banner_id', 'campaign_id', 'ad_group_id',
                                  'bannerStatus', 'bannerState', 'AgeLabel',
                                  'Type', 'AdImageHash', 'Text', 'Title',
                                  'Href', 'Mobile', 'Domain', 'DisplayUrlPath',
                                  'City', 'WorkTime', 'Phone', 'CompanyName',
                                  'ContactEmail', 'MetroStationId',
                                  'gotGEOcoordinates', 'SLtitle1',
                                  'SLtitle2', 'SLtitle3', 'SLtitle4', 'Geo',
                                  'NegativeKeywords', 'AdGroupName',
                                  'groupStatus','groupType','TrackingParams'],)
        with open('test_{}.csv'.format(h),'w') as f:
            df_0.to_csv(f,index=False,sep='\t',encoding='utf-8')
            df_2.to_csv(f, index=False, sep='\t',encoding='utf-8')
            df_3.to_csv(f, index=False, sep='\t')
            print(f.name)

    banner_id = []
    banner_id_kw = []
    value = []
    productivity = []
    curprice = []
    kw_state = []
    kw_status = []
    phrase_id = []
    ad_group_id = []
    ad_group_id_kw = []
    min_searchprice = []
    current_searchprice = []
    strategy_priority = []
    userparam1 = []
    userparam2 = [] #empty srings

    '''
       Bids and Prices of positions
    '''
    p11_price = []
    p11_bid = []
    p12_price = []
    p12_bid = []
    p13_price = []
    p13_bid = []
    p21_price = []
    p21_bid = []
    p22_price = []
    p22_bid = []
    p23_price = []
    p23_bid = []
    p24_price = []
    p24_bid = []
    campaign_id = []
    regions = []  # GEO in the second dataframe
    ban_status = []
    ban_state = []
    age_label = []
    type = []
    ad_image_hash = []
    dup = []
    domain = []
    negative_keywords = []
    ad_group_name = []
    group_status = []
    group_type = []
    track_par = []
    mobile = []
    text = []
    title = []
    href = []
    city = []
    work_time = []
    phone = []
    company_name = []
    contact_email = []
    metrostation_id = []
    got_geocoordinates = []
    sl_title_1 = []
    sl_title_2 = []
    sl_title_3 = []
    sl_title_4 = []
    ban_f = []
    for i in args:
        data_processing(login1=login,campaignID=i)
    data_frames(h)
main()
