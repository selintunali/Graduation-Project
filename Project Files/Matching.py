
## This Script is used for creating matching scores

import pandas as pd
from fuzzywuzzy import fuzz
import numpy as np
from datetime import datetime
filename = '2021-12-30'
df_clean = pd.read_csv(r"/home/selintunalitunali/SE 494 Project/"+filename+".csv")
df_clean.fillna('', inplace=True)
df_clean.rename(columns={ df_clean.columns[0]: "Root" }, inplace = True)
df_clean.rename(columns={ df_clean.columns[1]: "Prefix" }, inplace = True)
df_clean.rename(columns={ df_clean.columns[2]: "Suffix" }, inplace = True)
df_clean['index_col'] = df_clean.index

def exact_match(x,y,score):
    if x == y:
        return score
    else:
        return 0

#def matching(x):
print(datetime.now())
column_names=['similarityscore','Indname','Indadd','Outname','Outaddr','Index_col']
matchcombined = pd.DataFrame(columns=column_names)
df_cleanedfiltered = {}
df_cleaned= df_clean.copy().to_dict(orient = 'records')

for i in range(0,len(df_cleaned)):
    
        print('row:',i)
        listofexisting = list(matchcombined['Index_col'])
        cleanedList = [x for x in listofexisting if str(x) != 'nan']
        finallist = [int(x) for x in cleanedList] 

        if i in finallist:
            #print(i,' already exists')
            continue
        else:
            zipfilter = df_cleaned[i]['ZipCode']
            streetnamefilter = df_cleaned[i]['StreetName']
            addressnofilter = df_cleaned[i]['AddressNumber']
            poboxid = df_cleaned[i]['USPSBoxID']
            df_cleanedfiltered = df_cleaned
            df_cleanedfiltered = [d for d in df_cleanedfiltered if d['index_col'] != i]
            df_cleanedfiltered = [d for d in df_cleanedfiltered if d['index_col'] != i]
            df_cleanedfiltered = [d for d in df_cleanedfiltered if d['ZipCode'] == zipfilter and d['StreetName'] == streetnamefilter and d['AddressNumber'] == addressnofilter and d['USPSBoxID'] == poboxid]
            treshold = 95

            for j in range(0,len(df_cleanedfiltered)):
                        #print('j:',j)
                        if df_cleaned[i]['USPSBoxID'] == '':
                            
                            addresscore = exact_match(df_cleaned[i]['AddressNumber'],df_cleanedfiltered[j]['AddressNumber'],5)+exact_match(df_cleaned[i]['StreetNamePreDirectional'],df_cleanedfiltered[j]['StreetNamePreDirectional'],3)+exact_match(df_cleaned[i]['StreetType'],df_cleanedfiltered[j]['StreetType'],3)+exact_match(df_cleaned[i]['StreetNamePostDirectional'],df_cleanedfiltered[j]['StreetNamePostDirectional'],3)+exact_match(df_cleaned[i]['OccupancyType'],df_cleanedfiltered[j]['OccupancyType'],5)+exact_match(df_cleaned[i]['OccupancyIdentifier'],df_cleanedfiltered[j]['OccupancyIdentifier'],3)+exact_match(df_cleaned[i]['RouteType'],df_cleanedfiltered[j]['RouteType'],3)+exact_match(df_cleaned[i]['BuildingName'],df_cleanedfiltered[j]['BuildingName'],3)+exact_match(df_cleaned[i]['SubaddressType'],df_cleanedfiltered[j]['SubaddressType'],2)+exact_match(df_cleaned[i]['SubaddressIdentifier'],df_cleanedfiltered[j]['SubaddressIdentifier'],2)+exact_match(df_cleaned[i]['City'],df_cleanedfiltered[j]['City'],25)+exact_match(df_cleaned[i]['State'],df_cleanedfiltered[j]['State'],40)
                            namescore = fuzz.ratio(df_cleaned[i]['Root'],df_cleanedfiltered[j]['Root'])*0.7+exact_match(df_cleaned[i]['Prefix'],df_cleanedfiltered[j]['Prefix'],20)+exact_match(df_cleaned[i]['Suffix'],df_cleanedfiltered[j]['Suffix'],10)
                            similarityscore = 0.7*addresscore + 0.3*namescore
                            #print('similarityscore:',int(similarityscore))
                            
                            if int(similarityscore) == 100:
                                
                                #print('score 100, done')
                                newrow = {'similarityscore':similarityscore,                                          'Indname':str(df_cleaned[i]['Root'])+' '+                                                 str(df_cleaned[i]['Prefix'])+' '+str(df_cleaned[i]['Suffix']),                                          'Indadd':str(df_cleaned[i]['AddressNumber'])+' '+                                                 str(df_cleaned[i]['StreetNamePreDirectional'])+' '+str(df_cleaned[i]['StreetName'])+' '+                                                 str(df_cleaned[i]['StreetNamePostDirectional'])+' '+str(df_cleaned[i]['RouteType'])+' '+                                                 str(df_cleaned[i]['StreetType'])+' '+str(df_cleaned[i]['BuildingName'])+' '+                                                 str(df_cleaned[i]['OccupancyType'])+ ' ' + str(df_cleaned[i]['OccupancyIdentifier'])+' '+                                                 str(df_cleaned[i]['SubaddressType'])+' '+str(df_cleaned[i]['SubaddressIdentifier'])+' '+                                                 str(df_cleaned[i]['City'])+' '+str(df_cleaned[i]['State'])+' '+str(df_cleaned[i]['ZipCode']),
                                          'Outname':str(df_cleanedfiltered[j]['Root'])+' '+\
                                                 str(df_cleanedfiltered[j]['Prefix'])+' '+str(df_cleanedfiltered[j]['Suffix']),\
                                          'Outaddr':str(df_cleanedfiltered[j]['AddressNumber'])+' '+\
                                                 str(df_cleanedfiltered[j]['StreetNamePreDirectional'])+' '+str(df_cleanedfiltered[j]['StreetName'])+' '+\
                                                 str(df_cleanedfiltered[j]['StreetNamePostDirectional'])+' '+str(df_cleanedfiltered[j]['RouteType'])+' '+\
                                                 str(df_cleanedfiltered[j]['StreetType'])+' '+str(df_cleanedfiltered[j]['BuildingName'])+' '+\
                                                 str(df_cleanedfiltered[j]['OccupancyType'])+ ' ' + str(df_cleanedfiltered[j]['OccupancyIdentifier'])+' '+\
                                                 str(df_cleanedfiltered[j]['SubaddressType'])+' '+str(df_cleanedfiltered[j]['SubaddressIdentifier'])+' '+\
                                                 str(df_cleanedfiltered[j]['City'])+' '+str(df_cleanedfiltered[j]['State'])+' '+str(df_cleanedfiltered[j]['ZipCode']),\
                                            'Index_col':str(df_cleanedfiltered[j]['index_col'])}
                                
                                matchcombined = matchcombined.append(pd.DataFrame(newrow, index = [i]),ignore_index = True)
                                break
                            
                            elif int(similarityscore)>= treshold:
                                
                                newrow = {'similarityscore':similarityscore,'Indname':str(df_cleaned[i]['Root'])+' '+                                          str(df_cleaned[i]['Prefix'])+' '+str(df_cleaned[i]['Suffix']),                                          'Indadd':str(df_cleaned[i]['AddressNumber'])+' '+                                          str(df_cleaned[i]['StreetNamePreDirectional'])+' '+str(df_cleaned[i]['StreetName'])+                                          ' '+str(df_cleaned[i]['StreetNamePostDirectional'])+' '+
                                          str(df_cleaned[i]['RouteType'])+' '+str(df_cleaned[i]['StreetType'])\
                                          +' '+str(df_cleaned[i]['BuildingName'])+' '+str(df_cleaned[i]['OccupancyType'])+\
                                          ' ' + str(df_cleaned[i]['OccupancyIdentifier'])+' '+str(df_cleaned[i]['SubaddressType'])+\
                                          ' '+str(df_cleaned[i]['SubaddressIdentifier'])+' '+str(df_cleaned[i]['City'])+' '+\
                                          str(df_cleaned[i]['State'])+' '+str(df_cleaned[i]['ZipCode']),
                                          'Outname':str(df_cleanedfiltered[j]['Root'])+' '+\
                                                 str(df_cleanedfiltered[j]['Prefix'])+' '+str(df_cleanedfiltered[j]['Suffix']),\
                                          'Outaddr':str(df_cleanedfiltered[j]['AddressNumber'])+' '+\
                                                 str(df_cleanedfiltered[j]['StreetNamePreDirectional'])+' '+str(df_cleanedfiltered[j]['StreetName'])+' '+\
                                                 str(df_cleanedfiltered[j]['StreetNamePostDirectional'])+' '+str(df_cleanedfiltered[j]['RouteType'])+' '+\
                                                 str(df_cleanedfiltered[j]['StreetType'])+' '+str(df_cleanedfiltered[j]['BuildingName'])+' '+\
                                                 str(df_cleanedfiltered[j]['OccupancyType'])+ ' ' + str(df_cleanedfiltered[j]['OccupancyIdentifier'])+' '+\
                                                 str(df_cleanedfiltered[j]['SubaddressType'])+' '+str(df_cleanedfiltered[j]['SubaddressIdentifier'])+' '+\
                                                 str(df_cleanedfiltered[j]['City'])+' '+str(df_cleanedfiltered[j]['State'])+' '+str(df_cleanedfiltered[j]['ZipCode']),\
                                                 'Index_col':str(df_cleanedfiltered[j]['index_col'])}
                                
                                matchcombined = matchcombined.append(pd.DataFrame(newrow, index = [i]),ignore_index = True)

                        else:
                            #print('poboxexists')

                            addresscore = exact_match(df_cleaned[i]['USPSBoxID'],df_cleanedfiltered[j]['USPSBoxID'],70)+exact_match(df_cleaned[i]['USPSBoxType'],df_cleanedfiltered[j]['USPSBoxType'],30)
                            namescore = fuzz.ratio(df_cleaned[i]['Root'],df_cleanedfiltered[j]['Root'])*0.7+exact_match(df_cleaned[i]['Prefix'],df_cleanedfiltered[j]['Prefix'],20)+exact_match(df_cleaned[i]['Suffix'],df_cleanedfiltered[j]['Suffix'],10)
                            similarityscore = 0.9*addresscore + 0.1*namescore
                            #print('similarityscore:',int(similarityscore))
                            
                            if int(similarityscore) == 100:
                                #print('score 100, done')
                                newrow = {'similarityscore':similarityscore,'Indname':str(df_cleaned[i]['Root'])+' '+str(df_cleaned[i]['Prefix'])+' '+str(df_cleaned[i]['Suffix']),'Indadd':str(df_cleaned[i]['USPSBoxID'])+' '+str(df_cleaned[i]['USPSBoxType'])+' '+str(df_cleaned[i]['City']),'Outname':str(df_cleanedfiltered[j]['Root'])+' '+str(df_cleanedfiltered[j]['Prefix'])+' '+str(df_cleanedfiltered[j]['Suffix']),'Outaddr':str(df_cleanedfiltered[j]['USPSBoxID'])+' '+str(df_cleanedfiltered[j]['USPSBoxType'])+' '+str(df_cleanedfiltered[j]['City'])+' '+str(df_cleanedfiltered[j]['State'])+' '+str(df_cleanedfiltered[j]['ZipCode']),'Index_col':str(df_cleanedfiltered[j]['index_col'])}
                                matchcombined = matchcombined.append(pd.DataFrame(newrow, index = [i]),ignore_index = True)
                                break
                            
                            elif int(similarityscore)>= treshold:
                                newrow = {'similarityscore':similarityscore,'Indname':str(df_cleaned[i]['Root'])+' '+str(df_cleaned[i]['Prefix'])+' '+str(df_cleaned[i]['Suffix']),'Indadd':str(df_cleaned[i]['USPSBoxID'])+' '+str(df_cleaned[i]['USPSBoxType'])+' '+str(df_cleaned[i]['City']),'Outname':str(df_cleanedfiltered[j]['Root'])+' '+str(df_cleanedfiltered[j]['Prefix'])+' '+str(df_cleanedfiltered[j]['Suffix']),'Outaddr':str(df_cleanedfiltered[j]['USPSBoxID'])+' '+str(df_cleanedfiltered[j]['USPSBoxType'])+' '+str(df_cleanedfiltered[j]['City'])+' '+str(df_cleanedfiltered[j]['State'])+' '+str(df_cleanedfiltered[j]['ZipCode']),'Index_col':str(df_cleanedfiltered[j]['index_col'])}
                                matchcombined = matchcombined.append(pd.DataFrame(newrow, index = [j]),ignore_index = True)
                                
print(datetime.now())

for i in range(0,len(df_cleaned)):
    listofexistingindex = list(matchcombined['Index_col'])
    cleaned_List = [x for x in listofexistingindex if str(x) != 'nan']
    indexlist = [int(x) for x in cleaned_List] 

    if i not in indexlist:
            newrow = {'similarityscore':int(0),'Indname':str(df_cleaned[i]['Root'])+str(df_cleaned[i]['Prefix'])+str(df_cleaned[i]['Suffix']),                      'Indadd':str(df_cleaned[i]['Original_Address']),                     'Outname':str(df_cleaned[i]['Root'])+str(df_cleaned[i]['Prefix'])+str(df_cleaned[i]['Suffix']),                      'Outaddr':str(df_cleaned[i]['Original_Address']),'Index_col':str(df_cleaned[i]['index_col'])}
            matchcombined = matchcombined.append(pd.DataFrame(newrow, index = [i]),ignore_index = True)
            
            
matchcombined.to_csv(r"/home/selintunalitunali/SE 494 Project/matched_"+filename+".csv")
best_matchcombined = matchcombined.sort_values('similarityscore').drop_duplicates(["Indname"],keep='last')
best_matchcombined.to_csv(r"/home/selintunalitunali/SE 494 Project/best_matched_"+filename+".csv")
best_matchcombined['OutboundID'] = best_matchcombined.groupby(['Outaddr']).ngroup()
best_matchcombined = best_matchcombined.sort_values(by=['OutboundID'], ascending=True)
best_matchcombined.to_csv(r"/home/selintunalitunali/SE 494 Project/best+id_matched_"+filename+".csv")

