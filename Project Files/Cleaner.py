import pandas as pd
import numpy as np
import random
import re
import usaddress
import csv
from collections import OrderedDict

pd.set_option("display.max_rows", None, "display.max_columns", None)

tagDict = {
   'AddressNumber': 'AddressNumber',
   'StreetNamePreDirectional': 'StreetNamePreDirectional',
   'StreetName': 'StreetName',
   'StreetNamePostDirectional': 'StreetNamePostDirectional',
   'StreetNamePreType': 'RouteType',
   'StreetNamePostType': 'StreetType',
   'USPSBoxID': 'USPSBoxID',
   'USPSBoxType': 'USPSBoxType',
   'BuildingName': 'BuildingName',
   'OccupancyType': 'OccupancyType',
   'OccupancyIdentifier': 'OccupancyIdentifier',
   'SubaddressType': 'SubaddressType',
   'SubaddressIdentifier': 'SubaddressIdentifier',
   'PlaceName': 'City',
   'StateName': 'State',
   'ZipCode': 'ZipCode'
}
   
def tag_addr(x, tagDict):
    try:
        x = str(x)
        #print(x)
        parsedaddr = usaddress.tag(x, tag_mapping=tagDict)
        #print(parsedaddr)
        return parsedaddr
    except usaddress.RepeatedLabelError as e:
        return (handleError(e.parsed_string, tagDict),'Manual')
        #print("usaddress parsing error! " + str(x))

def handleError(p,tagDict):
    retDict = OrderedDict()
    for v in p:
        key = v[1]
        if key in tagDict.keys():
            key = tagDict[key]
        if key in retDict.keys() and v[0] != retDict[key]:
            if retDict[key] == 'PO' and v[0] == 'BOX':
                retDict[key] = 'PO BOX'
            elif key == 'City':
                retDict[key] = retDict[key] + " " + v[0]
            else:
                retDict[key] = retDict[key] + ", " + v[0]
        else:
            retDict[key] = v[0]
            
    return retDict


def cleanAddress(name, add1, add2, city, st, zc, pfx, sfx, replace, addDict, alphabetize=False, dropDBA=True):
    retName = re.sub(r'[^\w\s]', '', name)
    retName = retName.upper()    
    words = retName.split()
    if dropDBA and "DBA" in words and words[-1] != "DBA":
        words = words[words.index("DBA") + 1:]
    w,p,s = clean_company(words, pfx, sfx, replace)
    
    if len(w) > 1 and w[-2] == "PROFESSIONAL" and w[-1] == "CORPORATION":
        del w[-2]
        del w[-1]
        s.append('PC')
        
    
    
    if add2 != None and add1.find(add2) == -1 and isValidSecondAddr(add2):
        totAddr = add1 + " " + add2
    else:
        totAddr = add1
        
    if city[0:3].upper() == "ST ":
        city = "SAINT " + city[3:]
    elif city[0:4].upper() == "ST. ":
        city = "SAINT " + city[4:]
        
    if city[0:3].upper() == "MT ":
        city = "MOUNT " + city[3:]
    elif city[0:4].upper() == "MT. ":
        city = "MOUNT " + city[4:]
        
    city = city.replace(' ','_')
    rtadd = totAddr + " " + city + " " + st + " " + zc
    rtadd = rtadd.upper()
    rtadd = re.sub(r'[^\w\s]', '', rtadd)
    rtaddList = rtadd.split()
    for i,q in enumerate(rtaddList):
        if q in addDict.keys():
            rtaddList[i] = addDict[q].upper()
    rtadd = ' '.join(rtaddList)
    return w,p,s,rtadd
    
    
    
def cleanAddressParse(name, add1, add2, city, st, zc, pfx, sfx, replace, tagDict, addDict):
    w,p,s,add = cleanAddress(name, add1, add2, city, st, zc, pfx, sfx, replace, addDict)
    addressDict = tag_addr(add, tagDict)
    return w,sorted(p),sorted(s), addressDict


def isValidSecondAddr(line):
    w = line.split()
    for q in w:
        if len(q) == 1 or bool(re.search(r'\d',q)):
            return True
    return False
 
#apply mapping tables here
def clean_company(x, prefix_list, suffix_list, replace_table):
    prefixs = []
    suffixs = []
    for i,w in enumerate(x):
        if w in replace_table.keys():
            x[i] = replace_table[w].upper()
    while len(x) > 0 and x[0] in prefix_list:
        prefixs.append(x[0])
        del x[0]
    while len(x) > 0 and x[-1] in suffix_list:
        suffixs.insert(0,x[-1])
        del x[-1]
    
    return [q for q in x if q], prefixs, suffixs
    

def clean_zip(x):
    x = str(x)
    x = x.replace('.0', '')
    digit = len(x)
    if digit < 5:
        x = str(x).zfill(5)
    elif digit > 5:
        x = str(x)[0:5]
    return str(x)

df = pd.read_excel('uofi_project.xlsx')

df = df.iloc[:, 17:23]

df.fillna('', inplace=True)


df['indzip'] = df['indzip'].apply(lambda x: str(clean_zip(x)))

df = df.astype('string')

with open('Mapping Tables.csv') as infile:
    reader = csv.reader(infile)
    replace_dict = {rows[0]:rows[1] for rows in reader}
    
with open('Mapping Table - State.csv') as infile:
    reader = csv.reader(infile)
    state_table = {rows[0]:rows[1] for rows in reader}
    
with open('Mapping Table - Directional.csv') as infile:
    reader = csv.reader(infile)
    dir_table = {rows[0]:rows[1] for rows in reader}
    del dir_table['']
    
with open('Prefix_table.csv') as infile:
    reader = csv.reader(infile)
    prefix_list = []
    for row in reader:
        prefix_list.append(row[0])
        
with open('Suffix_table.csv') as infile:
    reader = csv.reader(infile)
    suffix_list = []
    for row in reader:
        suffix_list.append(row[0])
        
with open('Address_Mapping_Table.csv') as infile:
    reader = csv.reader(infile)
    addDict = {rows[0]: rows[1] for rows in reader}
    del addDict['Value']
    

#df.apply(lambda row: cleanAddressParse(row[0],row[1],row[2],row[3],row[4],row[5], prefix_list, suffix_list, replace_dict), axis=1)
#df.to_excel("cleaned1.xlsx")

def separate_dict(addr, key):
    if addr == None:
        return None
    addr = dict(addr[0])
    
    if key in addr:
        return addr[key]
    else:
        return None
    
def clean_dir(x,dirs):
    if x in dirs.keys():
        return dirs[x]
    else:
        return x
    
def clean_route_type(x, state_table):
    wrds = x.split()
    for i, q in enumerate(wrds):
        if q in state_table.keys():
            wrds[i] = state_table[q]
    return " ".join(wrds)
    

res = []

for idx, row in df.iterrows():
    w,p,s,add = cleanAddressParse(row[0],row[1],row[2],row[3],row[4],row[5], prefix_list, suffix_list, replace_dict, tagDict, addDict)
    res.append([w,p,s,add])
    
df2 = pd.DataFrame(res)

parsed_address = df2[3]
# After usaddress we need to separate combined columns once again, this is easy to do see the following example

key_list = [tagDict[k] for k in tagDict]

for key in key_list:
    df2[key] = parsed_address.apply(lambda x: separate_dict(x, key))

df2 = df2.fillna('')
df2.drop(3,1,inplace=True)

df2[0] = df2[0].apply(lambda x: ' '.join(x))
df2[1] = df2[1].apply(lambda x: ' '.join(sorted(x)))
df2[2] = df2[2].apply(lambda x: ' '.join(sorted(x)))
df2 = df2.rename(columns={0: 'Root', 1: 'Prefix', 2:'Suffix'})
df2['StreetNamePreDirectional'] = df2['StreetNamePreDirectional'].apply(lambda x: clean_dir(x,dir_table))
df2['RouteType'] = df2['RouteType'].apply(lambda x: clean_route_type(x, state_table))

#df2.to_excel('cleaned1.xlsx', index=False)
df2.to_csv('cleaned1_2.csv', index=False, encoding='utf-8')

#print(cleanAddress(name, a1,a2,c,s,z))

#usaddress.parse(a1 + ', ' + c + ', ' + s + ', ' + z)

