import os
import errno
import json

import requests
import time
import datetime
import math
import pandas as pd
import numpy as np
from simple_salesforce import Salesforce
import redis
from tqdm import tqdm
#from abort import HonorableError

import important

def make_path(path):
    try:
        os.makedirs(path)
    except OSError as exception:
        if exception.errno != errno.EEXIST:
            raise


class DerpCounter(object):
    def __init__(self, counter=0):
        self._counter = counter

    def reset(self, counter=0):
        self._counter = counter

    def inc(self):
        self._counter += 1

    @property
    def counter(self):
        return self._counter


class DummyRedis:
    def __init__(self):
        pass

    def set(self, *args, **kwargs):
        pass


counter = DerpCounter()


def instantiate_redis(bypass=True):
    # Instantiates new RedisClient; talks to web server for frontend
    if not bypass:
        RedisClient = redis.StrictRedis(host='localhost', port=6379, db=0)

    else:
        print("Can't start the redis instance")
        RedisClient = DummyRedis()

    return RedisClient


def accessContact2(contactVid, counter=DerpCounter(), basepath=None, flush=False):
    print('Running accessContact2')
    try:
        url = "https://api.hubapi.com/contacts/v1/contact/vid/" + \
            contactVid + "/profile?hapikey="+important.HSKY
        response = cachedGetter(url, basepath, flush)
    except ValueError:
        counter.counter.inc()
        time.sleep(3)
        if counter.counter >= 5:
            response = dict({"form-submissions": []})
        else:
            a, response = accessContact2(contactVid)
    return response["form-submissions"], response


def accessCompany2(companyVid, counter=DerpCounter(), basepath=None, flush=False):
    # print('Running accessCompany2')
    counter.reset()
    try:
        companyVidURL = "https://api.hubapi.com/companies/v2/companies/" + \
            companyVid + "/contacts?hapikey="+important.HSKY
        # requests.get(companyVidURL).json()
        response = cachedGetter(companyVidURL, basepath, flush)
    except ValueError:
        counter.inc()
        time.sleep(3)
        if counter.counter >= 5:
            response = dict({"contacts": [{"formSubmissions": []}]})
        else:
            result, companies = accessCompany2(companyVid)
            response = dict({"contacts": [{"formSubmissions": []}]})
    companies = response["contacts"]
    result = [items["formSubmissions"] for items in companies]
    return result, companies


def access_contact(contactVid, counter=DerpCounter(), basepath=None, flush=False):
    counter.reset()
    try:
        contactVidURL = "https://api.hubapi.com/contacts/v1/contact/vid/" + contactVid + \
            "/profile?hapikey="+important.HSKY+"&formSubmissionMode=all"
        # requests.get(companyVidURL).json()
        response = cachedGetter(contactVidURL, basepath, flush)
    except ValueError:
        counter.inc()
        time.sleep(3)
        if counter.counter >= 5:
            response = {}
        else:
            result = access_contact(contactVid)
            response = {}
    return response


def get_contact_form_timeline(contact, compNum, timestamps, timelineUnix):
    formTimeline = []
    firstname = contact['properties']['firstname']['value']
    lastname = contact['properties']['lastname']['value']
    try:
        country = contact['properties']['country']['value']
    except:
        country = 'Not Entered'
    if country == 'United States':
        try:
            state = contact['properties']['state']['value']
        except:
            state = 'Not Entered'
    else:
        state = ''
    for form in contact['form-submissions']:
        if (timestamps[compNum]-timelineUnix) < form['timestamp'] < timestamps[compNum]:
            formTimeline.append([form['title'], firstname+' '+lastname, form['timestamp'],country,state])
    return formTimeline


def timestamp_to_date(timestamp):
    date = datetime.datetime.fromtimestamp(
        int(timestamp/1000)).strftime('%Y-%m-%d')
    return date


def convert_marketing_stream_to_string(stream):
    timeline = "("
    for formNum, form in enumerate(stream):
        timeline += form[0]+", "+form[1]+", " + \
            str(timestamp_to_date(form[2]))
        if formNum+1 == len(stream):
            timeline += ")"
        else:
            timeline += " "+unichr(11157)+" "
    return timeline


def nameCompany(companyVid, counter=0, basepath=None, flush=False,
                verbose=False):
    if verbose:
        print('Running nameCompany')
    response = None
    result = ["empty"]  # junk start state for recursive formatting
    try:
        companyVidURL = "https://api.hubapi.com/companies/v2/companies/" + \
            str(companyVid).strip("[]") + \
            "?hapikey="+important.HSKY
        # requests.get(companyVidURL).json()
        response = cachedGetter(companyVidURL, basepath, flush)
    except ValueError:
        counter += 1
        time.sleep(3)
        if counter >= 5:
            result = []
        else:
            result = nameCompany(companyVid, counter)
    if response.has_key("properties") and result != []:
        result = response["properties"]["name"]["value"]
    else:
        result = []
    return result


class CachingGetter(object):
    default_cachepath = './cache/'

    def __init__(self, basepath=None):
        basepath = basepath if basepath is not None else CachingGetter.default_cachepath
        self.basepath = basepath
        make_path(basepath)

    @staticmethod
    def json_fetch_and_cache(cachePath, url):
        dealCall = requests.get(url)
        dealCall = dealCall.json()
        with open(cachePath, 'w') as f:
            json.dump(dealCall, f)
        return dealCall

    @staticmethod
    def sf_fetch_and_cache(cachePath, sf, query):
        odict = sf.query(query)
        with open(cachePath, 'w') as f:
            json.dump(odict, f)
        return odict

    def cachedGetter(self, url, flush=False):

        cachePath = "{}/{}.json".format(self.basepath, abs(hash(url)))

        if flush or not os.path.exists(cachePath):
            dealCall = CachingGetter.json_fetch_and_cache(cachePath, url)

        else:
            with open(cachePath, 'r') as f:
                try:
                    dealCall = json.load(f)
                except ValueError:
                    dealCall = CachingGetter.json_fetch_and_cache(
                        cachePath, url)

        return dealCall

    def sf_cachedGetter(self, sf, query, flush=False):

        cachePath = "{}/{}.json".format(self.basepath, abs(hash(query)))
        data = None
        if os.path.exists(cachePath) and not flush:
            with open(cachePath, 'r') as f:
                try:
                    data = json.load(f)
                except ValueError:
                    pass

        if data is None:
            data = CachingGetter.sf_fetch_and_cache(cachePath, sf, query)

        return data


def cachedGetter(url, basepath='./cache/', flush=False):
    cg = CachingGetter(basepath)
    return cg.cachedGetter(url, flush=flush)


def sf_cachedGetter(sf, query, basepath='./cache/', flush=False):
    cg = CachingGetter(basepath)
    return cg.sf_cachedGetter(sf, query, flush=flush)


class DealGetter(object):
    hapikey = important.HSKY

    def __init__(self, basepath='./cache/'):
        self.basepath = basepath
        make_path(basepath)

    def all_dealGetter(self, offset, flush=False):
        allDealsURL = "https://api.hubapi.com/deals/v1/deal/paged?" \
            "hapikey={}&includeAssociations=true&properties=dealname&limit=250&offset={}".format(
                self.hapikey, offset)

        return cachedGetter(allDealsURL, self.basepath, flush=flush)

    def dealGetter(self, increment, offset, since, flush=False):
        recentDealsURL = "https://api.hubapi.com/deals/v1/deal/recent/modified?" \
                         "hapikey={}&count={}&offset={}&since={}".format(
                             self.hapikey, increment, offset, since)

        return cachedGetter(recentDealsURL, self.basepath, flush=flush)

    def get_all_deals(self, startUnix=None, endUnix=None, flush=False):
        startUnix = startUnix if startUnix is not None else int(
            time.time() * 1000) - 30 * 24 * 3600 * 1000
        endUnix = endUnix if endUnix is not None else int(
            time.time() * 1000) * 24 * 3600 * 1000
        offset = 0
        dealName, company, contacts, timestamps = [], [], [], []

        # StartUnix Lookup -> dealLowerBound & dealUpperBound
        # dealLowerBound = 240000000 # start 2018
        dealLowerBound = 400000000  # start 2019
        # dealLowerBound = 1300000000 # start 2020

        # dealUpperBound = 400000000
        dealUpperBound = 1300000000
        # dealLowerBound = 1400000000 # end 2020

        # Need to sort deals at the least by timestamp

        #         for _ in tqdm(range(dealLowerBound, dealUpperBound)):

        # Set Progress Bar
        #         progressBar = tqdm(total=100)

        # Check to make sure dealCall percentage updates progressbar correctly
        #         def update_progressbar():
        #             if offset >= nextOffsetPercent:
        #                 pbar.update(1)
        #                 nextOffsetPercent += dealCallPercentile
        #                 update_progressbar()

        #         dealCallPercentile = (dealUpperBound-dealLowerBound)/100

    #     dg = DealGetter()
        offset = dealLowerBound
        while offset < dealUpperBound:
            dealCall = self.all_dealGetter(offset)
            nextDeals = dealCall.get("deals")  # get the deal information
            offset = dealCall.get("offset")
            for i, deals in enumerate(nextDeals):  # loop through each deal
                if deals["properties"].has_key("dealname"):
                    if deals["properties"]["dealname"]["timestamp"] >= startUnix and deals["properties"]["dealname"]["timestamp"] <= endUnix:
                        dealName.append(
                            deals["properties"]["dealname"]["value"])
                        timestamps.append(
                            deals["properties"]["dealname"]["timestamp"])
                        company.append(deals["associations"]
                                       ["associatedCompanyIds"])
                        contacts.append(
                            deals["associations"]["associatedVids"])
                        # If this timestamp is less than the last timestamp, then say so
                        # Log the latest timestamp
        return dealName, company, contacts, timestamps

        # update_progressbar(pbar, offset, dealCallPercentile)
#         pbar.close()

        # TODO:
        #  - Make sure progress bar update works in a function
        #  - Set bounds and create lookup table (for 2018)
        #  - Finalize and test

    def getDeals2(self, startUnix=None, increment=100, flushCache=False):  # feed in timeRange
        startUnix = startUnix if startUnix is not None else int(
            time.time() * 1000) - 30 * 24 * 3600 * 1000
        since = startUnix  # The start of the range to look at in Unix time
        offset = 0  # start at the first deal

        # get the most recent deals using the time range

        dealCall = self.dealGetter(10, offset, since, flush=True)

        dealTotal = dealCall["total"]  # from the data find the number of deals
        # Take this out in final release
        print("Total number of deals: " + str(dealTotal))

        dealName, oidList, amount, company, contacts = [], [
        ], [], [], []  # elements to be extracted from deals
        fullDeal = []

        # while offset < dealTotal:  # loop through all the recent deals and get their attributes
        for offset in tqdm(range(0, dealTotal, increment)):
            dealCall = self.dealGetter(
                increment, offset, since, flush=flushCache)
            nextDeals = dealCall.get("results")  # get the deal information
            # get the number of deals been through so far
            lastVid = dealCall.get("offset")
            for i, deals in enumerate(nextDeals):  # loop through each deal
                # all the deal call information so it won't have to be gotten again
                fullDeal.append(deals.items())
                if deals["properties"].has_key("amount"):
                    dealName.append(deals["properties"]["dealname"]["value"])
                    amount.append(deals["properties"]["amount"]["value"])
                    # closeDate.append(deals["properties"]["closedate"]["value"])
                    company.append(deals["associations"]
                                   ["associatedCompanyIds"])
                    contacts.append(deals["associations"]["associatedVids"])
                    oidList.append(
                        [deals["properties"]["hs_salesforceopportunityid"]["value"]])
                    # dealId.append(deals["dealId"])

                    # offset = lastVid
        return dealName, company, contacts, fullDeal, oidList, amount


def salesAttribution(sf, num, requestName, timestamp, attribUnix, duplicate=0, flush=False):
    # requestName = deals[dealNum] # set the deal name as the name to be used in the API request
    if requestName.find("'") > 0:  # If there is an apostraphe....
        index = requestName.find("'")  # ...find it...
        requestName = requestName[:index] + '\\' + requestName[
            index:]  # ...and put a \ infront of it so it can be in the call
    query = "SELECT Name,Id,AccountId,CreatedDate,IsWon,IsClosed,Amount,Contact__c, \
    OwnerId,StageName,CloseDate,Product_line__c,LeadSource,Type,Quote_Number__c, \
    Original_Quote_Number__c FROM Opportunity WHERE Name = '" + \
        requestName + "'"  # Get opportunity IsWon,IsClosed,CreatedDate,AccountId
    opList = sf_cachedGetter(sf, query, flush=flush)  # sf.query(query)

    if opList["records"] != []:
        #print("duplicate: "+str(duplicate)+", opList[records]: "+str(len(opList["records"])))
        if duplicate >= len(opList["records"]):
            #print("Error for Deal: "+str(requestName)+", duplicate: "+str(duplicate)+", records: "+str(len(opList["records"])))
            #while duplicate >= len(opList["records"]):
            #    duplicate -= 1
            #print("Error for: "+str(num) +" "+str(requestName)+", Breaking...")
            return
        query2 = "SELECT Name,Territory__c,Industry,BillingCity,BillingState,BillingCountry \
             FROM Account WHERE Id = '" + \
            opList["records"][duplicate]["AccountId"] + \
            "'"  # get Opportunity information from Account Object
        accList = sf_cachedGetter(sf, query2, flush=flush)  # sf.query(query)
        opList["records"][duplicate]["Deal"] = opList["records"][duplicate]["Name"]

        # Account for missing location fields and Countries without States
        if accList["records"][0]["BillingCity"] is None:
            accList["records"][0]["BillingCity"] = "Misc"
        if accList["records"][0]["BillingState"] is None:
            accList["records"][0]["BillingState"] = "Misc"

        # Assign Continent field to Opportunity data
        # Note: got a NoneType error on the following line
        if accList["records"][0]["Territory__c"] is None:
            accList["records"][0]["Cont"] = "ROW"
        elif "US" in accList["records"][0]["Territory__c"] or "CAN" in accList["records"][0]["Territory__c"]:
            accList["records"][0]["Cont"] = "NA"
        elif "Eur" in accList["records"][0]["Territory__c"]:
            accList["records"][0]["Cont"] = "EU"
        elif "Asia" in accList["records"][0]["Territory__c"]:
            accList["records"][0]["Cont"] = "APAC"
        else:
            accList["records"][0]["Cont"] = "ROW"

        # concatenate Account information with Opportunity information # AccountId is unique, so only one record will ever be needed
        opList["records"][duplicate].update(accList["records"][0])
        opInfo = opList["records"][duplicate]

        if opInfo["OwnerId"] != None:
            query3 = "SELECT Name FROM User WHERE Id = '" + \
                opInfo["OwnerId"] + \
                "'"  # get the BD person for this opportunity
            resp3 = sf_cachedGetter(
                sf, query3, flush=flush)  # sf.query(query)

            # concatenate User/Owner information as BD_person
            opInfo["OwnerId"] = resp3["records"][0]["Name"]
        else:
            opInfo["OwnerId"] = "Misc"

        if opInfo["Contact__c"] != None:
            customerContactQuery = "SELECT Name,MailingCountry,MailingState FROM Contact WHERE Id = '" + \
                opInfo["Contact__c"]+"'"
            customerContact = sf_cachedGetter(
                sf, customerContactQuery, flush=flush)  # sf.query(query)

            opInfo["Contact__c"] = customerContact["records"][0]["Name"]
            opInfo["MailingCountry"] = customerContact["records"][0]["MailingCountry"]
            opInfo["MailingState"] = customerContact["records"][0]["MailingState"]
        else:
            opInfo["Contact__c"] = "Misc"

        if opInfo["Type"] == 'Amendment':
            dealLifeQuery = "SELECT Amount FROM Opportunity WHERE Name LIKE '" \
                +opInfo['Original_Quote_Number__c']+"'"
            dealLife = sf_cachedGetter(sf, dealLifeQuery, flush=flush)
            dealLifetimeAmount = 0
            for dealInstance in dealLife["records"]:
                dealLifetimeAmount += int(dealInstance['Amount'])
            opInfo['Amount'] = str(dealLifetimeAmount)
            
        createdUnix = time.mktime(
            datetime.datetime.strptime(str(opList["records"][0]["CreatedDate"]).strip("[u']"),
                                       "%Y-%m-%dT%H:%M:%S.%f+0000").timetuple()) * 1000  # change created Date to unix time

        endtime = timestamp + attribUnix

        if timestamp < createdUnix <= endtime:
            opInfo.update({"inRange": True, "dealNum": num})
        else:
            opInfo.update({"inRange": False, "dealNum": num})
    else:
        opInfo = None

    return opInfo


def processNums(nums, compNum, timestamps, timelineUnix):
    if (timestamps[compNum]-timelineUnix) < nums['timestamp'] < timestamps[compNum]:
        # If form submissions are in the time scope specified...
        # ...put them in as legitimate forms
        return [compNum, nums.get("formTitle", 'no_formTitle'), nums["timestamp"]]
    return None


def processDigits(digits, compNum, timestamps, timelineUnix):
    if isinstance(digits, dict):
        digits = [digits]
    data = [processNums(nums, compNum, timestamps, timelineUnix)
            for nums in digits]
    data = [x for x in data if x is not None]
    return data


def processTempform(tempform, compNum, startUnix, endUnix):
    data = [processDigits(digits, compNum, startUnix, endUnix)
            for digits in tempform]
    data = [x for x in data if x is not None]
    return data


def make_report_activity_paths(activity, act_dictionary=None):
    if act_dictionary is None:
        act_dictionary = {}
    try:
        if act_dictionary.has_key(activity):
            response = act_dictionary[str(activity)]
        else:
            response = ["Unlabeled"]
            act_dictionary.update({activity: ['Unlabeled']})
        return response
    except:
        # send exception to the webpage
        print('Error occurred reading an activity again the config file')


def create_column_lists(Tree, debug=False):
    df = pd.DataFrame()
    try:
        add_space = len(Tree[0]) - 1
        column = [ele[0] for ele in Tree[1:]]
        for count, num in enumerate(column):
            if num == 'Unlabeled':
                for digit in range(add_space):
                    Tree[count+1].append('')
        for count, el in enumerate(Tree[0]):
            if debug:
                print(str(count)+' '+str(el)+'\n'+str(column))
            df[Tree[0][count]] = [ele[count] for ele in Tree[1:]]
    except IndexError:
         raise Exception(
             'A list of the wrong length detected for activity '+str(count+1))
    return df


class NirvanaManager(object):
    def __init__(self):
        self.RedisClient = instantiate_redis(bypass=False)
        self.counter = 0

    def app2(self, startDate=None, endDate=None, attribDays=30, eventDays=45, basepath='./cache/', flush=False, debug=False, act_dictionary=None):
        startDate = startDate if startDate is not None else int(
            time.time() * 1000) - 30 * 24 * 3600 * 1000
        endDate = endDate if endDate is not None else int(time.time() * 1000)
        print('Starting App2 Instance, {} {} {} {}'.format(
            startDate, endDate, attribDays, eventDays))

        timeline = attribDays
        
        startUnix = time.mktime(datetime.datetime.strptime(
            str(startDate), "%Y-%m-%d").timetuple()) * 1000
        endUnix = time.mktime(datetime.datetime.strptime(
            str(endDate), "%Y-%m-%d").timetuple()) * 1000

        # Change attribDays to attribUnix
        attribUnix = int(attribDays) * 24 * 3600 * 1000
        # Change eventDays to eventUnix
        eventUnix = int(eventDays) * 24 * 3600 * 1000

        timelineUnix = int(timeline) * 24 * 3600 * 1000

        # Necessary lists:
        formSubinRange, formSubsAll = [], []  # store form submissions in range
        dealName, companies, contacts, fullDeal = [], [], [], []  # store deal information
        formLog, formLast = [], []  # store all form submission call data
        opInfo, opP = [], []  # store opportunity call information

        def activityCategorize2(activity):
            out = ""
            try:
                if str(activity).lower().find("poster") >= 0 or str(activity).lower().find("white") >= 0 or str(
                    activity).lower().find("publication") >= 0 or str(activity).lower().find(
                        "application") >= 0 or str(
                        activity).lower().find("ppc") >= 0 or str(activity).lower().find("contact") >= 0 or str(
                        activity).lower().find("odw") >= 0 or str(activity).lower().find("webinar") >= 0:
                    out = "Website Enquiry"
                elif str(activity).lower().find("symposium") >= 0:
                    out = "Symposium"
                elif str(activity).lower().find("registration") >= 0:
                    out = "Registration"
                elif str(activity).lower().find("request") >= 0:
                    out = "Request"
            except UnicodeEncodeError:
                pass
            return out

        print("Getting deals...")
        self.RedisClient.set('Status', '{ status: "Getting deals..." }')
        dg = DealGetter()
        dealName, companies, contacts, timestamps = dg.get_all_deals(
            int(startUnix), int(endUnix))

        print("Getting form submissions...")
        self.RedisClient.set(
            'Status', '{ status: "Getting form submissions..." }')
        timelineStore = {}
        timelineFormNums = []
        for compNum, comp in enumerate(tqdm(companies)):
            touchpoints = []
            if comp != []:  # If there is a company number...
                tempforms, templog = accessCompany2(
                    str(comp).strip("[]"))  # ...get the form submissions from the company
                for contactNum, digits in enumerate(tempforms):
                    if digits != []:  # if the form isn't empty...
                        formSubsAll += processDigits(digits,
                                                     compNum, timestamps, timelineUnix)
                        if (timestamps[compNum]-timelineUnix) < digits[0]['timestamp'] < timestamps[compNum]:
                            contactVid = templog[contactNum]['vid']
                            contact = access_contact(str(contactVid))
                            touchpoints += get_contact_form_timeline(
                                contact, compNum, timestamps, timelineUnix)
                timelineStore[str(compNum)] = touchpoints
                timelineFormNums.append(len(touchpoints))
                # Store the output of the form for later use anyway
                formLog.append(templog)
            elif comp == [] and contacts[
                    compNum] != []:  # if there isn't a company number, but there is/are contact number(s)
                tempform, templog = accessContact2(
                    str(contacts[compNum]).strip("[]"))
                for contactNum, digits in enumerate(tempform):
                    if digits != []:  # if it isn't empty...
                        formSubsAll += processDigits(digits,
                                                     compNum, timestamps, timelineUnix)
                        if (timestamps[compNum]-timelineUnix) < digits[0]['timestamp'] < timestamps[compNum]:
                            contactVid = templog[contactNum]['vid']
                            contact = access_contact(str(contactVid))
                            touchpoints += get_contact_form_timeline(
                                contact, compNum, timestamps, timelineUnix)
                timelineStore[str(compNum)] = touchpoints
                formLog.append(templog)  # store the form output
            else:
                formSubsAll.append([])
                formLog.append([])

        sf = Salesforce(important.ACCT, important.PS, important.KY)
        print("Getting Opportunity info...")
        self.RedisClient.set(
            'Status', '{ status: "Getting Opportunity info..." }')
        for d, form in enumerate(tqdm(formSubsAll)):
            if form != [""] and form != [] and form != '':
                try:
                    if str(form[1]).find("*") >= 0:
                        timestamp = form[2]
                        opP.append(salesAttribution(
                            sf, form[0], dealName[form[0]], timestamp, attribUnix))
                    else:
                        timestamp = form[2]
                        opP.append(salesAttribution(
                            sf, form[0], dealName[form[0]], timestamp, attribUnix))
                except UnicodeEncodeError:
                    timestamp = form[2]
                    opP.append(salesAttribution(
                        sf, form[0], dealName[form[0]], timestamp, attribUnix))
            else:
                opP.append({"inRange": False, "Cont": None})

        aa = [[item, count] for count, item in enumerate(formSubsAll) if len(
            item) > 1 and opP[count] is not None and opP[count]["inRange"]]
        formLast = []
        fs = []
        opInfo = []
        for e, form in enumerate(aa):
            if len(form) > 1:
                if len(formLast) > 1:
                    if form[0][0] == formLast[0][0] and form[0][2] >= formLast[0][2]:
                        fs.pop()
                        opInfo.pop()
                fs.append(form[0])
                opInfo.append(opP[form[1]])
                if len(formLast) > 1:
                    if len(form) > 1 and form[0][0] == formLast[0][0] and form[0][2] <= formLast[0][2]:
                        fs.pop()
                        opInfo.pop()
                formLast = form

        #Requery any duplicates
        print("Requerying duplicate deal names...")
        self.RedisClient.set(
            'Status', '{ status: "Requerying duplicate deal names..." }')

        opReg = []
        duplicateDeals = []
        for c, item in enumerate(opInfo):
            duplicate = 0
            if item["Deal"] in opReg:
                duplicate = opReg.count(item["Deal"])
                if item["Deal"] not in duplicateDeals:
                    duplicateDeals += [item["Deal"]]
                opInfo[c] = salesAttribution(
                    sf, fs[c][0], dealName[fs[c][0]], fs[c][2], attribUnix, duplicate=duplicate)
            opReg.append(item["Deal"])

        dupleReg = range(len(duplicateDeals))
        for c, item in enumerate(duplicateDeals):
            dupleReg[c] = []
            query = "SELECT Name,Id,AccountId,CreatedDate,IsWon,IsClosed,Amount,Contact__c, \
                OwnerId,StageName,CloseDate,Product_line__c,LeadSource,Type,Quote_Number__c, \
                    Original_Quote_Number__c FROM Opportunity WHERE Name = '" + \
                item + "'"  # Get opportunity IsWon,IsClosed,CreatedDate,AccountId
            resp1 = sf_cachedGetter(sf, query)  # sf.query(query)
            for record in resp1["records"]:
                query2 = "SELECT Name,Territory__c,Industry,BillingCity,BillingState,BillingCountry \
                     FROM Account WHERE Id = '" + record["AccountId"] + \
                    "'"  # get Opportunity information from Account Object
                resp2 = sf_cachedGetter(sf, query2)  # sf.query(query)
                record["Deal"] = record["Name"]

                # Account for missing location fields and Countries without States
                if resp2["records"][0]["BillingCity"] is None:
                    resp2["records"][0]["BillingCity"] = "Misc"
                if resp2["records"][0]["BillingState"] is None:
                    resp2["records"][0]["BillingState"] = "Misc"

                # Assign Continent field to Opportunity data
                # Note: got a NoneType error on the following line
                if resp2["records"][0]["Territory__c"] is None:
                    resp2["records"][0]["Cont"] = "ROW"
                elif "US" in resp2["records"][0]["Territory__c"] or "CAN" in resp2["records"][0]["Territory__c"]:
                    resp2["records"][0]["Cont"] = "NA"
                elif "Eur" in resp2["records"][0]["Territory__c"]:
                    resp2["records"][0]["Cont"] = "EU"
                elif "Asia" in resp2["records"][0]["Territory__c"]:
                    resp2["records"][0]["Cont"] = "APAC"
                else:
                    resp2["records"][0]["Cont"] = "ROW"

                record.update(resp2["records"][0])

                if record["OwnerId"] != None:
                    query3 = "SELECT Name FROM User WHERE Id = '" + \
                        record["OwnerId"] + \
                        "'"  # get the BD person for this opportunity
                    resp3 = sf_cachedGetter(
                        sf, query3, flush=flush)  # sf.query(query)

                    # concatenate User/Owner information as BD_person
                    record["OwnerId"] = resp3["records"][0]["Name"]
                else:
                    record["OwnerId"] = "Misc"

                if record["Contact__c"] != None:
                    customerContactQuery = "SELECT Name,MailingCountry,MailingState FROM Contact WHERE Id = '" + \
                        record["Contact__c"]+"'"
                    customerContact = sf_cachedGetter(
                        sf, customerContactQuery, flush=flush)  # sf.query(query)

                    record["Contact__c"] = customerContact["records"][0]["Name"]
                    record["MailingCountry"] = customerContact["records"][0]["MailingCountry"]
                    record["MailingState"] = customerContact["records"][0]["MailingState"]
                else:
                    record["Contact__c"] = "Misc"

                dupleReg[c].append(record)

        for d, duple in enumerate(duplicateDeals):
            index = [c for c, item in enumerate(
            opInfo) if "Deal" in opInfo and item["Deal"].find(str(duple)) >= 0]
            companyName = [nameCompany(companies[fs[item][0]]) for item in index]
            for c, item in enumerate(companyName):
                for e, elem in enumerate(dupleReg[d]):
                    if item == elem["Name"] and datetime.datetime.fromtimestamp(int(fs[index[c]][2]/1000)).strftime('%Y-%m-%d %H:%M:%S') <= elem["CreatedDate"]:
                        elem["inRange"] = True
                        opInfo[index[c]] = dupleReg[d].pop(e)
                        break
                    else:
                        opInfo[index[c]]["inRange"] = False

        def filterTimelineStoreOnOpportunityLocation(fs, opInfo, timelineStore):
            for opportunity in opInfo:
                try:
                    countryLocation = opportunity["MailingCountry"]
                except:
                    continue
                if countryLocation == 'United States':
                    try:
                        countryState = opportunity["MailingState"]
                    except:
                        countryState = 'Not Entered'
                else:
                    countryState = ''
                for activityCount, formActivity in enumerate(timelineStore[str(
                    opportunity["dealNum"])]):
                    if countryLocation == 'UnitedStates':
                        if formActivity[4] != countryState:
                            timelineStore[str(opportunity["dealNum"])].pop(activityCount)
                    else:
                        if formActivity[3] != countryLocation:
                            timelineStore[str(opportunity["dealNum"])].pop(activityCount)

        def _generate_list_of_lists(indicies):
            listOfLists = []
            for _ in range(indicies):
                listOfLists.append([])
            return listOfLists
        
        # filterTimelineStoreOnOpportunityLocation(fs, opInfo, timelineStore)

        # timelineFormNums = [len(timeline) for timeline in timelineStore.values()]

        print("Creating report...")
        self.RedisClient.set('Status', '{ status: "Creating report..." }')

        activity = []  # add the values for each deal with the same activity, using the revenue value from Hubspot, not Salesforce
        companyNames = []
        deals = []
        activityVal = []
        activityCnt = []  # store the lead counts for each activity
        activityDate = []  # store the created date of the marketing activity
        activityWinNum = []  # Number of won deals
        activityOpenNum = []  # Number of open deals
        activityWinVal = []  # Number of won deals
        activityOpenVal = []  # Number of open deals

        # Fields for sorting
        contact = []
        direct_buyer = []
        deal_stage = []
        createDate = []
        closeDate = []
        productLine = []
        leadSource = []
        territory = []
        continent = []
        billingCity = []
        billingState = []
        billingCountry = []
        opportunityType = []
        quoteNumber = []
        originalQuoteNumber = []
        generatedColumns = _generate_list_of_lists(max(timelineFormNums)*5)
        lastFormSubColumns = [[],[],[],[],[]]
        marketingStream = []

        # Sanity Checks
        activity_timestamp = []
        opportunity_id = []

        def fill_in_report_columns(sortedTimelines, generatedColumns, lastFormSubColumns):
            for formSubsNum, marketingEvent in enumerate(sortedTimelines):
                if formSubsNum+1 == len(sortedTimelines):
                    lastFormSubColumns[0].append(marketingEvent[0])
                    lastFormSubColumns[1].append(marketingEvent[1])
                    lastFormSubColumns[2].append(datetime.datetime.fromtimestamp(
                        int(marketingEvent[2]/1000)).strftime('%Y-%m-%d'))
                    lastFormSubColumns[3].append(marketingEvent[3])
                    lastFormSubColumns[4].append(marketingEvent[4])
                    if len(sortedTimelines)*5 < len(generatedColumns):
                        for blankColumn in range(len(generatedColumns)-len(sortedTimelines)*5):
                            generatedColumns[-(blankColumn+1)].append('')
                generatedColumns[5*formSubsNum].append(marketingEvent[0])
                generatedColumns[5*formSubsNum+1].append(marketingEvent[1])
                generatedColumns[5*formSubsNum+2].append(datetime.datetime.fromtimestamp(
                    int(marketingEvent[2]/1000)).strftime('%Y-%m-%d'))
                generatedColumns[5*formSubsNum+3].append(marketingEvent[3])
                generatedColumns[5*formSubsNum+4].append(marketingEvent[4])
                
            return generatedColumns, lastFormSubColumns

        for count, dL in enumerate(fs):
            if len(dL) > 1 and opInfo[count] != "":
                if opInfo[count] and opInfo[count]["inRange"] is True:
                    activity.append(dL[1])
                    companyNames.append(opInfo[count]["Name"])
                    deals.append(opInfo[count]["Deal"])
                    contact.append(opInfo[count]["Contact__c"])
                    direct_buyer.append(opInfo[count]["OwnerId"])
                    deal_stage.append(opInfo[count]["StageName"])
                    createDate.append(datetime.datetime.strptime(str(
                        opInfo[count]["CreatedDate"]),  "%Y-%m-%dT%H:%M:%S.000+0000").strftime('%Y-%m-%d'))
                    closeDate.append(opInfo[count]["CloseDate"])
                    productLine.append(opInfo[count]["Product_line__c"])
                    leadSource.append(opInfo[count]["LeadSource"])
                    territory.append(opInfo[count]["Territory__c"])
                    continent.append(opInfo[count]["Cont"])
                    billingCity.append(opInfo[count]["BillingCity"])
                    billingState.append(opInfo[count]["BillingState"])
                    billingCountry.append(opInfo[count]["BillingCountry"])
                    opportunity_id.append(opInfo[count]["Id"])
                    opportunityType.append(opInfo[count]["Type"])
                    quoteNumber.append(opInfo[count]["Quote_Number__c"])
                    originalQuoteNumber.append(opInfo[count]["Original_Quote_Number__c"])
                    activity_timestamp.append(datetime.datetime.fromtimestamp(
                        int(dL[2]/1000)).strftime('%Y-%m-%d'))

                    generatedColumns, lastFormSubColumns = fill_in_report_columns(sorted(
                        timelineStore[str(dL[0])], key=lambda tstamp: tstamp[2]),
                        generatedColumns,lastFormSubColumns)
                        
                    if opInfo[count]["Amount"] is not None:
                        activityVal.append(float(opInfo[count]["Amount"]))
                    else:
                        activityVal.append(0)
                    activityCnt.append(1)
                    if opInfo[count]["IsWon"] is True:
                        activityWinNum.append(1)
                        if opInfo[count]["Amount"] is not None:
                            activityWinVal.append(
                                float(opInfo[count]["Amount"]))
                        else:
                            activityWinVal.append(0)
                    else:
                        activityWinNum.append(0)
                        activityWinVal.append(0)
                    if opInfo[count]["IsClosed"] is False:
                        activityOpenNum.append(1)
                        if opInfo[count]["Amount"] is not None:
                            activityOpenVal.append(
                                float(opInfo[count]["Amount"]))
                        else:
                            activityOpenVal.append(0)
                    else:
                        activityOpenNum.append(0)
                        activityOpenVal.append(0)

        actCost = [""] * len(activity)
        activityLosNum, activityLosVal = [], []
        for c, item in enumerate(activity):
            if activityWinNum[c] == 0 and activityOpenNum[c] == 0:
                activityLosNum.append(1)
                activityLosVal.append(activityVal[c])
            else:
                activityLosNum.append(0)
                activityLosVal.append(0)

        CPO = actCost
        CPWO = actCost
        CPLO = actCost

        # Take in the config file for each activity and label each Unlabeled activity
        Tree = [make_report_activity_paths(
            d, act_dictionary) for d in activity]
        if act_dictionary:
            Tree.insert(0, act_dictionary["headers"])

            # Create the dataframe with it's initial columns being created from the config file
            df = create_column_lists(Tree)
        else:
            df = pd.DataFrame()

        def generate_report_headers(extraColumnNumber):
            headers = ['Activity', 'Company', 'Deals', 'Activity Cost',
                        'Activity Revenue', 'Opportunity Count',
                        'Closed-Won Count', 'Closed-Won Value', 
                        'Closed-Lost Count', 'Closed-Lost Value', 
                        'Open Count','Open Value', 'CPO', 'CPWO', 'CPLO',
                       'Activity Date', 'Op Created Date', 'Contact', 
                       'Direct Buyer','Deal Stage', 'Closed Date', 
                       'Product Line', 'Lead Source', 'Territory', 'Cont',
                       'City', 'State', 'Country','Unique Op Id',
                       'Opportunity Type','Original Quote Number','Quote Number',]
            extraColumns = ['First Form Submission',
                            'First Form Submission Contact', 
                            'First Form Submission Date', 
                            'First Form Submission Country', 
                            'First Form Submission State', 
                            'Last Form Submission', 
                            'Last Form Submission Contact', 
                            'Last Form Submission Date',
                            'Last Form Submission Country',
                            'Last Form Submission State',]
            for formSubNum in range(2,extraColumnNumber):
                newColumns = ['Form Submission '+str(formSubNum),
                              'Form Submission '+str(formSubNum)+' Contact',
                              'Form Submission '+str(formSubNum)+' Date',
                              'Form Submission '+str(formSubNum)+' Country',
                              'Form Submission '+str(formSubNum)+' State',]
                extraColumns += newColumns
            headers += extraColumns
            return headers
        
        def append_timeline_columns(data_values,generatedColumns,lastFormSubColumns):
        
            data_values.append(generatedColumns[0])
            data_values.append(generatedColumns[1])
            data_values.append(generatedColumns[2])
            data_values.append(generatedColumns[3])
            data_values.append(generatedColumns[4])
            data_values.append(lastFormSubColumns[0])
            data_values.append(lastFormSubColumns[1])
            data_values.append(lastFormSubColumns[2])
            data_values.append(lastFormSubColumns[3])
            data_values.append(lastFormSubColumns[4])
            for columnNum,column in enumerate(generatedColumns):
                if columnNum > 4 and columnNum < len(generatedColumns)-5:
                    data_values.append(column)
            
            return data_values
        
        data_fields = generate_report_headers(max(timelineFormNums))                       
        data_values = [activity, companyNames, deals, actCost, activityVal, activityCnt, activityWinNum,
                       activityWinVal, activityLosNum, activityLosVal, activityOpenNum,
                       activityOpenVal, CPO, CPWO, CPLO, activity_timestamp, createDate, contact, direct_buyer, deal_stage, closeDate,
                       productLine, leadSource, territory, continent, billingCity, billingState, billingCountry, opportunity_id,
                       opportunityType, originalQuoteNumber, quoteNumber,]
         
        data_values = append_timeline_columns(data_values,generatedColumns,lastFormSubColumns)

        # print(timelineFormNums)
        # return timelineFormNums
        # print(str(len(data_fields))+" "+str(len(data_values)))
        # return locals()
        for datnum, datavals in enumerate(data_fields):
            df[datavals] = data_values[datnum]

        # print("Done.")
        # self.RedisClient.set('Status', '{ status: "Done." }')
        # return df

        # Tradeshow Lists

        print("Getting List info...")
        self.RedisClient.set('Status', '{ status: "Getting List info..." }')
        offset = 0
        done = False
        listOlist = []
        while done is False:
            listsCall = "https://api.hubapi.com/contacts/v1/lists?hapikey="+important.HSKY+"&count=250"
            # response = requests.get(listsCall + "&offset=" + str(offset)).json()

            response = requests.get(listsCall+"&offset="+str(offset)).json()
            #response = cachedGetter(listsCall + "&offset=" + str(offset), basepath, flush)

            offset = response["offset"]
            # Store the list name and Id to access the contacts
            for item in response["lists"]:
                if item.has_key("description") and item["description"].find("/") >= 0:
                    eventDate = item["description"]
                    index = eventDate.find("*")
                    eventDate = eventDate[index+1:].strip(" ")
                    eventDate = time.mktime(datetime.datetime.strptime(
                        str(eventDate), "%m/%d/%Y").timetuple()) * 1000
                    if item["createdAt"] >= startUnix and item["createdAt"] <= endUnix:
                        listOlist.append(
                            [item["name"], item["listId"], eventDate])
            if response["has-more"] is False:
                done = True
                # Pull all the contacts from each list
        print(listOlist)
        print("Getting List Contacts...")
        self.RedisClient.set(
            'Status', '{ status: "Getting List Contacts..." }')
        listContacts = []
        for item in listOlist:
            vidOffset = 0
            hasMore = True
            while hasMore is True:
                listContactCall = "https://api.hubapi.com/contacts/v1/lists/" + str(
                    item[
                        1]) + "/contacts/all?hapikey="+important.HSKY+"&count=100" + "&vidOffset=" + str(
                    vidOffset)
                resp = cachedGetter(listContactCall, basepath, flush)
                # resp = requests.get(listContactCall).json()
                vidOffset = resp["vid-offset"]
                hasMore = resp["has-more"]
                for person in resp["contacts"]:
                    if person.has_key("properties"):
                        if person["properties"].has_key("firstname") and person["properties"].has_key("lastname"):
                            listContacts.append([item[0], person["properties"]["firstname"]["value"],
                                                 person["properties"]["lastname"]["value"], item[2]])

        # Find if there are any opportunities for those contacts
        print("Getting Salesforce Contact Ids...")
        self.RedisClient.set(
            'Status', '{ status: "Getting Salesforce Contact Ids..." }')
        sf = Salesforce(important.ACCT, important.PS, important.KY)
        opList, rrrList = [], []
        for item in listContacts:
            if item[1].find("'") > 0:  # If there is an apostraphe....
                index = item[1].find("'")  # ...find it...
                item[1] = item[1][:index] + '\\' + item[1][index:]
            if item[2].find("'") > 0:  # If there is an apostraphe....
                index = item[2].find("'")  # ...find it...
                item[2] = item[2][:index] + '\\' + item[2][index:]
            rrr = sf_cachedGetter(sf, "SELECT Name,Id FROM Contact WHERE Name = '" + item[1] + " " + item[2] + "'",
                                  basepath, flush)
            # rrr = sf.query("SELECT Name,Id FROM Contact WHERE Name = '" + item[1] + " " + item[2] + "'")
            rrrList.append(rrr)
            if rrr[
                    "records"] != []:  # Assumes you won't have two people in the same list with the same first and last name
                opList.append([item[0], rrr["records"][0]["Id"],
                               item[3]])  # ...if you did it'd be virtually impossible to distinguish

        print("Getting Contact Opportunity Information...")
        self.RedisClient.set(
            'Status', '{ status: "Getting Contact Opportunity Information..." }')
        opList2 = []
        for item in opList:  # Get opportunity information for each contact with an opportunity
            # if there was a non-empty value returned from the contact Id...
            if item[1] is not None:
                requestName = item[1]  # ...take the contact Id...
                if str(requestName).find(
                        "'") > 0:  # This if statement filters for apostraphe's which can ruin a request, but likely won't be in a Contact Id
                    index = requestName.find("'")  #
                    requestName = requestName[:index] + \
                        '\\' + requestName[index:]  #
                opCall = sf_cachedGetter(sf, "SELECT Name,Id,AccountId,CreatedDate,IsWon,IsClosed,Amount,Contact__c, \
                    OwnerId,StageName,CloseDate,Product_line__c,LeadSource,Type, \
                        Quote_Number__c,Original_Quote_Number__c FROM Opportunity \
                             WHERE Contact__c = '" + requestName + "'",
                                         basepath, flush)  # ...add use it to find any opportunities
                if opCall["records"] != []:
                    newList = []
                    for elemen in opCall["records"]:
                        newList = sf_cachedGetter(sf, "SELECT Name, Territory__c,Industry,BillingCity, \
                            BillingState,BillingCountry FROM Account WHERE Id = '" +
                                                  elemen["AccountId"] + "'", flush=flush)  # get Opportunity information from Account Object
                        elemen['Deal'] = elemen['Name']

                        # Account for missing location fields and Countries without States
                        if newList["records"][0]["BillingCity"] is None:
                            newList["records"][0]["BillingCity"] = "Misc"
                        if newList["records"][0]["BillingState"] is None:
                            newList["records"][0]["BillingState"] = "Misc"

                        # Assign Continent field to Opportunity data
                        # Note: got a NoneType error on the following line
                        if newList["records"][0]["Territory__c"] is None:
                            newList["records"][0]["Cont"] = "ROW"
                        elif "US" in newList["records"][0]["Territory__c"] or "CAN" in newList["records"][0]["Territory__c"]:
                            newList["records"][0]["Cont"] = "NA"
                        elif "Eur" in newList["records"][0]["Territory__c"]:
                            newList["records"][0]["Cont"] = "EU"
                        elif "Asia" in newList["records"][0]["Territory__c"]:
                            newList["records"][0]["Cont"] = "APAC"
                        else:
                            newList["records"][0]["Cont"] = "ROW"

                        elemen.update(newList["records"][0])

                        if elemen["OwnerId"] != None:
                            query3 = "SELECT Name FROM User WHERE Id = '" + \
                                elemen["OwnerId"] + \
                                "'"  # get the BD person for this opportunity
                            resp3 = sf_cachedGetter(
                                sf, query3, flush=flush)  # sf.query(query)

                            # concatenate User/Owner information as BD_person
                            elemen["OwnerId"] = resp3["records"][0]["Name"]
                        else:
                            elemen["OwnerId"] = "Misc"

                        if elemen["Contact__c"] != None:
                            customerContactQuery = "SELECT Name,MailingCountry,MailingState FROM Contact WHERE Id = '" + \
                                elemen["Contact__c"]+"'"
                            customerContact = sf_cachedGetter(
                                sf, customerContactQuery, flush=flush)  # sf.query(query)

                            elemen["Contact__c"] = customerContact["records"][0]["Name"]
                            elemen["MailingCountry"] = customerContact["records"][0]["MailingCountry"]
                            elemen["MailingState"] = customerContact["records"][0]["MailingState"]
                        else:
                            elemen["Contact__c"] = "Misc"

                        opList2.append([item[0], elemen, item[2]])

                # for part in opCall["records"]:
                #     opList2.append([item[0], part, item[2]])  # store findings in a new list

        print("Filtering Contacts By Attribution Timerange...")
        self.RedisClient.set(
            'Status', '{ status: "Filtering Contacts By Attribution Timerange..." }')
        opList3 = []
        for item in opList2:
            if item[1] is not None:  # check to make sure there was something returned
                # change Salesforce timetag to Hubspot timestamp
                createdUnix = time.mktime(datetime.datetime.strptime(str(item[1]["CreatedDate"]).strip("[u']"),
                                                                     "%Y-%m-%dT%H:%M:%S.%f+0000").timetuple()) * 1000  # change created Date to unix time
                # if Opportunities created date is within the attribution range, add it to the new list
                if createdUnix >= item[2] and createdUnix <= item[2] + attribUnix:
                    opList3.append(item)

        print("Creating List outputs...")
        self.RedisClient.set(
            'Status', '{ status: "Creating List outputs..." }')
        # Code Block to sort deals by unique activity type
        activity = []  # add the values for each deal with the same activity, using the revenue value from Hubspot, not Salesforce
        companyNames = []
        deals = []
        activityVal = []
        activityCnt = []  # store the lead counts for each activity
        activityDate = []  # store the created date of the marketing activity
        activityWinNum = []  # Number of won deals
        activityOpenNum = []  # Number of open deals
        activityWinVal = []  # Number of won deals
        activityOpenVal = []  # Number of open deals

        # Fields for sorting
        contact = []
        direct_buyer = []
        deal_stage = []
        createDate = []
        closeDate = []
        productLine = []
        leadSource = []
        territory = []
        continent = []
        billingCity = []
        billingState = []
        billingCountry = []
        opportunityType = []
        quoteNumber = []
        originalQuoteNumber = []
        timelineFillerColumns = _generate_list_of_lists(
            max(timelineFormNums)*5)

        # Sanity Checks
        activity_timestamp = []
        opportunity_id = []

        def fill_all_columns_with_none(timelineFillerColumns):
            
            for column in timelineFillerColumns:
                column.append('')
            
            return timelineFillerColumns

        for count, dL in enumerate(opList3):
            if len(dL) > 1:
                activity.append(dL[0])
                companyNames.append(dL[1]["Name"])
                deals.append(dL[1]["Deal"])
                contact.append(dL[1]["Contact__c"])
                direct_buyer.append(dL[1]["OwnerId"])
                deal_stage.append(dL[1]["StageName"])
                createDate.append(datetime.datetime.strptime(str(
                    dL[1]["CreatedDate"]),  "%Y-%m-%dT%H:%M:%S.000+0000").strftime('%Y-%m-%d'))
                closeDate.append(dL[1]["CloseDate"])
                productLine.append(dL[1]["Product_line__c"])
                leadSource.append(dL[1]["LeadSource"])
                territory.append(dL[1]["Territory__c"])
                continent.append(dL[1]["Cont"])
                billingCity.append(dL[1]["BillingCity"])
                billingState.append(dL[1]["BillingState"])
                billingCountry.append(dL[1]["BillingCountry"])
                opportunity_id.append(dL[1]["Id"])
                opportunityType.append(dL[1]["Type"])
                quoteNumber.append(dL[1]["Quote_Number__c"])
                originalQuoteNumber.append(dL[1]["Original_Quote_Number__c"])
                activity_timestamp.append(datetime.datetime.fromtimestamp(
                    int(dL[2]/1000)).strftime('%Y-%m-%d'))
                timelineFillerColumns = fill_all_columns_with_none(timelineFillerColumns)
                if dL[1]["Amount"] is not None:
                    activityVal.append(float(dL[1]["Amount"]))
                    #activityVal.update({dL[1]: float(opInfo[count]["Amount"])})  # else, start for a new activity
                else:
                    activityVal.append(0)
                activityCnt.append(1)
                if dL[1]["IsWon"] is True:
                    activityWinNum.append(1)
                    if dL[1]["Amount"] is not None:
                        activityWinVal.append(float(dL[1]["Amount"]))
                    else:
                        activityWinVal.append(0)
                else:
                    activityWinNum.append(0)
                    activityWinVal.append(0)
                if dL[1]["IsClosed"] is False:
                    activityOpenNum.append(1)
                    if dL[1]["Amount"] is not None:
                        activityOpenVal.append(float(dL[1]["Amount"]))
                    else:
                        activityOpenVal.append(0)
                else:
                    activityOpenNum.append(0)
                    activityOpenVal.append(0)

        actCost = [""] * len(activity)

        activityLosNum, activityLosVal = [], []
        for c, item in enumerate(activity):
            if activityWinNum[c] == 0 and activityOpenNum[c] == 0:
                activityLosNum.append(1)
                activityLosVal.append(activityVal[c])
            else:
                activityLosNum.append(0)
                activityLosVal.append(0)
        # activityLosNum = [item - activityWinNum.values()[count] - activityClsdNum.values()[count] for count, item in
        #                   enumerate(activityCnt.values())]
        # activityLosVal = [item - activityWinVal.values()[count] - activityClsdVal.values()[count] for count, item in
        #                   enumerate(activityVal.values())]
        CPO = actCost
        CPWO = actCost
        CPLO = actCost

        # Take in the config file for each activity and label each Unlabeled activity
        Tree = [make_report_activity_paths(
            d, act_dictionary) for d in activity]
        list_in_act_dict = False
        if act_dictionary is None:
            dflist = pd.DataFrame()
        if act_dictionary:
            for key in Tree:
                if key != ['Unlabeled']:
                    Tree.insert(0, act_dictionary["headers"])

                    # Create the dataframe with it's initial columns being created from the config file
                    dflist = create_column_lists(Tree)
                    list_in_act_dict = True
                    break
        if list_in_act_dict is False and act_dictionary:
            Tree = [['List']]*len(Tree)
            Tree.insert(0, act_dictionary["headers"])
            dflist = create_column_lists(Tree)

        data_values = [activity, companyNames, deals, actCost, activityVal, activityCnt, activityWinNum,
                       activityWinVal, activityLosNum, activityLosVal, activityOpenNum,
                       activityOpenVal, CPO, CPWO, CPLO, activity_timestamp, createDate, contact, direct_buyer, deal_stage, closeDate,
                       productLine, leadSource, territory, continent, billingCity, billingState, billingCountry,
                       opportunity_id, opportunityType, originalQuoteNumber, quoteNumber,]
        data_values += timelineFillerColumns

        for datnum, datavals in enumerate(data_fields):
            dflist[datavals] = data_values[datnum]

        dfout = df.append(dflist, ignore_index=True)
        print("Done.")
        self.RedisClient.set('Status', '{ status: "Done." }')

        if debug:
            return locals()
        else:
            return dfout
