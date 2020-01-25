from app import celery
from app import Nirvana as nv
from app import CascadingSheetsCreator as csc
from app import DataFormatter as dat
import json
import time
import redis
import pandas as pd

from apscheduler.schedulers.background import BackgroundScheduler # Scheduler to update cache
from apscheduler.triggers.interval import IntervalTrigger

RedisClient = redis.StrictRedis(host='localhost', port=6379, db=0)

@celery.task(name='Nirvana.report')
def report(start,end,att,ev):
    mgr = nv.NirvanaManager()
    config = RedisClient.get('config') # Get the config file for sorting activities
    cc = csc.CascadingSheetsCreator()

    #Make sure the headers and the config file has the structure that it's suppose to
    if config:
        cc.check_headers(config)
    else:
        config = {'headers':['Misc']}

    # runs the main function
    df = mgr.app2(start,end,att,ev,act_dictionary=config)
        

    #Save the config file with the original activities and any new activities that were loaded
    with open('config.json', 'w') as fp:
        json.dump(config, fp)


    # headers_list = config['headers'] # List of headers used to format pivot table and sort activities
    # headers_list.append('Activity')
    # headers_list.append('Company')
    
    # Create the Cascading Report 
    # cc.generate(df,headers_list) # Report created with name 'CascadingReport.xlsx'
        # pt_dict = create_pivot_table(startdf,headers_list)
        # create_top_sheet(pt_dict['level1'],self.wb)
        # generate_recursive(pt_dict,self.wb,level=1,chain='Total')

    df.to_excel('CascadingReport.xlsx')

    formatter = dat.DataFormatter()
    formatter.nirvana_format('CascadingReport.xlsx')

    #Show a preview of the report in the web browser
    RedisClient.set('report', df.to_html())
    return True

@celery.task(name='Nirvana.refresh')
def refresh():
    def nirvana_wrapper():
        today = ('{}/{}/{}'.format(time.localtime().tm_mon,time.localtime().tm_mday,time.localtime().tm_year))
        last_year = ('{}/{}/{}'.format(time.localtime().tm_mon,time.localtime().tm_mday,time.localtime().tm_year-1))
        mgr = nv.NirvanaManager()
        mgr.app2(last_year,today,'30','45',flush=True)
        print("Refreshing!")
    
    scheduler = BackgroundScheduler()
    scheduler.start()
    scheduler.add_job(
        func= nirvana_wrapper,
        trigger=IntervalTrigger(seconds=86400)) # repeat every day
    return True


@celery.task(name='Nirvana.dummy')
def dummy():
    time.sleep(15)
    RedisClient.set('Status', '{ status: "Getting Deals..." }')
    time.sleep(3)
    RedisClient.set('Status', '{ status: "Getting Form Submissions..." }')
    time.sleep(3)
    RedisClient.set('Status', '{ status: "Getting Opportunity Info..." }')
    time.sleep(3)
    RedisClient.set('Status', '{ status: "Creating Outputs..." }')
    time.sleep(3)
    df = pd.DataFrame([range(5),range(2,10,2)],columns=['A','B','C','D','E'])
    report = df.to_excel("app/a.xlsx")
    RedisClient.set('report', df.to_html())
    RedisClient.set('Status', '{ status: "Done." }')
    return True