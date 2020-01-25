from app import app # Flask import based on file structure; imports init.py 
from flask import render_template,request,Response,json,redirect,send_file
import time
import yaml # Used to recieve celery task status as the report is generating
from app import Nirvana as nv 
from tasks import report, dummy, refresh
import redis
import pandas as pd

from werkzeug.utils import secure_filename # Use to verify against filename hackery

# Instantiates new RedisClient
RedisClient = redis.StrictRedis(host='localhost', port=6379, db=0)

def allowed_file(filename): # When configuration file is uploaded, this is used to make sure it's a JSON
	return '.' in filename and filename.rsplit('.',1)[1].lower() in set(['json'])

@app.route('/',  methods=['GET','POST'])
def index():
	return render_template('appArgs.html')
	

@app.route('/task_status',  methods=['GET'])
def task_status():

	# Fetch the task metadata from Redis
	if RedisClient.get('Status') is None:
		RedisClient.set('Status','Status is None')
	task_meta = yaml.load(RedisClient.get('Status'))

	# Return the task metadata as JSON
	return Response(json.dumps(task_meta), status=200, mimetype='application/json')

@app.route('/progress', methods=['GET'])
def progress():
	return render_template('progress.html') # While the celery server is generating the report
	                                        #   this displays the report's status to the user

@app.route('/result', methods=['GET'])
def result():
	df = RedisClient.get('report') # Take the dataframe of the report from Redis, after it generates
	downloady = "<a href='./download' download='report.xlsx'><button type='submit'>Download Report</button></a>" # Download button
	download2 = "<a href='./download_config' download='config.json'><button type='submit'>Download Config File</button></a>" # Download button


	# TODO - Add Bootstrap 4 to the output to make it pretty
	return df+"<br/>"+downloady+"<br/>"+download2
	#return render_template(result.html,df=df)

@app.route('/download', methods=['GET'])
# TODO - information gets cached in Firefox, preventing downloads
def download():
	response = Response()
	response.headers['X-UA-Compatible'] = 'IE=Edge,chrome=1'
	response.headers['Cache-Control'] = 'public, no-store, no-cache, must-revalidate, post-check=0, pre-check=0, max-age=0'
	response.headers['Pragma'] = 'no-cache'
	response.headers['Expires'] = '-1'
	return send_file('../CascadingReport.xlsx',cache_timeout=5)

@app.route('/download_config', methods=['GET'])
def download_config():
	response = Response()
	response.headers['X-UA-Compatible'] = 'IE=Edge,chrome=1'
	response.headers['Cache-Control'] = 'public, no-store, no-cache, must-revalidate, post-check=0, pre-check=0, max-age=0'
	response.headers['Pragma'] = 'no-cache'
	response.headers['Expires'] = '-1'
	return send_file('../config.json',cache_timeout=5)


@app.route('/config', methods = ['POST'])
def config():
	# Turns the request data into a dictionary
	requestData = request.get_json()

	# Isolates the 'config' attribute of the requestData
	configData = requestData['config']

	# Send config to Redis
	RedisClient.set('config', configData)

	# Handles response
	return Response(json.dumps(dict({ "success": True, "config": configData })), status=200, mimetype='application/json')


@app.route('/data', methods=['POST'])
def data():

	#return str(request.form['startDate'])

	report.delay(str(request.form['startDate']),str(request.form['endDate']),int(request.form['attribDays']),int(request.form['eventDays'])) # Sends the report request to celery
	return render_template('data.html')




