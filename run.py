#!flask/bin/python
from app import app
from app import tasks

if __name__ == '__main__':
	tasks.refresh.delay()
	app.run(host='0.0.0.0',port=5000,debug=True)