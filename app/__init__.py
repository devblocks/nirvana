from flask import Flask
from flask_celery import make_celery
#from flask_sqlalchemy import SQLAlchemy

app = Flask(__name__)
app.config.broker_url = 'redis://localhost:6379/0'
app.config.result_backend = 'redis://localhost:6379/0'
#app.conf.result_backend = 'cache'
#app.conf.cache_backend = 'memory'

# App configuration
# http://flask.pocoo.org/docs/0.11/config/
app.config.update(
	TEMPLATES_AUTO_RELOAD=True
)

#app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://nirvana:Enlightenment!@localhost/nirvana'
#db = SQLAlchemy(app)

celery = make_celery(app)



from tasks import report, dummy, refresh
from app import views
