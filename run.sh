sudo docker run -p 6379:6379 -d redis
celery -l info -E -A app.celery worker &
python run.py