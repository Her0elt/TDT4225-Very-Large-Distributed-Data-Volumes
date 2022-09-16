db:
	docker-compose up -d

down:
	docker-compose down -v

start:
	python3 main.py

setup:
	pip install -r requirements.txt