db:
	docker-compose up -d

down:
	docker-compose down -v

start:
	python3 main.py

setup:
	pip3 install -r requirements.txt