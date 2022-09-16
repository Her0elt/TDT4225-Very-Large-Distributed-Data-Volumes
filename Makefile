db:
	docker-compose up -d

# down:
	docker-compose down -v

# restart:
	make down && make db && make start

start:
	python3 main.py

setup:
	pip3 install -r requirements.txt

