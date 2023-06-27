.PHONY: run
run:
	docker compose up

.PHONY: stop
stop:
	docker compose down -v
