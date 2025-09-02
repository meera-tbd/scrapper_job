# [cursor:reason] Convenience targets for Dockerized automation in IST

.PHONY: up logs flower schedule beat worker down

up:
	docker compose up -d --build

logs:
	docker compose logs -f web celery_worker celery_beat

flower:
	@echo "Open http://localhost:5555"
	docker compose up -d flower

beat:
	docker compose logs -f celery_beat

worker:
	docker compose logs -f celery_worker

schedule:
	@docker compose exec web python manage.py upsert_automation_schedules || true

down:
	docker compose down
