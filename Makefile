# Makefile for local Airflow dev setup

PROJECT_NAME=contrail
AIRFLOW_CONTAINER=contrails-challenge-airflow-webserver-1
CONN_ID=fs_default
FS_PATH=/opt/data/raw_data

.PHONY: run_local_airflow stop clean logs

run_local_airflow:
	@echo "🚀 Building Docker images (if needed)..."
	docker compose build
	@echo "🔄 Starting Airflow stack..."
	docker compose up -d
	@echo "⏳ Waiting for Airflow webserver to be ready..."
	@until curl -sf http://localhost:8080/health | grep '"status": "healthy"'; do sleep 5; done
	@echo "✅ Webserver is up. Setting up connection $(CONN_ID)..."
	-docker exec $(AIRFLOW_CONTAINER) airflow connections delete $(CONN_ID)
	docker exec $(AIRFLOW_CONTAINER) airflow connections add '$(CONN_ID)' \
	  --conn-json '{"conn_type": "fs", "extra": {"path": "$(FS_PATH)"}}'
	@echo "🎉 Airflow is ready at http://localhost:8080"

stop:
	docker compose down

clean:
	docker compose down -v --remove-orphans

logs:
	docker compose logs -f
