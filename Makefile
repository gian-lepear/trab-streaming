run:
	@docker compose up

shell:
	@docker exec -it spark-master-1 bash

clean_spark_data:
	@sudo rm -rf data/stream/*;
	@sudo rm -rf data/checkpoint/*;
	@sudo rm -rf data/batch/*;
	@sudo rm data/checkpoint/.metadata.crc;
