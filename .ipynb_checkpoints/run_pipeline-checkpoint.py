import subprocess
import sys
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("PipelineRunner")

scripts = [
    "ingestion/ingest_raw_to_bronze.py",
    "spark_jobs/bronze_to_silver.py",
    "spark_jobs/silver_to_gold.py",
    "warehouse/load_to_postgres.py"
]

for script in scripts:
    logger.info(f"Running {script}")
    result = subprocess.run(
        ["spark-submit", script],
        capture_output=True,
        text=True
    )

    if result.returncode != 0:
        logger.error(f"Error in {script}")
        logger.error(result.stderr)
        sys.exit(1)
    else:
        logger.info(f"{script} completed successfully")

logger.info("FULL PIPELINE EXECUTED SUCCESSFULLY")