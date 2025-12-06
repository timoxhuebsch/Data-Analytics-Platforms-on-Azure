# run_all_layers.py
import sys
import os
import logging

# sicherstellen, dass die Layer-Ordner im Python-Pfad sind
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '00_landing')))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '01_bronze')))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '02_silver')))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '03_gold')))

from landing_app import main as landing_main      # aus landing/app.py
from bronze_app import main as bronze_main       # aus bronze/app.py
from silver_app import main as silver_main       # aus silver/app.py
from gold_app import main as gold_main         # aus gold/app.py

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

def main():
    log.info("Starting Landing layer...")
    landing_main()
    
    log.info("Starting Bronze layer...")
    bronze_main()
    
    log.info("Starting Silver layer...")
    silver_main()
    
    log.info("Starting Gold layer...")
    gold_main()
    
    log.info("All layers finished successfully.")

if __name__ == "__main__":
    main()
