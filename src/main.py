from ingestion import run_bronze
from transformation import run_silver
from gold import run_gold


def main():
    print("Starting pipeline...")
    run_bronze()
    run_silver()
    run_gold()
    print("Pipeline completed successfully")

if __name__ == "__main__":
    main()