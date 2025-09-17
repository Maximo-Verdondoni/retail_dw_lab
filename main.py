# main.py
from etl.load import load

def main():
    print("Iniciando pipeline de Data Warehouse...")
    load.run_pipeline()
    print("✅ Pipeline completada con éxito!")

if __name__ == "__main__":
    main()
