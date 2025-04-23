from dagster import IOManager, io_manager
import pandas as pd
import os

class CSVOutputIOManager(IOManager):
    def __init__(self, base_path="preprocess"):
        self.base_path = base_path

    def handle_output(self, context, obj: pd.DataFrame):
        filename = f"{context.asset_key.path[-1]}.csv"
        full_path = os.path.join(self.base_path, filename)
        obj.to_csv(full_path, index=False)
        context.log.info(f"✅ Saved CSV to {full_path}")

    def load_input(self, context):
        filename = f"{context.asset_key.path[-1]}.csv"
        full_path = os.path.join(self.base_path, filename)
        df = pd.read_csv(full_path)
        context.log.info(f"📥 Loaded CSV from {full_path}")
        return df

@io_manager
def csv_output_io_manager():
    return CSVOutputIOManager()
