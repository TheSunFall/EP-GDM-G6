from functools import reduce
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql.functions import cast, col, lit, try_divide
from pyspark.sql.types import DoubleType

from app.client.mef_client import MefClient
from app.pipeline.bronze import BronzePipeline
from app.settings.settings import settings


def main():
    print("Starting main...")
    pipeline = BronzePipeline()
    pipeline.run()


if __name__ == "__main__":
    main()
