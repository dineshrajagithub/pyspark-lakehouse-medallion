"""Script entry point for Databricks `spark_python_task` / spark-submit."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from lakehouse.pipeline import main  # noqa: E402

if __name__ == "__main__":
    main()
