# LSM Tree Indexing in Apache Iceberg — CS631 Project

Team: Shiv Narang (23b1075), Kaushal Vijayvergiya (23b0901), Kritin Gupta (23b0948)

* **`lsm-iceberg-java/`**: the main implementation (Java 17, Apache Iceberg 1.10). See its README.
  Quick start: `cd lsm-iceberg-java && ./run.sh test && ./run.sh demo`
* `checkpoint_report.pdf`: 2-page checkpoint printout (HTML source: `checkpoint_report.html`)
* `readme.txt`: original project proposal
* `lsm_iceberg/`, `scripts/`, `tests/`: earlier Python/PyIceberg prototype of the same design
  (setup: `python3 -m venv .venv && .venv/bin/pip install -r requirements.txt`;
  run: `.venv/bin/python scripts/demo.py`)
