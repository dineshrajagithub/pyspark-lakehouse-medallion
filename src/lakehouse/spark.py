from __future__ import annotations

from pyspark.sql import SparkSession


def get_spark(app_name: str = "lakehouse", use_delta: bool = False) -> SparkSession:
    """Return the active session (Databricks) or build a local one."""
    active = SparkSession.getActiveSession()
    if active is not None:  # e.g. inside a Databricks job
        _set_parsing_mode(active)
        return active

    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.session.timeZone", "UTC")
        # Keep partition values (batch ids) as strings.
        .config("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
    )
    if use_delta:
        from delta import configure_spark_with_delta_pip

        builder = (
            builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog",
                    "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        )
        session = configure_spark_with_delta_pip(builder).getOrCreate()
    else:
        session = builder.getOrCreate()
    _set_parsing_mode(session)
    return session


def _set_parsing_mode(session: SparkSession) -> None:
    # Silver rules rely on unparseable values becoming NULL (then quarantined)
    # instead of failing the whole job, so disable ANSI strict casting.
    session.conf.set("spark.sql.ansi.enabled", "false")
