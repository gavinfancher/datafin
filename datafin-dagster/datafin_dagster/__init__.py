from .definitions import defs

# Make defs available at the package level so Dagster can find it
__all__ = ["defs"]

# Package metadata
__version__ = "0.1.0"
__author__ = "DataFin Team"
__description__ = "Financial data orchestration with Dagster"