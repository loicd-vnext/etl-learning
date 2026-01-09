"""
ETL Demo Project - Main Package
"""

__version__ = "0.1.0"

from src.pipeline import ETLPipeline, PipelineConfig, run_pipeline

__all__ = [
    "ETLPipeline",
    "PipelineConfig",
    "run_pipeline"
]

