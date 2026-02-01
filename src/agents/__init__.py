"""Agent modules for The Watch."""

from .listener_agent import TelegramListener
from .graph_orchestrator import (
    # Pipelines
    processing_pipeline,
    analyst_pipeline,
    
    # API Functions
    process_telegram_message,
    query_safety_status,
    get_breaking_news,
    
    # Graph builders (for customization)
    create_processing_graph,
    create_analyst_graph,
)

__all__ = [
    # Listener
    "TelegramListener",
    
    # Pipelines
    "processing_pipeline",
    "analyst_pipeline",
    
    # API
    "process_telegram_message",
    "query_safety_status",
    "get_breaking_news",
    
    # Builders
    "create_processing_graph",
    "create_analyst_graph",
]
