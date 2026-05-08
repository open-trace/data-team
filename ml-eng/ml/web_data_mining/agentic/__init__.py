"""
Optional LangChain + Tavily layer on top of the RSS / fetch pipeline.

Install: pip install -r ml/web_data_mining/requirements-agent.txt
Env: TAVILY_API_KEY (and optionally OPENAI_API_KEY for the research CLI).
"""

from ml.web_data_mining.agentic.enrichment import try_enrich_with_tavily

__all__ = ["try_enrich_with_tavily"]
