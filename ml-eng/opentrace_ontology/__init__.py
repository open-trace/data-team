"""OpenTrace agricultural ontology (offline). See README.md.

>>> from opentrace_ontology import lookup, normalize_query
>>> normalize_query("current state of casssava in Nigeria")
'current state of cassava in Nigeria'
"""
from .core import (Match, lookup, normalize_query, expand, get, aliases, children,
                   derived_products, codes, to_faostat, to_hs, CORE_TYPES,
                   ancestors, descendants, warehouse, search)
from .routing import (ResolvedEntity, resolve, route, plan_query, tag_chunk, bq_targets, unresolved_terms,
                      clean_profile_country, MODE_BQ, MODE_QD, MODE_WEB)
__all__ = ["Match", "lookup", "normalize_query", "expand", "get", "aliases", "children",
           "derived_products", "codes", "to_faostat", "to_hs", "CORE_TYPES",
           "ancestors", "descendants", "warehouse", "search",
           "ResolvedEntity", "resolve", "route", "plan_query", "tag_chunk", "bq_targets", "unresolved_terms",
           "clean_profile_country", "MODE_BQ", "MODE_QD", "MODE_WEB"]
__version__ = "2026.10.15.3"
