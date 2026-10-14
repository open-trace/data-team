"""Slide copy for the Ask ADZA community teaching deck (16:9).

Regenerate from ml-eng/:
  python scripts/generate_ask_adza_teaching_pdf.py
  python scripts/generate_ask_adza_teaching_pptx.py
"""
from __future__ import annotations

from pathlib import Path

ML_ENG_ROOT = Path(__file__).resolve().parents[1]
DOCS_DIR = ML_ENG_ROOT / "ml" / "rag" / "docs"
OUTPUT_PPTX = DOCS_DIR / "OpenTrace-Ask-ADZA-Community-Teaching.pptx"
OUTPUT_PDF = DOCS_DIR / "OpenTrace-Ask-ADZA-Community-Teaching.pdf"
DOC_VERSION = "1.0"

# Each slide is a dict with ``kind`` plus layout fields and ``notes`` (speaker script).
# kinds: title | section | bullets | two_column | table | pipeline | cards

SLIDES: list[dict] = [
    # ------------------------------------------------------------------ Act 1
    {
        "kind": "title",
        "kicker": "OpenTrace community  ·  teaching deck",
        "title": "Ask ADZA",
        "subtitle": "OpenTrace’s RAG for African agricultural intelligence",
        "footer": "What we are building  ·  How we are building it  ·  Why the design is the lesson",
        "notes": (
            "Open with the product, not the stack. RAG is the method. Ask ADZA is what people use.\n\n"
            "Run of show: 35 minutes for the full deck; 12 minutes = product, naive vs production, "
            "one-picture architecture, bind-first SQL, Ghana walkthrough, OFIA/ACF, principles.\n\n"
            "Teaching beat: after the title, ask who in the room has shipped a chatbot that cites sources."
        ),
    },
    {
        "kind": "section",
        "title": "Act 1  ·  The product",
        "subtitle": "Why this exists, who it serves, what a good answer means",
        "notes": "Set the problem before any Qdrant or BigQuery. Keep this act under five minutes.",
    },
    {
        "kind": "bullets",
        "title": "The problem we are teaching against",
        "bullets": [
            "African agricultural intelligence is fragmented: FAO, FEWS NET, national stats, papers, news, extension",
            "Different grains — country vs district vs household — and different definitions of yield, price, food security",
            "Decision-makers still ask in natural language",
            "A chatbot that sounds right is not enough: a number without a source can mislead policy",
        ],
        "footnote": "Teaching beat: if ChatGPT already answers “maize yield in Kenya,” why build this?",
        "notes": (
            "Answer we want: generic models do not sit on our warehouse, our corpora, our geography, "
            "or our honesty rules. This deck teaches the contract, not the demo.\n\n"
            "Pause after the teaching beat and let the room answer before you give that line."
        ),
    },
    {
        "kind": "bullets",
        "title": "What we are building",
        "bullets": [
            "Ask ADZA is OpenTrace Africa’s natural-language interface",
            "Ask: maize yields in Kenya, rice prices in West Africa, Somalia food-security outlook",
            "Grounded in OpenTrace data and curated evidence — cited, so a human can check",
            "Honest when the grain, year, or table is missing",
            "Audience-aware: ministry vs farmer vs NGO vs agribusiness",
        ],
        "footnote": "Not a search box. A decision interface with a retrieval contract.",
        "notes": (
            "Lead with capability, then the four musts: grounded, cited, honest, audience-aware.\n\n"
            "Example questions are real gold-trace shapes, not marketing copy."
        ),
    },
    {
        "kind": "cards",
        "title": "Who it is for — same facts, different voice",
        "cards": [
            {
                "title": "Government",
                "body": "Planning takeaway, named indicators, and gaps that would change the recommendation.",
            },
            {
                "title": "NGOs / partners",
                "body": "Who and where is affected; overlapping climate, nutrition, and market risk.",
            },
            {
                "title": "Agribusiness / finance",
                "body": "Stability, volatility, sourcing and exposure for commercial decisions.",
            },
            {
                "title": "Farmers / co-ops",
                "body": "Plain language, bullets, no jargon dump — rainfall, markets, production takeaways.",
            },
        ],
        "footnote": "Retrieval is mostly shared. Generation is not.",
        "notes": (
            "Teaching beat: same Kenya rice numbers; a farmer gets takeaways, a planner gets monitoring cues.\n\n"
            "Persona is a generation register (category), not a different database. plan_type also gates retrieval "
            "(Farmers country filter, formation boost)."
        ),
    },
    {
        "kind": "two_column",
        "title": "Naive RAG vs production RAG",
        "left_title": "Tutorial RAG",
        "left": [
            "Embed the question",
            "Top-k chunks from one index",
            "Stuff into a prompt",
            "LLM talks",
        ],
        "right_title": "Ask ADZA",
        "right": [
            "Understand the job (measure, geo, time, mode)",
            "Retrieve from two peer systems",
            "Fuse, rerank, diversify",
            "Plan the answer shape, then generate",
        ],
        "footnote": "The LLM is last. The contract is first.",
        "notes": (
            "Why tutorials fail here: “price of maize in Nigeria 2022” is a table fact; district rainfall in Malawi "
            "may be unsupported grain; news can drown FAO; users type “prize od maize”.\n\n"
            "Teaching beat: which skipped step causes the worst failure? Usually wrong measure, wrong country, "
            "or generating a trend with no warehouse rows."
        ),
    },
    # ------------------------------------------------------------------ Act 2
    {
        "kind": "section",
        "title": "Act 2  ·  Two retrieval legs",
        "subtitle": "Documents and tables are peers. They meet at merge.",
        "notes": "Draw the one-picture pipeline. Everything later is a zoom-in on one box.",
    },
    {
        "kind": "pipeline",
        "title": "Runtime architecture",
        "steps": [
            "Control plane",
            "Vector leg",
            "BQ leg",
            "Merge",
            "Generate",
            "ACF",
        ],
        "footnote": "Vector and BQ are peers until merge. ACF Path B scores cited evidence only — not the retrieval dump.",
        "notes": (
            "Walk the diagram top to bottom. Entry → control plane → parallel retrieve_legs → "
            "merge/rerank → generate → ACF → export.\n\n"
            "Point at the dark ACF box: confidence is a first-class output, not a slide we skipped."
        ),
    },
    {
        "kind": "two_column",
        "title": "Two kinds of truth",
        "left_title": "Unstructured  ·  Qdrant",
        "left": [
            "Six corpora: news, papers, policies, reports, formation, OTA",
            "Good at why, how, policy, practice, “what does research say”",
            "Hybrid dense E5 + BM25 sparse",
        ],
        "right_title": "Structured  ·  BigQuery mart_dev",
        "right": [
            "Class engines bind tables, filters, measure columns",
            "Good at how much, when, where, compared to whom",
            "SQL compiled from the bind; NL2SQL is fallback",
        ],
        "footnote": "Do not mix grains: employment % shares ≠ household income samples. Classes encode do_not_mix.",
        "notes": (
            "A paper can explain a yield gap. Only the mart should state last year’s tonnes.\n\n"
            "Teaching beat: when should news outrank FAO? Almost never for a tonne-count."
        ),
    },
    # ------------------------------------------------------------------ Act 3
    {
        "kind": "section",
        "title": "Act 3  ·  Vector embedding and search",
        "subtitle": "Not embed(question) → top-k. A contract: chunk, prefix, hybrid, filter, cascade, route.",
        "notes": (
            "Workshop version: all remaining vector slides. Twelve-minute talk: embedding vs keyword, "
            "E5 prefixes, hybrid RRF, query views, cascade, cheat sheet."
        ),
    },
    {
        "kind": "two_column",
        "title": "What an embedding is",
        "left_title": "Keyword search",
        "left": [
            "Token overlap — maize must appear",
            "Language-locked",
            "Brittle to paraphrase and typos",
        ],
        "right_title": "Embedding search",
        "right": [
            "Similar meaning ≈ close in cosine space",
            "Multilingual E5 can align EN / FR / SW",
            "Helps paraphrases — is not a spellchecker",
        ],
        "footnote": "Embeddings do not know FAO vs a blog, 2022 vs 2014, or Ghana vs Kenya. Filters do.",
        "notes": (
            "Teaching beat: is retrieval “AI”? Only the encoder is a model. Filters, cascade, hybrid fusion, "
            "and corpus routing are systems design.\n\n"
            "A 384-d vector cannot replace a warehouse row."
        ),
    },
    {
        "kind": "two_column",
        "title": "Index time vs query time — the contract",
        "left_title": "Index (ingest)",
        "left": [
            "Chunk per corpus strategy",
            "Prefix: passage: …",
            "Encode with the same E5 model used at search",
            "Store dense + BM25 sparse + payload",
            "Build payload indexes so filters do not 400",
        ],
        "right_title": "Query",
        "right": [
            "Choose corpora; up to three query texts",
            "Prefix: query: …",
            "Hybrid search with geo / time / doc_kind inside prefetch",
            "If empty, relax filters in a fixed order",
            "Post-filter geography; stamp constraint_relaxed",
        ],
        "footnote": "The model at search must equal the model at index. Dim mismatch is a hard fail.",
        "notes": (
            "Teaching beat: we have already hit expected dim 384, got 768. That is an ops contract, "
            "not a “rerank later” problem.\n\n"
            "Bump INGEST_VERSION and reindex after encoder or chunking changes."
        ),
    },
    {
        "kind": "bullets",
        "title": "E5 prefixes — two words that change recall",
        "bullets": [
            "multilingual-E5 is asymmetric: index passage: …  ·  search query: …",
            "Skip the prefixes and you search a different space than the one you indexed",
            "Vectors are L2-normalized; Qdrant distance is cosine",
            "Backend: sentence_transformers locally, fastembed ONNX on Railway",
            "Changing to a 768-d model is a full corpus reindex, not a config flip",
        ],
        "footnote": "Prefix, dimension, and chunking are versioned together (INGEST_VERSION).",
        "notes": (
            "Show the two strings on a whiteboard if you can: passage: Rice production in Ghana… vs "
            "query: How has rice production in Ghana changed…\n\n"
            "One SentenceTransformer instance per process — load is slow and not thread-safe on Windows."
        ),
    },
    {
        "kind": "table",
        "title": "Three chunk / embed families — six collections",
        "headers": ["Family", "Collections", "Encoder", "Chunking"],
        "rows": [
            ["News", "news_data", "e5-base  ·  384-d", "recursive_semantic  ·  ~400 tok, 15% overlap"],
            [
                "Research",
                "academic, policies, reports, formation",
                "e5-small  ·  384-d",
                "hierarchical_semantic  ·  ~500 tok",
            ],
            ["OTA", "OTA_insights", "e5-base  ·  384-d", "lane_semantic  ·  insight / metric / rec"],
        ],
        "footnote": "Chunking is a retrieval decision. A 2,000-token PDF dump matches nothing useful.",
        "notes": (
            "News is short and timely. Papers have sections; we drop boilerplate section_role. "
            "OTA is three named dense vectors, not one blob.\n\n"
            "Teaching beat: should “how to plant rice” hit academic PDFs first? Formation exists so it does not have to."
        ),
    },
    {
        "kind": "bullets",
        "title": "What is actually stored in Qdrant",
        "bullets": [
            "Each point: dense vector(s) + BM25 sparse + payload (geo, year, doc_kind, text, ACF fields)",
            "News / research: one dense named vector + one sparse; OTA: three dense + two sparse",
            "HNSW + INT8 — approximate nearest neighbour: fast, not exact",
            "Payload indexes: doc_kind, geo_country_primary, country, geo_countries, published_at or publication_year",
            "Unindexed filter fields are dropped — a missing index must not 400 the turn",
        ],
        "footnote": "Research years are KEYWORD (MatchAny on 2019, 2020, …), never a numeric Range.",
        "notes": (
            "ANN is why we over-fetch, hybrid-fuse, then cross-encoder rerank later.\n\n"
            "Teaching beat: filter without an index = outage. Filter with the wrong type = silent empty."
        ),
    },
    {
        "kind": "pipeline",
        "title": "Hybrid search: dense + sparse + RRF",
        "steps": [
            "Query text",
            "E5 dense ~20",
            "BM25 sparse ~20",
            "RRF fuse",
            "Trim to top_k",
        ],
        "caption": (
            "Dense is good at paraphrases; sparse is good at rare tokens (IPC Phase 3, CHIRPS, variety names). "
            "Geo / time / doc_kind is applied inside each Prefetch — not after fusion. Hybrid failure → dense-only."
        ),
        "footnote": "Hybrid is two encodings of the same query against the same points — not two collections.",
        "notes": (
            "Qdrant-recommended pattern: filter inside Prefetch so Ghana is in the candidate set, "
            "not stripped after fusion.\n\n"
            "Teaching beat: why both? “East Coast fever” needs BM25; “how has production changed” needs E5."
        ),
    },
    {
        "kind": "table",
        "title": "Payload filters — the structured half of vector search",
        "headers": ["Signal", "How"],
        "rows": [
            ["Document type", "doc_kind = news_article, academic_article, policy_document, …"],
            ["One country", "geo_country_primary OR country OR geo_countries contains name"],
            ["Several countries", "should over per-country subfilters (compare queries)"],
            ["News time", "Lexicographic range on ISO published_at"],
            ["Research time", "publication_year in the years of the window"],
            ["News domain", "MatchText on domains (on by default)"],
        ],
        "footnote": "Compare + two countries: geo fallback is off. Thin evidence beats a swapped country.",
        "notes": (
            "Embeddings retrieve aboutness. Payload enforces who/when. You need both.\n\n"
            "Client post-filter on geography after cascade so a relaxed hit that mentions the wrong country still dies. "
            "Research also excludes boilerplate section_role."
        ),
    },
    {
        "kind": "table",
        "title": "Three query views — never replace the user",
        "headers": ["View", "What it is", "Used for"],
        "rows": [
            ["user_query", "Normalized raw text — always first", "Always embedded"],
            ["context_rewrite", "Related history + profile hints (country is a hint)", "Vector + BQ, only if related"],
            ["detail_rewrite", "Deterministic twin: geos, crops, years, measures", "Vector only — never SQL"],
        ],
        "footnote": "Up to three Qdrant passes, same filters, merge by point id. Flag user_query_dropped if omitted.",
        "notes": (
            "Follow-up: “what about Côte d’Ivoire?” after Ghana rice. user_query stays the follow-up; "
            "context_rewrite may add rice + production only because the turn is elliptical.\n\n"
            "Unrelated “thanks / new topic” must not glue Ghana onto the next search. "
            "detail_rewrite never hits BQ — avoids bind drift."
        ),
    },
    {
        "kind": "table",
        "title": "Corpus router — which of the six get a search",
        "headers": ["Cue", "Prefer", "Often skip"],
        "rows": [
            ["Briefing / latest news", "news, OTA", "academic, policies, formation"],
            ["What does research say", "academic, policies", "news, OTA, formation"],
            ["Fact lookup (numeric)", "public reports, news", "academic, policies, formation"],
            ["Farmers plan", "formation, news", "academic (unless research cues)"],
            ["Prices / markets", "news, OTA, reports", "—"],
            ["Outlook job", "OTA + public reports", "contract override"],
        ],
        "footnote": "Deterministic, no LLM. Never empty. Soft-skip cap 3 (or 4 on fact/briefing/research). Soft-fail empty corpora.",
        "notes": (
            "We pay Qdrant latency per collection. Routing is quality and cost.\n\n"
            "Selected corpora also get corpus_boost at rerank. Six collections run in a thread pool after decompose; "
            "vector and BQ start together."
        ),
    },
    {
        "kind": "pipeline",
        "title": "Filter cascade — recall without swapping country",
        "steps": [
            "Full filters",
            "Time ±1 year",
            "Drop time",
            "Drop geo",
            "Drop both",
        ],
        "caption": (
            "Stop at the first non-empty set. Stamp constraint_relaxed. "
            "fact_lookup / data_export_only: one level only. Two-country compare: no geo drop. Hard time filter: no time drop."
        ),
        "footnote": "Relaxing geo can surface a regional FAO report not tagged GHA — that is why we stamp and still post-filter.",
        "notes": (
            "Teaching beat: is relaxing geo a bug? Sometimes it is the only way to get untagged regional reports. "
            "Generator and ACF can treat relaxed hits as weaker evidence.\n\n"
            "Empty after all levels → [] for that corpus; the graph continues."
        ),
    },
    {
        "kind": "pipeline",
        "title": "End-to-end vector path",
        "steps": [
            "Decompose",
            "select_corpora",
            "Embed views",
            "Hybrid + filter",
            "Cascade",
            "Geo post-filter",
            "Merge views",
        ],
        "caption": (
            "Then fuse with BigQuery rows → rerank + diversify. "
            "News splits top_k across countries. Session KV may reuse hits; never cache the final answer."
        ),
        "footnote": "Soft-fail: one empty corpus does not kill the turn.",
        "notes": (
            "Walk this slowly if the room is engineers. If mixed community, spend the time on the next slide "
            "(Ghana) instead of naming every function."
        ),
    },
    {
        "kind": "bullets",
        "title": "Worked example — vector only",
        "bullets": [
            "How has rice production in Ghana changed over the last 5 years?",
            "Always embed the user question; detail twin may add Ghana | rice | production | years",
            "PROD tags boost public reports, news, formation; academic still likely active",
            "News: e5-base + Ghana + published_at; research family: e5-small + publication_year",
            "If Ghana+year is empty → widen year, then drop time — do not jump to Nigeria",
            "Vector explains why production moved. BQ states how many tonnes.",
        ],
        "footnote": "If you only show this slide, people think RAG is “search the PDFs.” Bring the warehouse next.",
        "notes": (
            "Hybrid: rice / Ghana / production get BM25 help; “changed over” is on the dense side.\n\n"
            "Hits tagged [News], [Public report], … later lose to BQ tonnes at rerank if both exist."
        ),
    },
    {
        "kind": "table",
        "title": "Vector failure modes we teach",
        "headers": ["Failure", "What broke", "Guardrail"],
        "rows": [
            ["Dim mismatch 384 vs 768", "Query encoder ≠ index encoder", "Profile + INGEST_VERSION; reindex"],
            ["Generic low-relevance hits", "query: / passage: omitted", "Central prefix helper"],
            ["Qdrant 400", "Filter on unindexed field", "Drop unindexed conditions"],
            ["Empty research years", "Range on KEYWORD year", "MatchAny on year list"],
            ["Nigeria story in Kenya vs Uganda", "Geo cascade too eager", "Fallback off on 2-country compare"],
            ["Follow-up searches wrong country", "Rewrite replaced user_query", "Always embed user_query; flag drop"],
        ],
        "footnote": "Debug order: encoder+dim → prefixes → indexes → geo/time → cascade stamp → router active → then the LLM.",
        "notes": (
            "Do not start with the prompt. Hybrid missing fastembed falls back to dense-only. "
            "News flood at generation is a diversify / BQ boost problem, not an embedding problem."
        ),
    },
    {
        "kind": "bullets",
        "title": "Vector robustness cheat sheet",
        "bullets": [
            "Same model, same dim, same prefixes at index and query",
            "Chunk per genre — dense for meaning, sparse for rare tokens, RRF to fuse",
            "Filter in ANN, not after; indexes are part of the schema",
            "Always search the user’s words; rewrites are extra passes",
            "Relax in order; stamp it; never swap country on a compare",
            "Six corpora, not one soup; ANN is approximate → over-fetch, then rerank",
        ],
        "footnote": "Vector is a peer of BigQuery, not a substitute for a number.",
        "notes": "Leave this up. Ten principles; we showed six on-slide. The rest: reindex is a release; soft-fail empty corpora.",
    },
    # ------------------------------------------------------------------ Act 4
    {
        "kind": "section",
        "title": "Act 4  ·  Warehouse, jobs, generation",
        "subtitle": "Bind first, SQL second. Plan the answer shape before the LLM writes prose.",
        "notes": "Return from the vector deep-dive. The Ghana walkthrough now uses both legs.",
    },
    {
        "kind": "bullets",
        "title": "Warehouse path — SQL is compiled, not hoped for",
        "bullets": [
            "Map the question onto 15 indicator classes (PROD, PRC, FS, CLIM, …)",
            "Class engines emit a bind contract: table, filters, measure columns — no SELECT string",
            "compile_sql_from_bind writes the fact_lookup / export SQL",
            "NL2SQL is fallback when the bind cannot compile; templates stay off the planned path",
            "Validate: SELECT-only, dataset allowlist (mart_dev), LIMIT — then execute",
        ],
        "footnote": "Engines bind. The compiler writes SQL. The LLM does not own the fact path.",
        "notes": (
            "Teaching beat: this is software engineering applied to RAG. The model proposes meaning; code owns the query.\n\n"
            "Analytical / export may still use a reasoner SQL escape. sql_compiler.py is a legacy stub — do not revive it."
        ),
    },
    {
        "kind": "table",
        "title": "Fifteen indicator classes — domain as routing",
        "headers": ["Code", "Class", "Code", "Class"],
        "rows": [
            ["PROD", "Agricultural production", "PRC", "Prices and markets"],
            ["FS", "Food security and nutrition", "FVC", "Food system and value chain"],
            ["CLIM", "Climate and weather", "SOIL", "Soil health and land"],
            ["EL", "Economic and livelihood", "GYI", "Gender, youth and inclusion"],
            ["AH", "Animal health", "VEG", "Vegetation"],
            ["ENV", "Environment and emissions", "INP", "Inputs"],
            ["HDI", "Human development", "BIO / RES", "Biodiversity  ·  Research system"],
        ],
        "footnote": "Each class has primary tables, companions, corpus policy, and hard rules. Nigeria maize price 2022 → PRC, not PROD.",
        "notes": (
            "Aligned with OpenTrace Mart Complete Guide §6. Multi-bind: intra-class companions plus cross-class "
            "supervisor secondaries merge into one bind_contracts map."
        ),
    },
    {
        "kind": "table",
        "title": "Task modes — not every question is the same job",
        "headers": ["Mode", "Meaning"],
        "rows": [
            ["fact_lookup", "One number / latest value"],
            ["analytical", "Trend, compare, ranking, report"],
            ["briefing", "What’s new"],
            ["research", "What does the literature say"],
            ["data_export_only", "Just the table"],
            ["clarify", "Missing measure / crop / place — ask, don’t guess"],
        ],
        "footnote": "“Which districts in Malawi had the highest rainfall in 2023?” → unsupported grain if the mart has no admin-2 rain.",
        "notes": (
            "Precedence after enricher/decompose/ontology: clarify → analytical → data_export_only → fact_lookup → "
            "research → briefing → chat.\n\n"
            "Teaching beat: the honest answer is unsupported grain, not a hallucinated district ranking."
        ),
    },
    {
        "kind": "pipeline",
        "title": "Ghana rice — both legs",
        "steps": [
            "Decompose",
            "PROD bind",
            "Vector + BQ",
            "Merge",
            "Rerank",
            "Trend plan",
            "Generate",
        ],
        "caption": (
            "Crop rice · geo Ghana · last 5 years · job trend · measure production (not yield, not price). "
            "Bind Ghana + rice + year window → compile SQL. Generation plan: shape = trend; lead with structured value; ground in BigQuery."
        ),
        "footnote": "Worst skip: wrong measure, wrong country, or a trend with no BQ rows.",
        "notes": (
            "Pause here. Ask the room which step, if skipped, causes the worst failure.\n\n"
            "Persona then shapes voice: farmer bullets vs planner monitoring cues. Citations from packed sources. "
            "ACF scores what was cited, not the whole retrieval dump."
        ),
    },
    {
        "kind": "pipeline",
        "title": "We reason three times",
        "steps": [
            "Pre-retrieval",
            "Retrieval",
            "Post-retrieval",
        ],
        "caption": (
            "1. Enrich → decompose → ontology → task mode → retrieval contract. "
            "2. Corpus select + class engines + bind/NL2SQL + rerank. "
            "3. build_generation_plan decides answer shape before the LLM writes prose."
        ),
        "footnote": "History is a companion embed, not a replacement embed.",
        "notes": "This is the “how we build” slide for engineers who want the control plane without the vector internals again.",
    },
    {
        "kind": "bullets",
        "title": "Fusion: retrieve wide, pack narrow",
        "bullets": [
            "Merge is the first moment both legs meet",
            "Cross-encoder rerank (production) scores the fused pool; source boost prefers BigQuery over news",
            "Diversify so journalism cannot occupy every slot",
            "Optional web fallback (Wikipedia, then Tavily) only if internal evidence is thin",
            "Still empty → deterministic “not enough information,” not a fluent guess",
        ],
        "footnote": "A good chunk that never survives rerank never reaches the model.",
        "notes": (
            "Static source boost: bigquery 0.12, academic/policy/report 0.06, news 0.04, web 0. "
            "Rerank never raises — degrades openrouter → cross_encoder → off."
        ),
    },
    {
        "kind": "two_column",
        "title": "Generation is planned, then written",
        "left_title": "Generation plan",
        "left": [
            "Shape: fact, ranking, comparison, trend, briefing, research, gap",
            "Evidence priority: e.g. BigQuery → public report → news",
            "Must-ground-in: warehouse vs narrative",
            "Persona register: vocabulary, tables vs bullets",
        ],
        "right_title": "Hard rules",
        "right": [
            "Do not invent warehouse numbers",
            "Typed gaps when SQL failed or grain is unsupported",
            "Citations from packed sources, not model memory",
            "Table names and SQL stay internal",
        ],
        "footnote": "v1 generation plan shapes the prompt; it does not re-filter chunks.",
        "notes": "Farmers: bullet layout. Analytical: dynamic outline from measure + companions + BQ fingerprint.",
    },
    {
        "kind": "two_column",
        "title": "Trust stack: OFIA vs ACF",
        "left_title": "OFIA — who may speak",
        "left": [
            "Tier 1: FAO, WB, national stats, peer review, BigQuery facts",
            "Tier 2: news, extension, OTA, co-ops, web fallback",
            "Tier 3: user-submitted observations (future)",
        ],
        "right_title": "ACF — how confident is this answer",
        "right": [
            "Scored from cited evidence only — not the retrieval dump",
            "Bands from very strong → no evidence",
            "A Tier-1 FAO report can still score low if it is off-year or off-country",
        ],
        "footnote": "Authority ≠ relevance. Do not collapse these two ideas.",
        "notes": (
            "Teaching beat: OFIA is source authority. ACF is claim confidence from what the model actually cited.\n\n"
            "Path B ACF is the API-facing 0–100 signal."
        ),
    },
    {
        "kind": "table",
        "title": "ACF Path B — bands on the answer",
        "headers": ["Band", "Label", "What it means"],
        "rows": [
            ["very_strong", "Very strong", "Cited OpenTrace sources align on geo, time, and claim"],
            ["strong", "Strong confidence", "Solid cited evidence; small gaps remain"],
            ["moderate", "Moderate", "Usable evidence, but coverage or recency is mixed"],
            ["limited", "Limited", "Thin or off-scope cites — or warehouse returned zero rows"],
            ["low", "Low confidence", "Timeout / weak orientation — not structured fact"],
            ["no_evidence", "No evidence", "Nothing scorable was cited, or warehouse never ran"],
        ],
        "footnote": "Score is 0–100. The API returns band, band_label, score, explanation, and any applied_ceiling.",
        "notes": (
            "ACF lives in acf_scoring.py. It adapts cited SourceRefs to ExtractedClaim, then score_evidence.\n\n"
            "Curated product/help answers use a fixed strong band — they are not triangulated."
        ),
    },
    {
        "kind": "bullets",
        "title": "How ACF scores a turn",
        "bullets": [
            "Only cited packed sources are scored — unused retrieval chunks do not inflate confidence",
            "BQ rows become ACF records via warehouse fields (geo, year, unit); vectors via payload",
            "Question type and claim level come from the query (fact vs trend vs outlook)",
            "Ceilings cap the score: bq_timeout, bq_empty, bq_never_executed, partial_panel, weak_orientation",
            "The user sees band + explanation with the answer — not a hidden debug field",
        ],
        "footnote": "A Tier-1 FAO chunk that is off-year still lowers ACF. Authority is OFIA; fit is ACF.",
        "notes": (
            "Walk a Ghana rice answer: warehouse rows cited → high ACF. Same prose with only news cites → lower. "
            "SQL timeout with no rows → low / no_evidence, even if the model sounds sure."
        ),
    },
    {
        "kind": "table",
        "title": "ACF ceilings — when confidence is capped",
        "headers": ["Ceiling", "Trigger", "Effect"],
        "rows": [
            ["bq_never_executed", "SQL never ran or failed validation", "no_evidence  ·  score 0"],
            ["bq_timeout", "Warehouse jobs timed out, no usable rows", "low  ·  score ≤ 35"],
            ["partial_panel", "Some companion queries timed out", "score cut; coverage reduced"],
            ["bq_empty", "Scoped filter returned zero rows", "limited  ·  score ≤ 45"],
            ["weak_orientation", "No federated rows or scored docs", "low  ·  score 20"],
            ["curated_v1", "Product / meta short-circuit", "strong  ·  not triangulation"],
        ],
        "footnote": "Ceilings protect decision-makers: fluent prose cannot outrun a failed warehouse query.",
        "notes": "Ask ADZA can be useful and humble. It must not be loud and empty.",
    },
    # ------------------------------------------------------------------ Act 5
    {
        "kind": "section",
        "title": "Act 5  ·  How we build and teach",
        "subtitle": "Ingest, languages, eval, principles, exercises",
        "notes": "Close on culture: we do not prompt until it looks nice.",
    },
    {
        "kind": "two_column",
        "title": "Ingest vs query",
        "left_title": "Offline",
        "left": [
            "Drive / local files → preprocess → JSONL chunks → Qdrant",
            "Stable chunk IDs, content hashes, per-corpus profiles",
            "Reindex is a release: bump INGEST_VERSION",
        ],
        "right_title": "Online",
        "right": [
            "LangGraph in chatbot/graph.py",
            "CLI, Streamlit inspector, FastAPI POST /query",
            "If retrieval is bad, do not start by swapping the LLM",
        ],
        "footnote": "Check encoder dim, chunking, payload indexes, bind completeness — then the prompt.",
        "notes": "Streamlit pipeline debug is a teaching tool: show decomposition, SQL, corpora, generation plan live if you have five minutes.",
    },
    {
        "kind": "bullets",
        "title": "Multilingual is a product requirement",
        "bullets": [
            "E5 multilingual embeddings with query: / passage: prefixes",
            "Country aliases, code-mixed questions, typos (“prize od maize”)",
            "Product answers exist in English, French, Swahili, Pidgin",
            "Farmer persona: bullets, not section headings",
        ],
        "footnote": "A geo alias miss (Côte d’Ivoire vs Ivory Coast) is a retrieval bug, not a “language model” bug.",
        "notes": "Robustness is part of the product. The typo gold trace is the teaching test.",
    },
    {
        "kind": "bullets",
        "title": "How we know it works",
        "bullets": [
            "Gold traces: canonical questions with expected class, measure, job",
            "Property tests: reasoner invariants, bind contracts, generation admission",
            "Retrieval eval: recall@k per corpus",
            "Streamlit inspector: decomposition, SQL, corpora, generation plan visible to builders",
        ],
        "footnote": "Examples: Ghana rice trend; Nigeria maize price; unsupported Malawi districts; prize od maize.",
        "notes": "We do not “prompt until it looks nice.” The inspector is how you teach the graph to new contributors.",
    },
    {
        "kind": "bullets",
        "title": "Design principles — take-home cheat sheet",
        "bullets": [
            "Two retrieval legs, one merge — documents and tables are peers",
            "Bind first, SQL second, NL2SQL last — models propose, compilers commit",
            "Domain taxonomy is routing; task mode before generation; plan shape after evidence",
            "Fail soft, never fail fake — clarify, typed gap, insufficient context",
            "Persona is generation, not a different database; ACF Path B scores cited evidence, not the retrieval dump",
            "Never replace the user’s question as the only embed string",
        ],
        "footnote": "Grow in layers. Working product first, then the next capability.",
        "notes": "These are the OpenTrace AGENTS.md instincts applied to RAG: no compatibility shims, no stopgaps, modular concerns.",
    },
    {
        "kind": "table",
        "title": "Try this — which leg should win?",
        "headers": ["#", "Question", "Expect"],
        "rows": [
            ["1", "What was the retail price of maize in Nigeria in 2022?", "BQ  ·  PRC"],
            ["2", "What does research say about conservation agriculture in the Sahel?", "Vector  ·  academic + reports"],
            ["3", "Which districts in Malawi had the highest rainfall in 2023?", "Gap  ·  unsupported grain"],
            ["4", "what is the prize od maize in nigeria in 2022", "Same as 1 — if understanding works"],
        ],
        "footnote": "Question 4 is the teaching test: robustness is part of the product.",
        "notes": "Give the room a minute on 1–3 before 4. Then ask how they would abuse the system — and which guardrail stops it.",
    },
    {
        "kind": "two_column",
        "title": "Q&A  ·  run of show",
        "left_title": "Leave these up",
        "left": [
            "Why not let the LLM write all SQL?",
            "When should news outrank FAO?",
            "What if the user asks below our geographic grain?",
            "How would you abuse this — and which guardrail stops it?",
        ],
        "right_title": "Timing",
        "right": [
            "0–5 min  ·  product and audience",
            "5–12  ·  RAG teaching + two legs",
            "12–22  ·  vector unit + Ghana",
            "22–30  ·  warehouse, trust, eval",
            "30–35  ·  principles, exercises, Q&A",
            "12-min cut: product, naive vs production, architecture, bind, Ghana, ACF, principles",
        ],
        "footnote": f"Regenerate: python scripts/generate_ask_adza_teaching_pptx.py   ·   v{DOC_VERSION}   ·   ml/rag/docs/",
        "notes": (
            "We are not wrapping an LLM around agriculture. We are building a retrieval and honesty system "
            "that an LLM is allowed to narrate.\n\n"
            "Deeper: ARCHITECTURE.md, Streamlit, python -m ml.rag.run, POST /query."
        ),
    },
]
