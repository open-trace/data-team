import socket
import pytest
import opentrace_ontology as oo

@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    def guard(*a, **k): raise AssertionError("network access attempted")
    monkeypatch.setattr(socket, "socket", guard)
    monkeypatch.setattr(socket, "create_connection", guard)

def ids(text, **kw):
    return [m.ontology_id for m in oo.lookup(text, **kw)]

@pytest.mark.parametrize("q", ["casssava", "cassva", "casava", "cassavva"])
def test_misspellings_resolve_to_cassava(q):
    assert "OT:CROP:cassava" in ids(f"current state of {q} in Nigeria")

def test_normalize_query_fixes_typo():
    assert oo.normalize_query("What is the current state of casssava in Nigeria") == \
        "What is the current state of cassava in Nigeria"

@pytest.mark.parametrize("q,lang", [("manioc", "fra"), ("mandioca", "por"), ("muhogo", "swh"), ("rogo", "hau"),
                                    ("akpu", "ibo"), ("ege", "yor"), ("bankye", "twi"), ("mangahazo", "mlg"), ("imyumbati", "kin")])
def test_cassava_multilingual(q, lang):
    assert ids(f"price of {q}", lang=lang)[-1] == "OT:CROP:cassava"

@pytest.mark.parametrize("q,lang,expected", [("masara", "hau", "OT:CROP:maize"), ("agbado", "yor", "OT:CROP:maize"),
    ("gyada", "hau", "OT:CROP:groundnut"), ("shinkafa", "hau", "OT:CROP:rice"), ("iresi", "yor", "OT:CROP:rice"),
    ("doya", "hau", "OT:CROP:yam"), ("mahindi", "swh", "OT:CROP:maize"), ("ng'ombe", "swh", "OT:ANIMAL:cattle"),
    ("kaza", "hau", "OT:ANIMAL:chicken"), ("ji", "ibo", "OT:CROP:yam")])
def test_african_language_names(q, lang, expected):
    assert expected in ids(q, lang=lang)

def test_short_local_word_not_matched_without_lang():
    assert "OT:CROP:yam" not in ids("ji price")      # Igbo 'ji' only unlocked with lang='ibo'


@pytest.mark.parametrize(
    "q,lang,expected",
    [
        ("ji", "ibo", "OT:CROP:yam"),
        ("oka", "yor", "OT:CROP:maize"),
        ("ata", "yor", "OT:CROP:pepper"),
    ],
)
def test_short_ambiguous_locals_need_lang(q, lang, expected):
    """len<=3 locals must not fire on English questions without lang=."""
    english = f"what is the data for {q}"
    assert expected not in ids(english)
    assert expected not in ids(f"{q} price")
    assert expected in ids(q, lang=lang)
    assert expected in ids(english, lang=lang)

def test_country_names_are_not_entities():
    assert ids("bei ya muhogo Tanzania") == ["OT:CROP:cassava"]

def test_products_and_derivation():
    assert ids("garri price in Lagos") == ["OT:PRODUCT:garri"]
    d = oo.derived_products("OT:CROP:cassava")
    assert "OT:PRODUCT:garri" in d and "OT:PRODUCT:fufu" in d
    assert "OT:PRODUCT:palm_kernel_cake" in oo.derived_products("OT:CROP:oil_palm")

def test_multiword_longest_match():
    assert ids("fall armyworm in maize") == ["OT:DISEASE:fall_armyworm", "OT:CROP:maize"]
    assert ids("palm kernel cake for feed")[0] == "OT:PRODUCT:palm_kernel_cake"

def test_codes():
    assert oo.to_faostat("OT:CROP:cassava") == ["125"]
    assert oo.to_faostat("OT:CROP:maize")
    assert "agrovoc" in oo.codes("OT:CROP:cassava")

def test_expand_for_qdrant():
    terms = {t.lower() for t in oo.expand("OT:CROP:cassava")}
    assert {"cassava", "manioc", "mandioca", "garri"} <= terms

def test_ambiguity_reported_and_country_disambiguates():
    m = oo.lookup("beans")[0]
    assert m.ambiguous and {"OT:CROP:common_bean", "OT:CROP:cowpea"} <= set(m.candidates)
    assert oo.lookup("beans", country="NGA")[0].ontology_id == "OT:CROP:cowpea"

def test_canonicalize_all():
    assert oo.normalize_query("farashin rogo", lang="hau", canonicalize="all") == "farashin cassava"

def test_no_false_fuzzy_on_common_words():
    assert ids("what is the current state of the market today") in ([], ["OT:SERVICE:markets"])


# Priority vernacular matrix (africa_core + enrichment). Each ID must resolve in lang.
@pytest.mark.parametrize(
    "oid,q,lang",
    [
        ("OT:CROP:cassava", "rogo", "hau"),
        ("OT:CROP:cassava", "ege", "yor"),
        ("OT:CROP:cassava", "akpu", "ibo"),
        ("OT:CROP:cassava", "muhogo", "swh"),
        ("OT:CROP:cassava", "manioc", "fra"),
        ("OT:CROP:maize", "masara", "hau"),
        ("OT:CROP:maize", "agbado", "yor"),
        ("OT:CROP:maize", "mahindi", "swh"),
        ("OT:CROP:rice", "shinkafa", "hau"),
        ("OT:CROP:yam", "doya", "hau"),
        ("OT:CROP:yam", "ji", "ibo"),
        ("OT:CROP:groundnut", "gyada", "hau"),
        ("OT:CROP:cocoa", "kokko", "hau"),
        ("OT:CROP:coffee", "kofi", "yor"),
        ("OT:PRODUCT:garri", "gari", "hau"),
        ("OT:ANIMAL:cattle", "shanu", "hau"),
        ("OT:ANIMAL:goat", "ewure", "yor"),
    ],
)
def test_vernacular_matrix(oid, q, lang):
    assert oid in ids(q, lang=lang)
