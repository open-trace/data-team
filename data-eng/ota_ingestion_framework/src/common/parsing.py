# Author: Daniel Wondyfiraw
# Updated: 2026-10-30
"""Parse source responses; keep request handling in fetching_method.py."""

import re

from src.common.source_testing import Page, ProbeError

POSTBACK = re.compile(r"__doPostBack\(\s*(['\"])(.*?)\1\s*,\s*(['\"])(.*?)\3\s*\)")


def html_page(body):
    return Page(body.decode("utf-8-sig", errors="replace"))


def html_scripts(body):
    """Read script references and inline text without executing JavaScript."""
    from html.parser import HTMLParser

    class Scripts(HTMLParser):
        def __init__(self):
            super().__init__(convert_charrefs=True)
            self.blocks = []
            self.current = None

        def handle_starttag(self, tag, attrs):
            if tag == "script":
                self.current = {"src": dict(attrs).get("src"), "text": ""}
                self.blocks.append(self.current)

        def handle_data(self, text):
            if self.current is not None:
                self.current["text"] += text

        def handle_endtag(self, tag):
            if tag == "script":
                self.current = None

    parser = Scripts()
    parser.feed(body.decode("utf-8-sig", errors="replace"))
    return parser.blocks


def parse_naerls_ajax(body, page_url):
    """Read NAERLS's literal jQuery AJAX contracts; do not evaluate JavaScript."""
    from urllib.parse import urljoin, urlsplit

    contracts = {}
    for script in html_scripts(body):
        text = script["text"]
        for start in re.finditer(r"\$\.ajax\s*\(\s*\{", text):
            # Find the complete options object, ignoring braces inside strings
            # and comments. A partial regex match could mix two AJAX calls.
            offset = start.end() - 1
            depth, quote, escaped, comment = 0, None, False, None
            end = None
            index = offset
            while index < len(text):
                char = text[index]
                pair = text[index:index + 2]
                if comment:
                    if comment == "//" and char == "\n":
                        comment = None
                    elif comment == "/*" and pair == "*/":
                        comment = None
                        index += 1
                elif quote:
                    if escaped:
                        escaped = False
                    elif char == "\\":
                        escaped = True
                    elif char == quote:
                        quote = None
                elif pair in {"//", "/*"}:
                    comment = pair
                    index += 1
                elif char in "\"'`":
                    quote = char
                elif char == "{":
                    depth += 1
                elif char == "}":
                    depth -= 1
                    if depth == 0:
                        end = index + 1
                        break
                index += 1
            if end is None:
                continue
            block = text[offset:end]
            url = re.search(r"\burl\s*:\s*(['\"])([^'\"]+)\1\s*[,}]", block)
            method = re.search(r"\b(?:type|method)\s*:\s*(['\"])(GET|POST)\1", block, re.I)
            payload = re.search(r"\bdata\s*:\s*\{([^{}]*)\}", block)
            if not (url and method and payload):
                continue
            target = urljoin(page_url, url.group(2))
            if urlsplit(target).scheme != "https" or urlsplit(target).netloc != urlsplit(page_url).netloc:
                continue
            if re.search(r"\bcontentType\s*:", block):
                # The shared client sends form data; other encodings need review.
                continue
            keys = re.findall(r"(?:^|,)\s*['\"]?([A-Za-z_][\w\[\]-]*)['\"]?\s*:", payload.group(1))
            # Diagnostics expose only public control names and operation names,
            # never CSRF expressions or complete JavaScript/body contents.
            public = {"filter1", "filter2", "market_id", "agricproduct", "priceregime"}
            input_shapes = {}
            for key in keys:
                if key not in public:
                    continue
                expression = re.search(r"(?:^|,)\s*['\"]?" + re.escape(key) + r"['\"]?\s*:\s*([A-Za-z_]\w*)\s*(?:,|$)", payload.group(1))
                variable = expression.group(1) if expression else None
                assignments = list(re.finditer(r"\b" + re.escape(variable) + r"\s*=\s*([^;\n]+)", text[:offset])) if variable else []
                assignment = assignments[-1].group(1) if assignments else ""
                input_shapes[key] = {
                    "uses_same_named_variable": variable == key,
                    "assignment_operations": sorted(set(re.findall(r"\.\s*(val|join|toString|map|serialize|serializeArray)\s*\(", assignment))),
                }
            bindings = []
            for match in re.finditer(
                r"\$\(\s*['\"]#([\w-]+)['\"]\s*\)\s*\.html\(\s*([\w]+)(?:\.([\w]+)|\[['\"]([\w]+)['\"]\])?\s*\)", block
            ):
                bindings.append({"selector": match.group(1), "response_key": match.group(3) or match.group(4)})
            contract = {"url": target, "method": method.group(2).upper(), "keys": keys, "bindings": bindings,
                        "public_input_shapes": input_shapes}
            route = urlsplit(target).path.rstrip("/").rsplit("/", 1)[-1]
            if route in contracts and contracts[route] != contract:
                raise ProbeError("NAERLS has ambiguous AJAX definitions for a dropdown request.")
            contracts[route] = contract
    return contracts


def parse_naerls_options(body, contract):
    """Read actual select markup or script-bound HTML option fragments."""
    import json

    text = body.decode("utf-8-sig", errors="replace")
    try:
        result = json.loads(text)
    except ValueError:
        result = text
    fragments = [(None, result)] if isinstance(result, str) else []
    field_names = {"filter1", "filter2", "market_id", "agricproduct", "priceregime"}
    for binding in contract["bindings"]:
        fragment = result
        if binding["response_key"]:
            fragment = result.get(binding["response_key"]) if isinstance(result, dict) else None
        if isinstance(fragment, str):
            fragments.append((binding["selector"], fragment))
    options = {}
    for selector, fragment in fragments:
        if selector in field_names and "<option" in fragment.lower() and "<select" not in fragment.lower():
            fragment = f'<select name="{selector}">{fragment}</select>'
        page = html_page(("<form>" + fragment + "</form>").encode())
        for form in page.forms:
            for select in form["selects"]:
                name = select["name"].removesuffix("[]")
                if name not in field_names:
                    continue
                choices = []
                for option in select["options"]:
                    label = " ".join(option["text"]).strip()
                    value = option["value"] if option["value"] is not None else label
                    if not str(value).strip() or re.search(r"^(?:[-\s]*select\b|[-\s]*choose\b)", label, re.I):
                        continue
                    choices.append({"label": label, "value": value})
                options[name] = choices
    return options


def postback_actions(page):
    """Read actions actually present in links; never invent a request target."""
    actions = []
    for link in page.links:
        match = POSTBACK.search(link["href"])
        if match:
            actions.append({"target": match.group(2), "argument": match.group(4)})
    return actions


def parse_tshwane_products(body):
    """Read the actual product search listing, not package/statistics tables."""
    page = html_page(body)
    products, seen = [], set()
    for row in page.rows:
        for href in row.links:
            match = POSTBACK.search(href)
            if not match:
                continue
            target, argument = match.group(2), match.group(4)
            if not target.endswith("$GridView1") or not re.fullmatch(r"Select\$\d+", argument):
                continue
            if len(row.cells) != 2 or not row.cells[1].strip():
                raise ProbeError("Product selection row has an unexpected layout.")
            identity = (target, argument)
            if identity in seen:
                continue
            seen.add(identity)
            products.append({"product_name_source": row.cells[1].strip(), "event_target": target, "event_argument": argument})
    if not products:
        raise ProbeError("The empty search did not expose a selectable product catalogue.")
    paging = [a for a in postback_actions(page) if a["target"].endswith("$GridView1") and a["argument"].startswith("Page$")]
    return products, paging


def parse_tshwane_packages(body):
    """Read the package ID and source attributes from selectable GridView2 rows."""
    page = html_page(body)
    packages, seen = [], set()
    for row in page.rows:
        for href in row.links:
            match = POSTBACK.search(href)
            if not match or not match.group(2).endswith("$GridView2") or not re.fullmatch(r"Select\$\d+", match.group(4)):
                continue
            if len(row.cells) != 7:
                raise ProbeError("Package selection row has an unexpected layout.")
            key = (match.group(2), match.group(4))
            if key in seen:
                continue
            seen.add(key)
            packages.append(dict(zip(
                ["product_name_source", "package_id_source", "grade_source", "container_source", "mass_source", "count_source"], row.cells[1:]
            ), event_target=key[0], event_argument=key[1]))
    if not packages:
        raise ProbeError("No selectable packages found; empty coverage cannot be assumed successful.")
    paging = [a for a in postback_actions(page) if a["target"].endswith("$GridView2") and a["argument"].startswith("Page$")]
    return packages, paging


def parse_tshwane_statistics(body, product, package, *, source_id, retrieved_at):
    """Preserve source fields and dates; never guess missing currency or mass units."""
    import hashlib
    import json
    from datetime import datetime
    from decimal import Decimal, InvalidOperation

    from src.common.source_testing import normalise, observation_date

    page = html_page(body)
    fields = {}
    for row in page.rows:
        if len(row.cells) == 2:
            key = normalise(row.cells[0])
            if key in fields and fields[key] != row.cells[1]:
                raise ProbeError("Statistics contain conflicting repeated field labels.")
            fields[key] = row.cells[1]
    match = re.search(r"Data as at\s+(\d{1,2}\s+\w+\s+\d{4})", " ".join(page.text), re.I)
    observed = observation_date(match.group(1)) if match else None
    if observed is None:
        raise ProbeError("Statistics have no parseable source observation date.")
    if normalise(fields.get("product")) != normalise(product["product_name_source"]):
        raise ProbeError("Statistics product differs from the requested product.")
    for field, expected in (("grade", "grade_source"), ("container", "container_source"), ("count", "count_source")):
        if normalise(fields.get(field)) != normalise(package[expected]):
            raise ProbeError(f"Statistics {field} differs from the selected package.")

    issues = []

    def number(key, required=False):
        raw = fields.get(key, "").strip()
        if not raw:
            if required:
                raise ProbeError(f"Statistics are missing {key}.")
            return None
        try:
            value = Decimal(raw.replace(",", ""))
        except InvalidOperation:
            raise ProbeError(f"Statistics {key} is not numeric.") from None
        if not value.is_finite():
            raise ProbeError(f"Statistics {key} must be finite.")
        if value < 0:
            issues.append(f'Negative source value retained for review: {key}.')
        return str(value)

    mass = number("mass")
    if mass is not None:
        try:
            if Decimal(mass) != Decimal(package["mass_source"].replace(",", "")):
                raise ProbeError("Statistics mass differs from the selected package.")
        except InvalidOperation:
            raise ProbeError("Selected package mass is not numeric.") from None
    low, average, high = (number(key, required=True) for key in ("lowestprice", "averageprice", "highestprice"))
    if not Decimal(low) <= Decimal(average) <= Decimal(high):
        issues.append("Price ordering is inconsistent: expected low <= average <= high.")
    age = (datetime.fromisoformat(retrieved_at).date() - observed).days
    if age < 0:
        issues.append("Observation date is in the future relative to retrieval.")
    identity = {"source_id": source_id, "observed_at": observed.isoformat(), "package_id": package["package_id_source"], "product": fields.get("product"), "grade": fields.get("grade"), "container": fields.get("container"), "mass": mass, "count": fields.get("count"), "province": fields.get("province")}
    record_id = hashlib.sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest()
    return {
        "observation_id": record_id, "source_id": source_id, "observed_at": observed.isoformat(),
        "retrieved_at_utc": retrieved_at, "market": "Tshwane Fresh Produce Market",
        "commodity_source": fields["product"], "package_id_source": package["package_id_source"],
        "grade_source": fields.get("grade"), "container_source": fields.get("container"),
        "province_source": fields.get("province"), "count_source": fields.get("count") or None,
        "package_mass_source": mass, "mass_unit": None, "currency": None, "price_basis": None,
        "low_price": low, "average_price": average, "high_price": high,
        "quantity_sold": number("quantitysold"), "stock": number("quantityonhand"),
        "opening_balance": number("openingbalance"), "sales_value": number("valueofsales"),
        "total_mass_source": number("totalmass"), "voided_source": fields.get("voided") or None,
        "rand_per_kg_source": number("randperkg"), "source_fields": fields,
        "age_days": age, "quality_issues": issues,
    }
