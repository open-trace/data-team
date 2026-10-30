# Author: Daniel Wondyfiraw
# Updated: 2026-10-30
"""Shared fetching functions used by the single collector."""

import json


def fetch_json_records(
    client,
    url,
    *,
    params=None,
    records_path="",
    record_shape="list",
    save_response=None,
):
    """Request JSON and select its configured list of records."""

    body, final_url = client.get(
        "json_api",
        url,
        params=params,
    )
    if save_response is not None:
        save_response(body)

    try:
        data = json.loads(body)
    except (ValueError, UnicodeError) as error:
        raise ValueError(
            "Expected JSON, but the response was not valid JSON."
        ) from error

    # Example: "latestPrices" selects data["latestPrices"].
    # A dotted path such as "data.results" selects nested objects.
    records = data

    for key in records_path.split(".") if records_path else []:
        if not isinstance(records, dict) or key not in records:
            raise ValueError(
                f"Configured JSON record path is missing: {records_path}"
            )

        records = records[key]

    if record_shape == "object":
        if not isinstance(records, dict) or not records or "error" in records:
            raise ValueError("Expected one nonempty JSON record object without an API error.")
        records = [records]
    elif record_shape != "list":
        raise ValueError("Unsupported JSON record shape.")
    if not isinstance(records, list):
        raise ValueError("The selected JSON value is not a record list.")

    if not all(isinstance(record, dict) for record in records):
        raise ValueError("Each JSON record must be an object.")

    return records, final_url


def fetch_durban_records(client, url, *, save_response):
    """Read the initial daily table without filtering crops or rewriting values."""
    from src.common.parsing import html_page
    from src.common.source_testing import ProbeError, normalise

    body, final_url = client.get("durban_daily_table", url)
    save_response(body)
    page = html_page(body)
    required = {normalise(label) for label in
                ("Weight (Kg)", "Low Price", "High Price", "Average Price", "Date")}
    for table in page.tables:
        for index, row in enumerate(table):
            columns = [normalise(cell) for cell in row.cells]
            if not required.issubset(columns):
                continue
            if len(set(columns)) != len(columns) or not all(columns):
                raise ProbeError("Daily table has empty or repeated column names.")
            records = []
            for item in table[index + 1:]:
                if not any(cell.strip() for cell in item.cells) or item.cells == row.cells:
                    continue
                if len(item.cells) != len(columns):
                    raise ProbeError("Daily table row width differs from its headers; original HTML saved for review.")
                records.append(dict(zip(columns, item.cells)))
            return records, final_url, dict(zip(columns, row.cells))
    raise ProbeError("Expected Durban daily price table was not found; original HTML saved for review.")


def fetch_cape_town_records(client, url, *, save_response):
    """Fetch the known daily table and preserve its explicit page-level date."""
    import re
    from src.common.parsing import html_page
    from src.common.source_testing import ProbeError, table_records, normalise, observation_date

    body, final_url = client.get("cape_town_daily_table", url)
    save_response(body)
    page = html_page(body)
    headers, records = table_records(page, ["DESC", "Container", "Mass", "Grade", "Low Price", "High Price", "Average Price"])
    match = re.search(r"Daily Statistical Prices\s*(?:for\s*)?[:\-]?\s*(\d{1,2}/\d{1,2}/\d{4})", " ".join(page.text), re.I)
    observed = observation_date(match.group(1)) if match else None
    if observed is None:
        raise ProbeError("Source observation date missing; no retrieval date was substituted.")
    for record in records:
        record["source_observation_date"] = observed.isoformat()
    return records, final_url, dict(zip([normalise(h) for h in headers], headers))


def fetch_naerls_selections(client, body, page, url):
    """Follow dropdown stages, checking at most one market per target state.

    Each route/method/key set must also exist in this run's inline JavaScript.
    Option IDs come from actual responses. No JS execution or endpoint guessing.
    """
    import re

    from src.common.parsing import parse_naerls_ajax, parse_naerls_options, html_page
    from src.common.source_testing import ProbeError, form_data, normalise, display_url

    contracts = parse_naerls_ajax(body, url)
    form = page.form("agricproduct")
    token = form_data(form).get("_token")
    if not token:
        raise ProbeError("NAERLS session form token is missing.")
    geography = next((select for select in form["selects"] if select["name"] == "filter1"), None)
    states = next((option for option in geography["options"]
                   if normalise(" ".join(option["text"])) == "states"), None) if geography else None
    if states is None:
        raise ProbeError("The current NAERLS form has no States option.")
    values = {"filter1": states["value"] if states["value"] is not None else " ".join(states["text"])}
    controls = {select["name"].removesuffix("[]"): select for select in form["selects"]}
    client.evidence.check("naerls_select_controls", "PASS",
                          "Array/scalar selection uses the actual HTML multiple attribute, not the field-name suffix.",
                          controls={name: {"id": select["id"], "multiple": select["multiple"]}
                                    for name, select in controls.items()
                                    if name in {"filter1", "filter2", "market_id", "agricproduct", "priceregime"}})

    def ajax_value(field, choice):
        if field not in controls:
            raise ProbeError(f"NAERLS dropdown {field}: actual select control is missing.")
        return [choice["value"]] if controls[field]["multiple"] else choice["value"]

    def request_options(route, response_field, known, context):
        contract = contracts.get(route)
        if contract is None or not contract["keys"] or any(key not in known for key in contract["keys"]):
            raise ProbeError(f"NAERLS dropdown {route}: no supported current script contract. No request was guessed.")
        for key, shape in contract["public_input_shapes"].items():
            if not shape["uses_same_named_variable"] or shape["assignment_operations"] != ["val"]:
                raise ProbeError(f"NAERLS dropdown {route}: input {key} uses unsupported JavaScript conversion; no encoding was guessed.")
        # jQuery serializes array-valued form parameters with [] suffixes.
        payload = {(key + "[]" if isinstance(known[key], list) and not key.endswith("[]") else key): known[key]
                   for key in contract["keys"]}
        old_headers = dict(client.session.headers)
        try:
            client.session.headers.update({"X-Requested-With": "XMLHttpRequest", "Referer": url})
            response, _ = client.request("naerls_" + route, contract["method"], contract["url"],
                                         data=payload if contract["method"] == "POST" else None,
                                         params=payload if contract["method"] == "GET" else None)
        finally:
            client.session.headers.clear()
            client.session.headers.update(old_headers)
        options = parse_naerls_options(response, contract)
        choices = options.get(response_field, [])
        try:
            response_value = json.loads(response)
            response_shape = {"format": "json", "root_type": type(response_value).__name__,
                              "keys": sorted(response_value) if isinstance(response_value, dict) else []}
        except (ValueError, UnicodeError):
            response_shape = {"format": "html_or_text"}
        text = response.decode("utf-8-sig", errors="replace")
        response_page = html_page(response)
        response_shape.update(
            bytes_received=len(response), empty_body=not text.strip(),
            option_tags=len(re.findall(r"<option\b", text, re.I)),
            select_tags=len(re.findall(r"<select\b", text, re.I)),
            escaped_option_tags=len(re.findall(r"&lt;option\b", text, re.I)),
            form_count=len(response_page.forms), table_count=len(response_page.tables),
            visible_text_characters=len(" ".join(response_page.text)),
        )
        public_payload = {key: {"type": type(value).__name__, "item_count": len(value) if isinstance(value, list) else 1}
                          for key, value in payload.items() if key.removesuffix("[]") != "_token"}
        client.evidence.check("naerls_" + route + ".options", "PASS" if choices else "NOT_TESTED",
                              "Options parsed from the actual dropdown response; they do not verify any price observations.",
                              method=contract["method"], url=display_url(contract["url"]),
                              response_field=response_field, option_count=len(choices), options=choices,
                              response_shape=response_shape, parsed_select_fields=sorted(options),
                              script_response_bindings=contract["bindings"],
                              public_payload_shapes=public_payload,
                              script_public_input_shapes=contract["public_input_shapes"], query_context=dict(context))
        return choices

    base = {"_token": token, "filter1": values["filter1"]}
    choices = request_options("getstatesorzones", "filter2", base, {})
    aliases = {"fct", "abuja", "abujafct", "federalcapitalterritory", "edo", "benue", "delta"}
    targets = [choice for choice in choices if normalise(choice["label"]) in aliases][:4]
    if not targets:
        raise ProbeError("NAERLS returned no recognised target-state options; no unrelated state was substituted.")
    attempted = []
    for state in targets:
        context = {"state_option": state["label"]}
        attempted.append(state["label"])
        known = dict(base, filter2=ajax_value("filter2", state))
        markets = request_options("getstatesorzones2", "market_id", known, context)
        if not markets:
            continue
        market = markets[0]
        context["market_option"] = market["label"]
        known["market_id"] = ajax_value("market_id", market)
        products = request_options("getagricproducts", "agricproduct", known, context)
        if not products:
            continue
        product = products[0]
        context["product_option"] = product["label"]
        known["agricproduct"] = ajax_value("agricproduct", product)
        regimes = request_options("getpriceregime", "priceregime", known, context)
        if not regimes:
            continue
        regime = regimes[0]
        values.update({"filter2[]": [state["value"]], "market_id[]": [market["value"]],
                       "agricproduct": product["value"], "priceregime": regime["value"]})
        client.evidence.check("naerls_query_selection", "PASS",
                              "First usable target-state selection chosen. At most one market per attempted state was checked; none of these options proves price coverage.",
                              selected_options={**context, "price_type_option": regime["label"]},
                              states_attempted=attempted)
        return values
    raise ProbeError("NAERLS returned no usable product/price-type selection for the first market in each attempted target state. Other markets and price coverage remain unverified.")


def fetch_html_form(client, name, url):
    """Open a form in the client's existing session and require ASP.NET state."""
    from src.common.parsing import html_page

    body, final_url = client.get(name, url)
    page = html_page(body)
    page.form("__VIEWSTATE")
    return body, page, final_url


def submit_html_form(client, name, page, current_url, values):
    """Post current hidden state to the actual same-origin form action."""
    from urllib.parse import urljoin, urlsplit

    from src.common.parsing import html_page
    from src.common.source_testing import ProbeError, form_data, secure_url

    form = page.form("__VIEWSTATE")
    target = secure_url(urljoin(current_url, form["action"]))
    if urlsplit(target).netloc != urlsplit(current_url).netloc:
        raise ProbeError("Cross-host form submission is not permitted.")
    if form["method"] != "POST":
        raise ProbeError("Expected a POST form; source layout has changed.")
    data = form_data(form)
    data.update(values)
    body, final_url = client.request(name, "POST", target, data=data)
    return body, html_page(body), final_url


def fetch_tshwane_catalogue(client, url, save_response):
    """Discover products through an empty Contains search, without a crop filter."""
    from src.common.parsing import parse_tshwane_products
    from src.common.source_testing import ProbeError

    body, page, final_url = fetch_html_form(client, "search_form", url)
    save_response("search_form.html", body)
    form = page.form("__VIEWSTATE")
    fields = {item.get("name") for item in form["inputs"] + form["selects"]}
    values = {
        "ctl00$ContentPlaceHolder1$ddlMonth": "Contains",
        "ctl00$ContentPlaceHolder1$txtItem": "",
        "ctl00$ContentPlaceHolder1$BtnSubmit": "Submit Query",
    }
    if not set(values).issubset(fields):
        raise ProbeError("Expected product search controls are missing.")
    body, _, final_url = submit_html_form(client, "all_products", page, final_url, values)
    save_response("product_search.html", body)
    products, paging = parse_tshwane_products(body)
    return {"products": products, "pagination_actions": paging, "final_url": final_url}


def fetch_tshwane_prices(client, url, save_response, *, source_id, max_products=None):
    """Yield catalogue, package, record and failure events using fresh run form state.

    All returned selections are attempted until the source time budget is reached.
    Paging controls are reported as gaps rather than silently ignored.
    """
    import time
    from datetime import datetime, timezone

    from src.common.parsing import html_page, parse_tshwane_packages, parse_tshwane_statistics
    from src.common.source_testing import ProbeError

    saved = {}

    def capture(name, body):
        saved[name] = body
        return save_response(name, body)

    catalogue = fetch_tshwane_catalogue(client, url, capture)
    products = catalogue["products"]
    listing = html_page(saved["product_search.html"])
    yield {"kind": "catalogue", "products": products, "pagination": catalogue["pagination_actions"]}
    for index, product in enumerate(products):
        if max_products is not None and index >= max_products:
            yield {"kind": "stopped", "reason": "Configured product limit reached.", "unvisited_products": len(products) - index}
            return
        if time.monotonic() >= client.deadline:
            yield {"kind": "stopped", "reason": "Source time budget exhausted.", "unvisited_products": len(products) - index}
            return
        product_key = f"product_{index:04d}"
        yield {"kind": "product_started", "product": product["product_name_source"]}
        try:
            body, package_page, package_url = submit_html_form(client, product_key, listing, catalogue["final_url"], {"__EVENTTARGET": product["event_target"], "__EVENTARGUMENT": product["event_argument"]})
            save_response(f"{product_key}_packages.html", body)
            packages, paging = parse_tshwane_packages(body)
            if any(p["product_name_source"].strip().casefold() != product["product_name_source"].strip().casefold() for p in packages):
                raise ProbeError("Returned package list belongs to a different product.")
            yield {"kind": "packages", "product": product["product_name_source"], "count": len(packages), "pagination": paging}
        except (ProbeError, ValueError, KeyError) as error:
            yield {"kind": "failure", "stage": "product", "product": product["product_name_source"], "error": str(error)}
            continue
        for package_index, package in enumerate(packages):
            if time.monotonic() >= client.deadline:
                yield {"kind": "stopped", "reason": "Source time budget exhausted.", "unvisited_products": len(products) - index - 1, "unvisited_packages": len(packages) - package_index}
                return
            package_key = f"{product_key}_package_{package_index:04d}"
            yield {"kind": "package_started"}
            try:
                body, _, _ = submit_html_form(client, package_key, package_page, package_url, {"__EVENTTARGET": package["event_target"], "__EVENTARGUMENT": package["event_argument"]})
                response_file = f"{package_key}_statistics.html"
                save_response(response_file, body)
                retrieved = datetime.now(timezone.utc).isoformat()
                record = parse_tshwane_statistics(body, product, package, source_id=source_id, retrieved_at=retrieved)
                record["response_file"] = response_file
                yield {"kind": "record", "record": record}
            except (ProbeError, ValueError, KeyError) as error:
                yield {"kind": "failure", "stage": "package", "product": product["product_name_source"], "package_id_source": package["package_id_source"], "error": str(error)}
        yield {"kind": "product_finished", "product": product["product_name_source"]}
