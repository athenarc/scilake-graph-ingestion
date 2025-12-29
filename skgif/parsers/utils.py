def add_multilingual_fields(target: dict, source: dict, base_key: str) -> None:
    """Populate target with multilingual values from source.
    - If source contains key 'none', set target[base_key] to the first element if list, else the value.
    - For other language keys, set target[f"{base_key}_{lang}"] similarly.
    """
    if not source:
        return

    if "none" in source:
        vals = source["none"]
        if isinstance(vals, list) and vals:
            target[base_key] = vals[0]
        else:
            target[base_key] = vals

    for lang, vals in source.items():
        if lang == "none":
            continue
        if isinstance(vals, list) and vals:
            target[f"{base_key}_{lang}"] = vals[0]
        else:
            target[f"{base_key}_{lang}"] = vals


def clean_empty(value):
    """Recursively remove null/empty values from dicts and lists.
    Rules:
    - Drop keys with value None, "", empty list [], or empty dict {}.
    - For lists, clean each element and drop elements that become empty; drop list if ends empty.
    - Keep falsy but meaningful values like 0 or False.
    """
    if isinstance(value, dict):
        cleaned = {}
        for k, v in value.items():
            cv = clean_empty(v)
            if cv is None:
                continue
            if cv == "":
                continue
            if isinstance(cv, (list, dict)) and not cv:
                continue
            cleaned[k] = cv
        return cleaned
    if isinstance(value, list):
        cleaned_list = []
        for item in value:
            ci = clean_empty(item)
            if ci is None or ci == "":
                continue
            if isinstance(ci, (list, dict)) and not ci:
                continue
            cleaned_list.append(ci)
        return cleaned_list
    return value
