import os
import json
import gzip
import argparse
import re
import hashlib
from typing import Iterable, Dict, Any, Tuple


def iter_jsonl_gz(path: str) -> Iterable[Dict[str, Any]]:
    """
    Stream JSON objects from a .jsonl.gz file.

    Each line is expected to be a complete JSON object like the sample
    record shown by the user (with fields: doi, spaces, research_artifacts, mentions, ...).
    """
    with gzip.open(path, mode="rt", encoding="utf-8") as f:
        for line_no, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except Exception as e:
                print(f"Warning: failed to parse JSON on line {line_no} of {path}: {e}")
                continue


def slugify(value: str) -> str:
    """
    Build a simple slug for local identifiers from the "Research Artifact" label.
    """
    value = (value or "").strip().lower()
    # Replace spaces and separators with underscores, drop problematic chars
    for ch in [" ", "\t", "\n", "/", "\\", ":", ";", ","]:
        value = value.replace(ch, "_")
    while "__" in value:
        value = value.replace("__", "_")
    return value.strip("_") or "unnamed"


def parse_urls(raw: Any) -> list:
    """
    Parse the raw 'URLs' field from the input into a clean list of URLs.

    Example input:
        "ftp://cddis.gsfc.nasa.gov/gnss/data/daily (50.0%)\\nhttp://www.rtklib.com/ (50.0%)"

    Output:
        ["ftp://cddis.gsfc.nasa.gov/gnss/data/daily", "http://www.rtklib.com/"]
    """
    if not isinstance(raw, str):
        return []

    lines = raw.splitlines()
    urls: list = []

    # Pattern to strip trailing " (xx.x%)" or "(xx%)" from each line
    perc_pattern = re.compile(r"\s*\(\s*\d+(\.\d+)?%\s*\)\s*$")

    for line in lines:
        line = line.strip()
        if not line:
            continue
        # Remove trailing percentage in parentheses
        line = perc_pattern.sub("", line).strip()
        if line:
            urls.append(line)

    return urls


def prune_empty_fields(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Remove keys whose values are empty or null.

    We drop:
      - None
      - empty strings
      - empty lists
      - empty dicts

    Numeric zeros (0, 0.0) and False are **kept**.
    """
    if not isinstance(data, dict):
        return data

    cleaned: Dict[str, Any] = {}
    for k, v in data.items():
        if v is None:
            continue
        if isinstance(v, str) and v == "":
            continue
        if isinstance(v, (list, tuple)) and len(v) == 0:
            continue
        if isinstance(v, dict) and len(v) == 0:
            continue
        cleaned[k] = v
    return cleaned


def split_artifact_and_relation_fields(
    artifact: Dict[str, Any],
    paper_id: Any,
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """
    Given one item from `research_artifacts`, split fields into:
      - artifact_node_props: properties intrinsic to the ResearchArtifact node
      - relation_props: properties that are paper-specific and belong on the relation

    Node (ResearchArtifact) properties:
      - local_identifier (derived from paper_id + type + hash(label))
      - label                <- "Research Artifact"
      - type                 <- "Type"
      - licenses             <- "Licenses"
      - versions             <- "Versions"
      - urls                 <- "URLs"

    Relation properties (per-paper usage of the artifact):
      - research_artifact_score <- "Research Artifact Score"
      - owned                   <- "Owned"
      - owned_percentage        <- "Owned Percentage"
      - owned_score             <- "Owned Score"
      - reused                  <- "Reused"
      - reused_percentage       <- "Reused Percentage"
      - reused_score            <- "Reused Score"
      - citations               <- "Citations"
      - mentions_count          <- "Mentions Count"
    """
    label = artifact.get("Research Artifact") or ""
    art_type = artifact.get("Type")
    licenses = artifact.get("Licenses")
    versions = artifact.get("Versions")
    urls = parse_urls(artifact.get("URLs"))

    # Combine paper_id, type and a short hash of the label for a stable identifier.
    # paper_id is required and assumed non-null.
    type_part = (str(art_type).strip().lower() if art_type is not None else "")
    # Use a short hash of the original label (and type) to distinguish similar labels
    hash_input = f"{label}|{art_type}"
    label_hash = hashlib.md5(hash_input.encode("utf-8")).hexdigest()[:8]
    local_identifier = f"{paper_id}:{type_part}:{label_hash}"

    artifact_node_props = {
        "local_identifier": local_identifier,
        "label": label,
        "type": art_type,
        "licenses": licenses,
        "versions": versions,
        "urls": urls,
    }

    relation_props = {
        "research_artifact_score": artifact.get("Research Artifact Score"),
        "owned": artifact.get("Owned"),
        "owned_percentage": artifact.get("Owned Percentage"),
        "owned_score": artifact.get("Owned Score"),
        "reused": artifact.get("Reused"),
        "reused_percentage": artifact.get("Reused Percentage"),
        "reused_score": artifact.get("Reused Score"),
        # "Citations" deliberately ignored
        "mentions_count": artifact.get("Mentions Count"),
    }

    # Drop empty / null fields from both node and relation parts
    return prune_empty_fields(artifact_node_props), prune_empty_fields(relation_props)


def safe_space_name(space: str) -> str:
    """
    Make a filesystem-safe name for a given `spaces` value.
    """
    space = str(space or "").strip()
    if not space:
        return "unknown"
    for ch in ["/", "\\", " ", "\t"]:
        space = space.replace(ch, "_")
    while "__" in space:
        space = space.replace("__", "_")
    return space.strip("_")


def process_research_artifacts_file(
    input_path: str,
    output_dir: str,
) -> Dict[str, int]:
    """
    Parse a JSONL.GZ file where each record has at least:

      - doi                   : DOI of the paper (source id)
      - spaces                : a single space string (e.g. "cancer")
      - research_artifacts    : list of artifact dicts (as in the user sample)
      - mentions              : (ignored)

    and write **per-space** gzipped JSONL files, one line per
    (paper, research artifact) pair.

    Each output line has the shape:

      {
        "doi": "<paper DOI>",
        "space": "<space string>",
        "paper_id": <paper_id>,
        "input_path": "<input_path from record>",
        "artifact": { ... ResearchArtifact node properties ... },
        "relation": { ... relation (per-paper) properties ... }
      }

    The `artifact` object corresponds to a `:ResearchArtifact` node:
      - `local_identifier` (slug of "Research Artifact")
      - `label`
      - `cluster`
      - `type`
      - `licenses`
      - `versions`
      - `urls`

    The `relation` object corresponds to properties on the relationship
    from the Product (identified by DOI) to the ResearchArtifact.

    Returns:
        dict: mapping space -> number of artifact usages written
    """
    if not os.path.isfile(input_path):
        raise FileNotFoundError(f"Input file not found: {input_path}")

    os.makedirs(output_dir, exist_ok=True)

    space_files: Dict[str, gzip.GzipFile] = {}
    counts: Dict[str, int] = {}

    try:
        for rec in iter_jsonl_gz(input_path):
            doi = (rec.get("doi") or "").strip().lower()
            paper_id = rec.get("paper_id")
            space = rec.get("spaces")
            artifacts = rec.get("research_artifacts") or []

            if not doi or not space or paper_id is None or not isinstance(artifacts, list) or not artifacts:
                # Nothing useful in this record
                continue

            space_key = safe_space_name(space)
            if space_key not in space_files:
                out_name = f"{space_key}_research_artifacts.jsonl.gz"
                out_path = os.path.join(output_dir, out_name)
                space_files[space_key] = gzip.open(out_path, mode="wt", encoding="utf-8")
                counts[space_key] = 0
                print(f"Opened output file for space '{space}': {out_path}")

            out_f = space_files[space_key]

            for art in artifacts:
                if not isinstance(art, dict):
                    continue

                artifact_node_props, relation_props = split_artifact_and_relation_fields(
                    art, paper_id=paper_id
                )

                out_record = {
                    "doi": doi.lower(),  # Ensure DOI is lowercase
                    "space": space,
                    "artifact": artifact_node_props,
                    "relation": relation_props,
                }

                json.dump(out_record, out_f, ensure_ascii=False)
                out_f.write("\n")
                counts[space_key] = counts.get(space_key, 0) + 1

    finally:
        for space_key, f in space_files.items():
            f.close()
            print(f"Closed output file for space '{space_key}' with {counts.get(space_key, 0)} records")

    return counts


def process_research_artifacts_dir(
    input_dir: str,
    output_dir: str,
) -> Dict[str, int]:
    """
    Process all `.json.gz` files in a directory, aggregating results into
    per-space gzipped JSONL files.

    This keeps one set of writers open for all files so that per-space
    outputs are contiguous and not overwritten.
    """
    if not os.path.isdir(input_dir):
        raise NotADirectoryError(f"Input directory not found: {input_dir}")

    os.makedirs(output_dir, exist_ok=True)

    space_files: Dict[str, gzip.GzipFile] = {}
    counts: Dict[str, int] = {}

    try:
        for filename in sorted(os.listdir(input_dir)):
            if not filename.endswith(".json.gz"):
                continue
            path = os.path.join(input_dir, filename)
            if not os.path.isfile(path):
                continue

            print(f"Processing file: {path}")

            for rec in iter_jsonl_gz(path):
                doi = (rec.get("doi") or "").strip().lower()
                paper_id = rec.get("paper_id")
                space = rec.get("spaces")
                artifacts = rec.get("research_artifacts") or []

                if not doi or not space or paper_id is None or not isinstance(artifacts, list) or not artifacts:
                    # Nothing useful in this record
                    continue

                space_key = safe_space_name(space)
                if space_key not in space_files:
                    out_name = f"{space_key}_research_artifacts.jsonl.gz"
                    out_path = os.path.join(output_dir, out_name)
                    space_files[space_key] = gzip.open(out_path, mode="wt", encoding="utf-8")
                    counts[space_key] = 0
                    print(f"Opened output file for space '{space}': {out_path}")

                out_f = space_files[space_key]

                for art in artifacts:
                    if not isinstance(art, dict):
                        continue

                    artifact_node_props, relation_props = split_artifact_and_relation_fields(
                        art, paper_id=paper_id
                    )

                    out_record = {
                        "doi": doi,
                        "space": space,
                        "artifact": artifact_node_props,
                        "relation": relation_props,
                    }

                    json.dump(out_record, out_f, ensure_ascii=False)
                    out_f.write("\n")
                    counts[space_key] = counts.get(space_key, 0) + 1

    finally:
        for space_key, f in space_files.items():
            f.close()
            print(f"Closed output file for space '{space_key}' with {counts.get(space_key, 0)} records")

    return counts


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Parse ARC research_artifacts JSONL.GZ and create per-space files "
            "for ResearchArtifact nodes and their relations to papers (by DOI). "
            "The --input argument may be either a single .json.gz file or a "
            "directory containing many gzipped JSON files."
        )
    )
    parser.add_argument(
        "--input",
        "-i",
        dest="input_path",
        required=True,
        help="Path to a single input JSONL.GZ file OR a directory containing multiple *.json.gz files",
    )
    parser.add_argument(
        "--output-dir",
        "-o",
        dest="output_dir",
        default="/data2/tmp/raa_reevaluate_071025/artifacts_parsed",
        help=(
            "Directory where per-space ResearchArtifact JSONL.GZ files will be written "
            "(default: /data2/tmp/raa_reevaluate_071025/artifacts_parsed)"
        ),
    )
    return parser


def main() -> None:
    parser = build_arg_parser()
    args = parser.parse_args()

    # If --input points to a directory, process all *.json.gz files inside;
    # otherwise treat it as a single input file.
    if os.path.isdir(args.input_path):
        counts = process_research_artifacts_dir(
            input_dir=args.input_path,
            output_dir=args.output_dir,
        )
    else:
        counts = process_research_artifacts_file(
            input_path=args.input_path,
            output_dir=args.output_dir,
        )

    print("\nResearchArtifact extraction completed.")
    for space, cnt in sorted(counts.items()):
        print(f"  {space}: {cnt} artifact usages")


if __name__ == "__main__":
    main()

