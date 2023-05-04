from namessync.obis import fetch_nonmatching
from namessync.vliz import AlreadyExistsException, VlizSession
import logging
from termcolor import colored
import pyworms
import urllib.parse
from datetime import datetime


logger = logging.getLogger("namessync")


def check_name_has_exact_match(scientificname: str, scientificnameauthorship: str):
    matches = pyworms.aphiaRecordsByMatchNames(urllib.parse.quote(scientificname), marine_only=False)
    if matches is not None:
        assert len(matches) == 1
        matches = matches[0]
        for match in matches:
            if match["match_type"] == "exact":
                if scientificnameauthorship is None or scientificnameauthorship == match["authority"]:
                    return True
    return False


def sync_to_vliz(max_items=10, dry_run=False):
    session = VlizSession()
    annotated_list = session.fetch_annotated_list()
    non_matching = fetch_nonmatching()

    # create annotated list map based on name and authorship

    annotated_map = {}
    for item in annotated_list:
        key = (item["scientificName"].strip() if item["scientificName"] is not None else "") + "|" + (item["scientificNameAuthorship"].strip() if item["scientificNameAuthorship"] is not None else "")
        if key in annotated_map:
            annotated_map[key].append(item)
        else:
            annotated_map[key] = [item]

    # check non matching names against annotated list

    for item in non_matching:
        key = (item["scientificname"].strip() if item["scientificname"] is not None else "") + "|" + (item["scientificnameauthorship"].strip() if item["scientificnameauthorship"] is not None else "")
        if key in annotated_map:
            logger.debug(f"Key {key} already in annotated list")
        else:
            try:
                has_exact_match = check_name_has_exact_match(item["scientificname"], item["scientificnameauthorship"])
            except Exception:
                logger.error(colored(f"Error while querying WoRMS for {key}", "red"))
                continue
            if has_exact_match:
                logger.info(colored(f">>> Key {key} has exact match in WoRMS, skipping", "grey"))
            else:
                logger.info(colored(f">>> Trying to add {key} to the annotated list", "yellow"))
                data = {
                    "scientificName": item["scientificname"],
                    "scientificNameAuthorship": item["scientificnameauthorship"],
                    "phylum": item["phylum"],
                    "class": item["class"],
                    "order": item["order"],
                    "family": item["family"],
                    "genus": item["genus"],
                    "recordCount": int(item["records"]),
                    "datasets": [{"uuid": d.split(";")[0], "url": d.split(";")[1]} for d in item["datasets"].split("|")]
                }
                if not dry_run:
                    try:
                        session.add_annotated_list(data)
                        logger.info(colored(f"    Added {key} to the annotated list", "green"))
                    except AlreadyExistsException:
                        logger.info(colored(f"    Key {key} already exists according to API", "red"))

                if max_items is not None:
                    max_items = max_items - 1
                    if max_items == 0:
                        logger.info(colored("Reached max items, stopping", "blue"))
                        break
