import logging
from namessync import sync_to_vliz


logger = logging.getLogger("namesssync")
logger.setLevel(logging.DEBUG)


if __name__ == "__main__":
    sync_to_vliz()
