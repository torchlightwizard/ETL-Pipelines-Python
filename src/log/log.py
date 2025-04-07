import logging
import os


def setup_logging(arg):
    log_path = "./logs/"
    os.makedirs(log_path, exist_ok=True)

    for handler in logging.root.handlers[:]:  
        logging.root.removeHandler(handler)
    logging.shutdown()

    logging.basicConfig(
        filename=os.path.join(log_path, f"workflow_{arg}.log"),
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )