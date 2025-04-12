from src.workflow.workflowbuilder import WorkflowBuilder

from src.extract.extract import FromDb
from src.extract.open_meteo import OMFetch
from src.extract.polygon import PFetch

from src.transform.transform import Log
from src.transform.transform import Sort
from src.transform.open_meteo import OMJsonToDF
from src.transform.open_meteo import OMPreprocess
from src.transform.open_meteo import OMPredict
from src.transform.polygon import PJsonToDF
from src.transform.polygon import PPreprocess
from src.transform.polygon import PPostprocess
from src.transform.polygon import PPredict

from src.load.load import LoadToDb

from schema.lake.open_meteo import schema as lomschema
from schema.lake.polygon import schema as lpschema
from schema.warehouse.open_meteo import data_schema as womdschema
from schema.warehouse.open_meteo import predict_schema as wompschema
from schema.warehouse.polygon import schema_data as wpdschema
from schema.warehouse.polygon import schema_predict as wppschema

from src.dashboard.dashboard import Dashboard
from utils.ml.globals import DAYS_TO_LEARN
from src.log.log import setup_logging

import logging
import sys

args = sys.argv[1:]

for arg in args:
    if arg == "newopenmeteo":   # predict rainfall using new open meta data from api
        setup_logging(arg)
        logging.info("Started.\n")

        workflow = (WorkflowBuilder()
                    .add(OMFetch("https://api.open-meteo.com/v1/forecast"))
                    .add(LoadToDb("lake", "open_meteo", lomschema))
                    .add(OMJsonToDF())
                    .add(LoadToDb("warehouse", "open_meteo_data", womdschema))
                    .add(OMPreprocess())
                    .add(OMPredict())
                    .add(LoadToDb("warehouse", "open_meteo_predict", wompschema))
                    .add(Log(True))
                    .build())
        workflow.run()



    if arg == "oldopenmeteo":   # predict rainfall using open meta data in warehouse
        setup_logging(arg)
        logging.info("Started.\n")

        workflow = (WorkflowBuilder()
                    .add(FromDb("warehouse", "open_meteo_data", womdschema, "date", 30))
                    .add(OMPreprocess())
                    .add(OMPredict())
                    .add(LoadToDb("warehouse", "open_meteo_predict", wompschema))
                    .add(Log(True))
                    .build())
        workflow.run()



    if arg == "newpolygon":   # predict stock price using new polygon data from api
        setup_logging(arg)
        logging.info("Started.\n")

        workflow = (WorkflowBuilder()
                    .add(PFetch("POLYGON_KEY"))
                    .add(LoadToDb("lake", "polygon", lpschema))
                    .add(PJsonToDF())
                    .add(Sort("from"))
                    .add(LoadToDb("warehouse", "polygon_data", wpdschema))
                    .add(PPreprocess())
                    .add(PPredict())
                    .add(PPostprocess())
                    .add(LoadToDb("warehouse", "polygon_predict", wppschema))
                    .add(Log(True))
                    .build())
        workflow.run()



    if arg == "oldpolygon":   # predict stock price using polygon data in warehouse
        setup_logging(arg)
        logging.info("Started.\n")

        workflow = (WorkflowBuilder()
                    .add(FromDb("warehouse", "polygon_data", wpdschema, "from", DAYS_TO_LEARN + 1))
                    .add(PPreprocess())
                    .add(PPredict())
                    .add(PPostprocess())
                    .add(LoadToDb("warehouse", "polygon_predict", wppschema))
                    .add(Log(True))
                    .build())
        workflow.run()


    if arg == "dashboard":
        setup_logging(arg)
        logging.info("Started.\n")

        workflow = (WorkflowBuilder()
                    .add(Dashboard())
                    .build())
        workflow.run()