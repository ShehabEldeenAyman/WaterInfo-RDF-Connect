import logging
from dataclasses import dataclass
from logging import getLogger, Logger
import requests

from rdfc_runner import Processor, ProcessorArgs, Reader, Writer


# --- Type Definitions ---
@dataclass
class TemplateArgs(ProcessorArgs):
    reader: Reader
    


# --- Processor Implementation ---
class GraphdbProcessorPy(Processor[TemplateArgs]):
    logger: Logger = getLogger('rdfc.TemplateProcessor')

    def __init__(self, args: TemplateArgs):
        super().__init__(args)
        self.logger.debug(msg="Created TemplateProcessor with args: {}".format(args))

    async def init(self) -> None:
        """This is the first function that is called (and awaited) when creating a processor.
        This is the perfect location to start things like database connections."""
        self.logger.debug("Initializing TemplateProcessor with args: {}", self.args)

    async def transform(self) -> None:
        
        async for msg in self.args.reader.strings():
            self.insert_db(msg)

    async def produce(self) -> None:
        """Function to start the production of data, starting the pipeline.
        This function is called after all processors are completely set up."""
        pass
#####################################################################################

    def insert_db(self,rdf_data):
        # GraphDB configuration
        GRAPHDB_BASE_URL = "http://localhost:7200"
        REPOSITORY = " RDFconnect"  # replace with your repository name
        ENDPOINT = f"{GRAPHDB_BASE_URL}/repositories/{REPOSITORY}/statements"
        # HTTP headers
        headers = {
            "Content-Type": "text/turtle",  # Format of RDF data
        }

        try:
            # POST the RDF data to GraphDB
            response = requests.post("http://localhost:7200/repositories/RDFconnect/statements", data=rdf_data.encode("utf-8"), headers=headers)

            # Check response
            if response.status_code == 204:
                self.logger.debug("Data inserted successfully!")
            else:
                self.logger.debug(f"Failed to insert data: {response.status_code}")
                self.logger.debug(response.text)

        except requests.exceptions.ConnectionError as e:
            self.logger.debug("Could not connect to GraphDB. Is the server running?")
            self.logger.debug(f"Details: {e}")

        except requests.exceptions.Timeout:
            self.logger.debug("Request to GraphDB timed out. The server may be too slow or unreachable.")

        except requests.exceptions.RequestException as e:
            # Catches all other requests-related errors
            self.logger.debug("An unexpected error occurred while communicating with GraphDB.")
            self.logger.debug(f"Details: {e}")
