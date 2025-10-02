import logging
from dataclasses import dataclass
from logging import getLogger, Logger
import morph_kgc
from rdflib import Graph
import csv

from rdfc_runner import Processor, ProcessorArgs, Reader, Writer


# --- Type Definitions ---
@dataclass
class TemplateArgs(ProcessorArgs):
    reader: Reader
    writer: Writer


# --- Processor Implementation ---
class MorphKGCProcessorPy(Processor[TemplateArgs]):
    logger: Logger = getLogger('rdfc.TemplateProcessor')

    def __init__(self, args: TemplateArgs):
        super().__init__(args)
        self.clear_temp_file()
        self.finalGraph: Graph | None = None
        self.config_str = """
[DEFAULT]
main_dir: ./

[CONFIGURATION]
output_file: knowledge-graph.ttl

[DataSource1]
mappings: NewMapping.rml.ttl
file_path: temp_data.csv
"""


        self.logger.debug(msg="Created TemplateProcessor with args: {}".format(args))

    async def init(self) -> None:

        self.logger.debug("Initializing TemplateProcessor with args: {}", self.args)

    async def transform(self) -> None:
        async for msg in self.args.reader.strings():
            self.write_temp_file(msg)



    async def produce(self) -> None:
        pass

########################################################################################
    def clear_temp_file(self) -> None:
        outfile = open("./temp_data.csv", "wb")
        outfile.close()

    def write_temp_file(self,msg) -> None:
        # Open the destination file
        outfile = open("temp_data.csv", "ab")
        outfile.write(msg)
        outfile.close()
    
    def materialize(self):
        graph = Graph()
        # generate the triples and load them to an RDFLib graph
        graph = morph_kgc.materialize(self.config_str)
        return graph
    
