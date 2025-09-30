import logging
from dataclasses import dataclass
from logging import getLogger, Logger
import aiofiles

from rdfc_runner import Processor, ProcessorArgs, Reader, Writer

from rdflib import Graph
from pyshacl import validate

# --- Type Definitions ---
@dataclass
class TemplateArgs(ProcessorArgs):
    datareader: Reader
    datawriter: Writer
    loc: str
    shaclwriter: Writer


# --- Processor Implementation ---
class SHACLvalidatePy(Processor[TemplateArgs]):
    logger: Logger = getLogger('rdfc.TemplateProcessor')

    def __init__(self, args: TemplateArgs):
        super().__init__(args)
        self.logger.debug(msg="Created TemplateProcessor with args: {}".format(args))
        self.shaclfile: Graph | None = None 

    async def init(self) -> None:
        self.logger.debug("Initializing TemplaSHACLvalidatePy Processor with args: {}", self.args)
        async with aiofiles.open(self.args.loc, mode='r') as f:
            ttl_data = await f.read()
        g = Graph()
        g.parse(data=ttl_data, format='ttl')
        self.shaclfile = g

    async def transform(self) -> None:

        datagraph = Graph()
       
        async for msg in self.args.datareader.strings():
            datagraph.parse(data=msg, format="turtle")
            conforms, report_graph, report_text = validate(datagraph,shacl_graph=self.shaclfile,inference='rdfs',abort_on_error=False,meta_shacl=False,debug=False)
            await self.args.shaclwriter.string(report_text)
            await self.args.datawriter.string(datagraph.serialize(format="turtle"))


        await self.args.datawriter.close()
        await self.args.shaclwriter.close()
        


    async def produce(self) -> None:
        """Function to start the production of data, starting the pipeline.
        This function is called after all processors are completely set up."""
        pass
