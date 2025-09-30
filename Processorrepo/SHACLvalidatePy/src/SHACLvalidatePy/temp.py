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
    shaclreader: Reader
    shaclwriter: Writer


# --- Processor Implementation ---
class SHACLvalidatePy(Processor[TemplateArgs]):
    logger: Logger = getLogger('rdfc.TemplateProcessor')

    def __init__(self, args: TemplateArgs):
        super().__init__(args)
        self.logger.debug(msg="Created TemplateProcessor with args: {}".format(args))

    async def init(self) -> None:
        """This is the first function that is called (and awaited) when creating a processor.
        This is the perfect location to start things like database connections."""
        self.logger.debug("Initializing TemplateProcessor with args: {}", self.args)

    async def transform(self) -> None:
        #dataChunk=[]
        #shaclChunk=[]
        datagraph = Graph()
        shaclgraph = Graph()


        async for msg in self.args.datareader.strings():
            datagraph.parse(data=msg, format="turtle")
            #fulldata=fulldata+msg
            #dataChunk.append(msg)
            #await self.args.datawriter.string(msg)

        async for msg in self.args.shaclreader.strings():
            shaclgraph.parse(data=msg, format="turtle")
            #fullshacl=fullshacl+msg
            #shaclChunk.append(msg)
            #await self.args.shaclwriter.string(msg)

        #fulldata = ''.join(dataChunk)
        #fullshacl = ''.join(shaclChunk)

        conforms, report_graph, report_text = validate(
    datagraph,
    shacl_graph=shaclgraph,
    inference='rdfs',      # or 'none' / 'owlrl'
    abort_on_error=False,
    meta_shacl=False,
    debug=False
)
        #await self.args.shaclwriter.string(report_graph.serialize(format="turtle"))
        await self.args.shaclwriter.string(report_text)

        # async with aiofiles.open("./WFresources/generated/internalreport.txt", mode="w") as af:
        #     await af.write(report_text)
        #     await af.flush()
       

        if conforms:
            self.logger.debug("Data conforms to the SHACL shapes!")
            await self.args.datawriter.string(datagraph.serialize(format="turtle"))

        else:
            self.logger.debug("Data does NOT conform to the SHACL shapes!")
            await self.args.datawriter.string(report_graph.serialize(format="turtle"))


        await self.args.datawriter.close()
        await self.args.shaclwriter.close()
        


    async def produce(self) -> None:
        """Function to start the production of data, starting the pipeline.
        This function is called after all processors are completely set up."""
        pass
