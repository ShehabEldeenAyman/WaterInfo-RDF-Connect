import logging
from dataclasses import dataclass
from logging import getLogger, Logger

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
        self.logger.debug(msg="Created TemplateProcessor with args: {}".format(args))

    async def init(self) -> None:

        self.logger.debug("Initializing TemplateProcessor with args: {}", self.args)

    async def transform(self) -> None:


    async def produce(self) -> None:
        pass

########################################################################################
    def clear_temp_file(self) -> None:
        outfile = open("temp_data.csv", "wb")
        outfile.close()
