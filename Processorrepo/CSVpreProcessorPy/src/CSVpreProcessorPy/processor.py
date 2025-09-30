import logging
from dataclasses import dataclass
from logging import getLogger, Logger

from rdfc_runner import Processor, ProcessorArgs, Reader, Writer
import pandas as pd
import io

# --- Type Definitions ---
@dataclass
class TemplateArgs(ProcessorArgs):
    reader: Reader
    writer: Writer


# --- Processor Implementation ---
class CSVpreProcessorPy(Processor[TemplateArgs]):
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
            self.logger.log(msg=msg, level=logging.INFO)
            df = pd.read_csv(io.StringIO(msg))
            df['Timestamp'] = df['Timestamp'].astype(str).str.replace(' ','T', regex=False)
            updated_msg = df.to_csv(index=False)
            await self.args.writer.string(updated_msg)

        
        await self.args.writer.close()

    async def produce(self) -> None:
        """Function to start the production of data, starting the pipeline.
        This function is called after all processors are completely set up."""
        pass
