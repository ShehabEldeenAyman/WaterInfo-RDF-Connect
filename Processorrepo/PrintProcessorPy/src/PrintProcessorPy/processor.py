import logging
from dataclasses import dataclass
from logging import getLogger, Logger
import aiofiles
from rdfc_runner import Processor, ProcessorArgs, Reader, Writer
import asyncio




# --- Type Definitions ---
@dataclass
class TemplateArgs(ProcessorArgs):
    reader: Reader
    loc: str


# --- Processor Implementation ---
class PrintProcessorPy(Processor[TemplateArgs]):
    logger: Logger = getLogger('rdfc.TemplateProcessor')

    def __init__(self, args: TemplateArgs):
        super().__init__(args)
        self.logger.debug(msg="Created TemplateProcessor with args: {}".format(args))

    async def init(self) -> None:
        self.logger.debug("Initializing TemplateProcessor with args: {}", self.args)
        try:
            async with aiofiles.open(self.args.loc, mode="w") as af:
                await af.write("")  # Truncate by overwriting with empty content
            self.logger.debug(f"File {self.args.loc} has been truncated and is ready for writing.")
        except Exception as exc:
            self.logger.exception("Failed to truncate the file: %s", exc)
            raise

    async def transform(self) -> None:
        """Read strings from the reader and append them to the file asynchronously."""
        # Open the file once in append mode to avoid repeated open/close overhead.
        try:
            async with aiofiles.open(self.args.loc, mode="a") as af:
                async for msg in self.args.reader.strings():
                    # Ensure msg is a string
                    text = msg if isinstance(msg, str) else str(msg)

                    # Log the incoming message (use info or logger.log with level param)
                    self.logger.info(text)

                    # Write and flush asynchronously
                    await af.write(text)
                    await af.flush()
        except asyncio.CancelledError:
            # Allow cancellation to propagate cleanly
            self.logger.debug("transform cancelled")
            raise
        except Exception as exc:
            self.logger.exception("Error during transform: %s", exc)
            raise
            

    async def produce(self) -> None:
        """Function to start the production of data, starting the pipeline.
        This function is called after all processors are completely set up."""
        pass