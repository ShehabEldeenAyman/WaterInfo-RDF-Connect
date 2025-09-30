import logging
from dataclasses import dataclass
from logging import getLogger, Logger
import asyncio
import os
import aiofiles
from rdfc_runner import Processor, ProcessorArgs, Reader, Writer
import subprocess

# --- Type Definitions ---
@dataclass
class TemplateArgs(ProcessorArgs):
    reader: Reader
    writer: Writer
    mappingFile: str


# --- Processor Implementation ---
class RmlProcessorPy(Processor[TemplateArgs]):
    logger: Logger = getLogger('rdfc.TemplateProcessor')

    def __init__(self, args: TemplateArgs):
        super().__init__(args)
        self.logger.debug(msg="Created TemplateProcessor with args: {}".format(args))

    async def init(self) -> None:
        """This is the first function that is called (and awaited) when creating a processor.
        This is the perfect location to start things like database connections."""
        self.logger.debug("Initializing RmlProcessorPy with args: {}", self.args)

    async def transform(self) -> None:

        # command = ["java", "-jar", "rmlmapper.jar", "-m", self.args.mappingFile, "-o", 'temp.ttl']
        # result = subprocess.run(command, capture_output=True, text=True)
        
        # file_output = ''

        # if result.returncode == 0:
        #     f = open('temp.ttl')
        #     file_output = f.read()
        #     # Echo the message to the writer
        #     if self.args.writer:
        #         await self.args.writer.string(file_output)
        
        command = ["java", "-jar", "rmlmapper.jar", "-m", self.args.mappingFile, "-o", "temp.ttl"]
        process = await asyncio.create_subprocess_exec(
        *command,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )
        stdout, stderr = await process.communicate()
        file_output = ''

        if process.returncode == 0:
        # Async file read (better in async functions)
            async with aiofiles.open('temp.ttl', 'r') as f:
                file_output = await f.read()
        if self.args.writer:
            await self.args.writer.string(file_output)

        # Close the writer after processing all messages
        if self.args.writer:
            await self.args.writer.close()
        self.logger.debug("done reading RmlProcessorPy so closed writer.")

    async def produce(self) -> None:
        """Function to start the production of data, starting the pipeline.
        This function is called after all processors are completely set up."""
        pass