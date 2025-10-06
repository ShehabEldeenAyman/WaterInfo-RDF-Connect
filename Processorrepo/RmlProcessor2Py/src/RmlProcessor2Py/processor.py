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
class RmlProcessor2Py(Processor[TemplateArgs]):
    logger: Logger = getLogger('rdfc.TemplateProcessor')

    def __init__(self, args: TemplateArgs):
        super().__init__(args)
        self.logger.debug(msg="Created TemplateProcessor with args: {}".format(args))

    async def init(self) -> None:

        self.logger.debug("Initializing RmlProcessorPy with args: {}", self.args)

    async def transform(self) -> None:    
        async for msg in self.args.reader.strings():
            self.write_temp_file(msg)


    async def produce(self) -> None:

        pass
###############################################################################################################
    def mapdata(self):
        command = ["java", "-jar", "rmlmapper.jar", "-m", self.args.mappingFile, "-o", "./WFresources/generatedRDF.ttl"]

        # Run the process synchronously
        process = subprocess.run(command, capture_output=True, text=True)

        # file_output = ''
        # if process.returncode == 0:
        #     # Regular file read (synchronous)
        #     with open('temp.ttl', 'r') as f:
        #         file_output = f.read()

        # # Write output if writer exists
        # if self.args.writer:
        #     self.args.writer.string(file_output)

        # # Close the writer after processing all messages
        # if self.args.writer:
        #     self.args.writer.close()

        # self.logger.debug("done reading RmlProcessorPy so closed writer.")

    def write_temp_file(self,msg) -> None:
        # Open the destination file
        with open("temp_data.csv", "a", encoding="utf-8") as outfile:
            outfile.write(msg)