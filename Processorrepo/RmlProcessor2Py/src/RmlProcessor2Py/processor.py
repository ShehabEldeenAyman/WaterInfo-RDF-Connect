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
        self.clear_temp_CSV_file()
        self.finalGraph = ''

    async def init(self) -> None:

        self.logger.debug("Initializing RmlProcessorPy with args: {}", self.args)

    async def transform(self) -> None:    
        async for msg in self.args.reader.strings():
            self.write_temp_CSV_file(msg)
            process = self.mapdata()
            if process.returncode == 0:
                self.finalGraph = self.read_temp_RDF_file()
                print("mapping process returned positive code")
                await self.args.writer.string(self.finalGraph)

            else:
                print("mapping process returned negative code")
                await self.args.writer.string('error')

        #self.delete_temp_CSV_file()
        #self.delete_temp_RDF_file()
        await self.args.writer.close()



    async def produce(self) -> None:

        pass
###############################################################################################################
    def mapdata(self):
        command = ["java", "-jar", "rmlmapper.jar", "-m", self.args.mappingFile, "-o", "./WFresources/generatedRDF.ttl"] #newMapping.rml.ttl
        # Run the process synchronously
        process = subprocess.run(command, capture_output=True, text=True)
        return process

    def write_temp_CSV_file(self,msg) -> None:
        # Open the destination file
        with open("./WFresources/temp_data.csv", "a", encoding="utf-8") as outfile:
            outfile.write(msg)

    def clear_temp_CSV_file(self) -> None:
        with open("./WFresources/temp_data.csv", "w", encoding="utf-8") as outfile:
            pass
    
    def delete_temp_CSV_file(self) -> None:
        if os.path.exists("./WFresources/temp_data.csv"):
            os.remove("./WFresources/temp_data.csv")
            print("File deleted successfully.")
        else:
            print("File not found.")

    def delete_temp_RDF_file(self) -> None:
        if os.path.exists("./WFresources/generatedRDF.ttl"):
            os.remove("./WFresources/generatedRDF.ttl")
            print("File deleted successfully.")
        else:
            print("File not found.")
    
    def read_temp_RDF_file(self):
        with open("./WFresources/generatedRDF.ttl", "r", encoding="utf-8") as file:
            return file.read()
