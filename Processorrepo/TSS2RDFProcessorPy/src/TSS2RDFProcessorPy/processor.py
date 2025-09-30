import logging
from dataclasses import dataclass
from logging import getLogger, Logger

from rdfc_runner import Processor, ProcessorArgs, Reader, Writer
from rdflib import Graph,URIRef,Namespace,BNode,Literal
from rdflib.namespace import XSD,RDF
import pandas as pd
import argparse
from collections import defaultdict
from datetime import datetime
import json

# --- Type Definitions ---
@dataclass
class TemplateArgs(ProcessorArgs):
    reader: Reader
    writer: Writer


# --- Processor Implementation ---
class TSS2RDFProcessorPy(Processor[TemplateArgs]):
    logger: Logger = getLogger('rdfc.TemplateProcessor')

    def __init__(self, args: TemplateArgs):
        super().__init__(args)
        self.finalGraph: Graph | None = None
        self.logger.debug(msg="Created TemplateProcessor with args: {}".format(args))

    async def init(self) -> None:
        
        self.logger.debug("Initializing TemplateProcessor with args: {}", self.args)

    async def transform(self) -> None:
        
        datagraph = Graph()
        async for msg in self.args.reader.strings():
            datagraph.parse(data=msg, format="turtle")
            self.finalGraph = self.CreateRDF(datagraph)
            await self.args.writer.string(self.finalGraph.serialize(format="turtle"))
        
        await self.args.writer.close()

        
    async def produce(self) -> None:
        """Function to start the production of data, starting the pipeline.
        This function is called after all processors are completely set up."""
        pass

#####################################################################################

    def CreateRDF(self,graph):
        # Nested dict: subject -> predicate -> list of objects
        #results = defaultdict(lambda: defaultdict(list))

        #Create RDF
        prefix_tss = Namespace('https://w3id.org/tss#')
        prefix_ex  = Namespace('http://example.org/')
        prefix_sosa  = Namespace('http://www.w3.org/ns/sosa/')
        prefix_xsd  = Namespace('http://www.w3.org/2001/XMLSchema#')
        final_graph = Graph()
        final_graph.bind('tss', prefix_tss)
        final_graph.bind('ex', prefix_ex)
        final_graph.bind('sosa', prefix_sosa)
        final_graph.bind('xsd', prefix_xsd)
        print('Started creating final graph')

        snippet_id_dic = {} #this dictionary is designed to bind snippet subject to point id from array. 

        get_snippet_query = '''
        PREFIX sosa: <http://www.w3.org/ns/sosa/>
        PREFIX tss:  <https://w3id.org/tss#>
        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX xsd:  <http://www.w3.org/2001/XMLSchema#>

        SELECT ?snippet ?P ?O ?about ?aboutP ?aboutO
        WHERE {
        ?snippet a tss:Snippet .
        ?snippet ?P ?O .

        OPTIONAL {
            ?snippet tss:about ?about .
            ?about ?aboutP ?aboutO .
        }
        }

                '''
        tss_points = URIRef("https://w3id.org/tss#points")
        tss_Snippet = URIRef("https://w3id.org/tss#Snippet")
        tss_PointTemplate = URIRef("https://w3id.org/tss#PointTemplate")


        for subj, pred, obj, about, aboutP, aboutO in graph.query(get_snippet_query):
            # store snippet triples
            if subj and pred and obj:
                if pred == tss_points: #this is where i should focus on derefrencing the json object
                    #results[subj][pred].append(obj)
                    parsed = json.loads(str(obj)) #json array
                    for point in parsed:
                        json_id = point['id']
                        json_time = point['time']
                        json_value = point['value']
                        #now convert them from strings and add them to final graph
                        json_time = Literal(json_time,datatype=XSD.dateTime)
                        
                        if str(json_value) in ["true", "false"]:
                            json_value = Literal(json_value.lower(), datatype=XSD.boolean)
                        else:
                            try:
                                json_value = Literal(float(json_value), datatype=XSD.decimal)  # Attempt to convert to a number
                            except ValueError:
                                json_value = Literal(json_value, datatype=XSD.string) # If it's not a number, store it as a string

                        #json_value = Literal(json_value,datatype=XSD.decimal) #it is not always a decimal. sometimes it is a boolean value. it is replaced with the if else block above.
                        json_id = URIRef(json_id)

                        snippet_id_dic[json_id] = subj  #the dictionary is mapped in reverse because json_id is unique, subject is not.
                        final_graph.add((json_id,URIRef("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"),URIRef("http://www.w3.org/ns/sosa/Observation")))
                        final_graph.add((json_id,URIRef("http://www.w3.org/ns/sosa/resultTime"),json_time))
                        final_graph.add((json_id,URIRef("http://www.w3.org/ns/sosa/hasSimpleResult"),json_value))

                    #final_graph.add((subj, pred, obj)) #instead of adding the entire json object, will break it down and add each element as object.
                    
                if obj == tss_Snippet:
                    continue

            # # store about triples
            if about and aboutP and aboutO:
                if aboutO != tss_PointTemplate:
                #results[subj][aboutP].append(aboutO)
                #final_graph.add((subj, aboutP, aboutO))
                    for key,value in snippet_id_dic.items():
                        #remember key id of reading, value is subject snippet
                        if subj==URIRef(value):
                            final_graph.add((key, aboutP, aboutO))

        print('Final graph created successfully')
        return final_graph
