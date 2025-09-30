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
class RDF2TSSProcssorPy(Processor[TemplateArgs]):
    logger: Logger = getLogger('rdfc.TemplateProcessor')

    def __init__(self, args: TemplateArgs):
        super().__init__(args)
        self.finalGraph: Graph | None = None
        self.sensorSet: set = set()
        self.logger.debug(msg="Created TemplateProcessor with args: {}".format(args))

    async def init(self) -> None:
        self.logger.debug("Initializing TemplateProcessor with args: {}", self.args)

    async def transform(self) -> None:

        datagraph = Graph()
        

        async for msg in self.args.reader.strings():
            datagraph.parse(data=msg, format="turtle")
            self.sensorSet = self.CreateSensorSet(datagraph)
            self.finalGraph = self.CreateTSS(self.sensorSet,datagraph)

            await self.args.writer.string(self.finalGraph.serialize(format="turtle"))

        await self.args.writer.close()

    async def produce(self) -> None:
        pass

    ##################################################################################
    def CreateSensorSet(self,graph):
        sensor_set = set()
        get_sensor_query = ''' 
PREFIX sosa: <http://www.w3.org/ns/sosa/>

SELECT DISTINCT ?sensor
WHERE {
  ?s sosa:madeBySensor ?sensor .
}
'''
        print('Started identifying unique sensors')
        # store actual RDF terms (URIRef or Literal), not stringified values
        for sensor in graph.query(get_sensor_query):
            sensor_term = sensor[0]   # this is an rdflib term (URIRef or Literal)
            sensor_set.add(sensor_term)

        print('Sensors identified successfully')
        return sensor_set

    def CreateTSS(self,sensor_set, graph):
        prefix_tss = Namespace('https://w3id.org/tss#')
        prefix_ex  = Namespace('http://example.org/')
        prefix_sosa  = Namespace('http://www.w3.org/ns/sosa/')
        final_graph = Graph()
        final_graph.bind('tss', prefix_tss)
        final_graph.bind('ex', prefix_ex)
        final_graph.bind('sosa', prefix_sosa)
        print('Started creating final graph')

        for sensor in sensor_set:
            # use sensor.n3() so SPARQL receives a proper token: <uri> or "literal"^^xsd:...
            sensor_token = sensor.n3() if hasattr(sensor, 'n3') else f"<{str(sensor)}>"

            tss_points = []
            tss_query = f'''
            PREFIX sosa: <http://www.w3.org/ns/sosa/>

            SELECT ?READING ?TIME ?OBSERVATION ?observedProperty
            WHERE {{
                ?OBSERVATION a sosa:Observation ;
                            sosa:resultTime ?TIME ;
                            sosa:hasSimpleResult ?READING ;
                            sosa:observedProperty ?observedProperty ;
                            sosa:madeBySensor {sensor_token} .
            }}
            ORDER BY ?TIME
            '''
            results = graph.query(tss_query)
            for row in results:
                # each row values are rdflib terms; keep them
                data = {
                    'time': row.TIME,
                    'value': row.READING,
                    'id': row.OBSERVATION,
                    'observedProperty': row.observedProperty
                }
                tss_points.append(data)

            if not tss_points:
                print(f'Warning: no observations found for sensor {sensor_token}; skipping.')
                continue  # avoid indexing into empty list

            json_object = json.dumps([
                {
                    'time': str(p['time']),    # convert to string for JSON
                    'value': str(p['value']),
                    'id': str(p['id'])
                }
                for p in tss_points
            ])

            # choose an output subject: use the real URI if sensor is URIRef,
            # otherwise mint an example URI for that sensor value
            if isinstance(sensor, URIRef):
                subject = sensor
            else:
                # for a Literal sensor (e.g. "24002042"), create a stable URI in ex: namespace
                safe_id = str(sensor).replace(' ', '_')
                subject = prefix_ex[f"sensor/{safe_id}"]

            temporary_node = BNode()

            final_graph.add((subject, RDF.type, prefix_tss.Snippet))
            # store the JSON timeseries as a literal (you had RDF.JSON before; keep as plain literal or use a string)
            final_graph.add((subject, prefix_tss.points, Literal(json_object)))
            # Add from / to times using the original RDF time Literals
            final_graph.add((subject, prefix_tss["from"], tss_points[0]['time']))
            final_graph.add((subject, prefix_tss.to, tss_points[-1]['time']))
            final_graph.add((subject, prefix_tss.pointType, prefix_sosa.Observation))

            # tss context omitted as in your original comment

            final_graph.add((subject, prefix_tss.about, temporary_node))
            final_graph.add((temporary_node, RDF.type, prefix_tss.PointTemplate))
            # madeBySensor should point to original sensor term if it was a URI, otherwise to an identifier URI
            if isinstance(sensor, URIRef):
                final_graph.add((temporary_node, prefix_sosa.madeBySensor, sensor))
            else:
                # point to an identifier URI we just created
                final_graph.add((temporary_node, prefix_sosa.madeBySensor, subject))
            # use observedProperty from the first point (assumes same observedProperty for sensor)
            final_graph.add((temporary_node, prefix_sosa.observedProperty, tss_points[0]['observedProperty']))

        print('Graph created successfully')
        return final_graph   