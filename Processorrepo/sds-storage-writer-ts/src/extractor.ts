import type * as RDF from "@rdfjs/types";
import { RDF as RDFT, RelationType, SDS } from "@treecg/types";
import { Parser } from "n3";
import { extractShapes, match, subject } from "rdf-lens";

const SHAPE = `
@prefix cidoc: <http://www.cidoc-crm.org/cidoc-crm/>.
@prefix rdfl: <https://w3id.org/rdf-lens/ontology#>.
@prefix sosa: <http://www.w3.org/ns/sosa/>.
@prefix dcterms: <http://purl.org/dc/terms/>.
@prefix xsd: <http://www.w3.org/2001/XMLSchema#>.
@prefix sh: <http://www.w3.org/ns/shacl#>.
@prefix sds: <https://w3id.org/sds#>.

[ ] a sh:NodeShape;
  sh:targetClass <Record>;
  sh:property [
    sh:name "stream";
    sh:path sds:stream;
    sh:datatype xsd:string;
    sh:minCount 1;
    sh:maxCount 1;
  ], [
    sh:name "payload";
    sh:path sds:payload;
    sh:datatype xsd:string;
    sh:minCount 1;
    sh:maxCount 1;
  ], [
    sh:name "buckets";
    sh:path sds:bucket;
    sh:datatype xsd:string;
  ], [
    sh:name "dataless";
    sh:path sds:dataless;
    sh:datatype xsd:boolean;
    sh:minCount 0;
    sh:maxCount 1;
  ].

[ ] a sh:NodeShape;
  sh:targetClass <Bucket>;
  sh:property [
    sh:name "id";
    sh:path ( );
    sh:datatype xsd:string;
    sh:minCount 1;
    sh:maxCount 1;
  ], [
    sh:name "streamId";
    sh:path sds:stream;
    sh:datatype xsd:string;
    sh:minCount 1;
    sh:maxCount 1;
  ], [
    sh:name "immutable";
    sh:path sds:immutable;
    sh:datatype xsd:boolean;
    sh:maxCount 1;
  ], [
    sh:name "root";
    sh:path sds:isRoot;
    sh:datatype xsd:boolean;
    sh:maxCount 1;
  ], [
    sh:name "empty";
    sh:path sds:empty;
    sh:datatype xsd:boolean;
    sh:maxCount 1;
  ].

[ ] a sh:NodeShape;
  sh:targetClass <Relation>;
  sh:property [
    sh:name "type";
    sh:path sds:relationType;
    sh:datatype xsd:string;
    sh:minCount 1;
    sh:maxCount 1;
  ], [
    sh:name "stream";
    sh:path ( [ sh:inversePath sds:relation ] sds:stream );
    sh:datatype xsd:string;
    sh:minCount 1;
    sh:maxCount 1;
  ], [
    sh:name "origin";
    sh:path [ sh:inversePath sds:relation ];
    sh:datatype xsd:string;
    sh:minCount 1;
    sh:maxCount 1;
  ], [
    sh:name "bucket";
    sh:path sds:relationBucket;
    sh:datatype xsd:string;
    sh:minCount 1;
    sh:maxCount 1;
  ], [
    sh:name "path";
    sh:path sds:relationPath;
    sh:class <RdfThing>;
    sh:maxCount 1;
  ], [
    sh:name "value";
    sh:path sds:relationValue;
    sh:class <RdfThing>;
    sh:maxCount 1;
  ].

[ ] a sh:NodeShape;
  sh:targetClass <RdfThing>;
  sh:property [
    sh:name "id";
    sh:path ( );
    sh:maxCount 1;
    sh:minCount 1;
    sh:datatype xsd:any;
  ], [
    sh:name "quads";
    sh:path ( );
    sh:maxCount 1;
    sh:minCount 1;
    sh:class rdfl:CBD;
  ].

`

const Shapes = extractShapes(new Parser().parse(SHAPE));

export type Record = {
    stream: string;
    payload: string;
    buckets: string[];
    dataless?: boolean;
};

export type Bucket = {
    id: string;
    streamId: string;
    immutable?: boolean;
    root?: boolean;
    empty?: boolean;
};

export type RdfThing = {
    id: RDF.Term;
    quads: RDF.Quad[];
};

export type Relation = {
    type: RelationType;
    stream: string;
    origin: string;
    bucket: string;
    value?: RdfThing;
    path?: RdfThing;
};

const RecordLens = match(undefined, SDS.terms.payload, undefined)
    .thenAll(subject)
    .thenSome(Shapes.lenses["Record"]);

const BucketLens = match(undefined, RDFT.terms.type, SDS.terms.custom("Bucket"))
    .thenAll(subject)
    .thenSome(Shapes.lenses["Bucket"]);

const RelationLens = match(
    undefined,
    RDFT.terms.type,
    SDS.terms.custom("Relation"),
)
    .thenAll(subject)
    .thenSome(Shapes.lenses["Relation"]);

export class Extract {
    private data: RDF.Quad[] = [];
    private description: RDF.Quad[] = [];
    private removeDescription: RDF.Quad[] = [];

    constructor(full: RDF.Quad[]) {
        full.forEach((q) => {
            if (q.graph.equals(SDS.terms.custom("DataDescription"))) {
                this.description.push(q);
            } else if (
                q.graph.equals(SDS.terms.custom("RemoveDataDescription"))
            ) {
                this.removeDescription.push(q);
            } else {
                this.data.push(q);
            }
        });
    }

    getData(): RDF.Quad[] {
        return this.data;
    }

    getRecords(): Record[] {
        return <Record[]>RecordLens.execute(this.description);
    }

    getBuckets(): Bucket[] {
        return <Bucket[]>BucketLens.execute(this.description);
    }

    getRelations(): Relation[] {
        return <Relation[]>RelationLens.execute(this.description);
    }

    getRemoveRelations(): Relation[] {
        return <Relation[]>RelationLens.execute(this.removeDescription);
    }
}

export class Extractor {
    constructor() {}

    extract_quads(quads: RDF.Quad[]): Extract {
        return new Extract(quads);
    }

    extract(inp: string): Extract {
        return new Extract(new Parser().parse(inp));
    }
}
