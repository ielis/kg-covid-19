import csv
import gzip
import os
import uuid
from collections import namedtuple
from typing import Optional

from kg_covid_19.transform_utils.transform import Transform

KgxNode = namedtuple('KgxNode', ['id', 'category', 'name', 'provided_by'])
KgxEdge = namedtuple('KgxEdge',
                     ['id', 'subject', 'predicate', 'object', 'relation', 'primary_knowledge_source', 'category',
                      'publications'])


class DiseaseAnnotations(Transform):
    """DiseaseAnnotations parses HPO annotations file (HPOA)
    """

    def __init__(self, input_dir: str = None, output_dir: str = None):
        source_name = "HPOA"
        self._file_header = ['DatabaseID', 'DiseaseName', 'Qualifier', 'HPO_ID',
                             'Reference', 'Evidence', 'Onset', 'Frequency', 'Sex',
                             'Modifier', 'Aspect', 'Biocuration']
        # TODO(ielis): think about:
        #  - do we add terms that are excluded/not present in the disease?
        #  - do we use all Aspect types, or only `P`?
        self._add_excluded_phenotype = False
        super().__init__(source_name, input_dir, output_dir)

    def run(self, data_file: Optional[str] = None):
        if not data_file:
            data_file = os.path.join(self.input_base_dir, 'phenotype.hpoa')
        compression: Optional[str] = None

        self.parse(data_file, compression)

    def parse(self, data_file: str, compression: Optional[str] = None):

        #  TODO(justaddcoffee) - to check if these values are OK
        category = 'biolink:Disease'
        provided_by = 'HPO:hpoa'
        has_phenotype = 'biolink:has_phenotype'
        has_symptom = 'RO:0002200'
        phenotype_to_disease_association = 'biolink:PhenotypeToDiseaseAssociation'

        # --------------------------------------------------------------------------------------------------------------

        tsv_dialect = TsvDialect()

        nodes = set()
        edges = set()

        with self._open_file(data_file, compression) as fh:
            # skip the first 5 lines of the header
            [next(fh) for _ in range(5)]

            reader = csv.DictReader(fh, fieldnames=self._file_header, dialect=tsv_dialect)
            for row in reader:
                qualifier = row['Qualifier']
                phenotype_is_present: bool
                if qualifier == '':
                    phenotype_is_present = True
                elif qualifier == 'NOT':
                    phenotype_is_present = False
                else:
                    raise ValueError(f'Unexpected qualifier `{qualifier}` in line {row}')

                if (not self._add_excluded_phenotype) and (not phenotype_is_present):
                    continue

                disease_name = row['DiseaseName']
                disease_id = row['DatabaseID']
                hpo_id = row['HPO_ID']

                node = KgxNode(id=disease_id, category=category, name=disease_name, provided_by=provided_by)
                nodes.add(node)

                edge_id = 'urn:uuid:' + str(uuid.uuid3(uuid.NAMESPACE_DNS,
                                                       '-'.join([disease_id, hpo_id, str(phenotype_is_present)])))
                edge = KgxEdge(id=edge_id, subject=disease_id, predicate=has_phenotype, object=hpo_id,
                               relation=has_symptom, primary_knowledge_source=provided_by,
                               category=phenotype_to_disease_association, publications=row['Reference'])
                edges.add(edge)

        # write nodes
        nodes_out_path = os.path.join(self.output_dir, 'HPOA_nodes.tsv')
        node_tsv_column_names = ['id', 'category', 'name', 'provided_by']
        with open(nodes_out_path, 'w', newline='') as fh:
            writer = csv.DictWriter(fh, fieldnames=node_tsv_column_names, dialect=tsv_dialect)
            writer.writeheader()
            for node in nodes:
                row = {
                    'id': node.id, 'category': node.category,
                    'name': node.name, 'provided_by': node.provided_by
                }
                writer.writerow(row)

        # write edges
        edges_out_path = os.path.join(self.output_dir, 'HPOA_edges.tsv')
        edge_tsv_column_names = ['subject', 'predicate', 'object', 'relation', 'provided_by', '']
        with open(edges_out_path, 'w', newline='') as fh:
            writer = csv.DictWriter(fh, fieldnames=edge_tsv_column_names, dialect=tsv_dialect)
            writer.writeheader()
            for edge in edges:
                row = {
                    'subject': edge.subject, 'predicate': edge.predicate,
                    'object': edge.object, 'relation': edge.relation,
                    'provided_by': edge.primary_knowledge_source
                }
                writer.writerow(row)

    @staticmethod
    def _open_file(data_file: str, compression: Optional[str]):
        if compression:
            if compression == 'gz':
                return gzip.open(data_file, newline='')
            else:
                raise ValueError(f'Cannot open file with `{compression}` compression')
        else:
            return open(data_file, newline='')


class TsvDialect(csv.Dialect):
    delimiter = "\t"
    escapechar = '\\'
    doublequote = False
    skipinitialspace = True
    lineterminator = os.linesep
    quoting = csv.QUOTE_NONE
