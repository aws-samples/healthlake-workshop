# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import sys
import json
import csv
import pandas as pd
import boto3
from awsglue.utils import getResolvedOptions
from io import StringIO

s3 = boto3.resource('s3')
s3_client = boto3.client('s3')

files = []

args = getResolvedOptions(sys.argv,
                          ['bucket', 'prefix'])


def get_docref_files():
    response = s3_client.list_objects_v2(
        Bucket=args['bucket'],
        Prefix=args['prefix']
    )
    print(response)
    for file in response['Contents']:
        print(file['Key'])
        download_docref_files(file['Key'])


def download_docref_files(file):
    prefix = file
    file = prefix.split("/")[-1]
    files.append(file)
    with open("/tmp/" + file, 'wb') as data:
        s3_client.download_fileobj(args['bucket'], prefix, data)


get_docref_files()

for file in files:
    print("/tmp/" + file)
    with open("/tmp/" + file, 'rb') as fin:
        fin = fin.readlines()
        # print(fin)
        row_id = 0
        output = []

        for line in fin:
            data = json.loads(line)
            record_id = data['id']
            patient_id = data['subject']['reference']
            encounter_id = data['context']['encounter'][0]['reference']

            ## get the top-level FHIR extension element
            try:
                data = data['extension']
            except KeyError:
                continue

            for item in data:
                if item['url'] == 'http://healthlake.amazonaws.com/aws-cm/':
                    data = item['extension']
                    break

            ## get the Colossus inferred ICD-10 extension element
            for item in data:
                if item['url'] == 'http://healthlake.amazonaws.com/aws-cm/infer-icd10/':
                    data = item['extension']
                    break

            ## process each of the inferred Colossus ICD-10 entities
            for item in data:
                if item['url'] == 'http://healthlake.amazonaws.com/aws-cm/infer-icd10/aws-cm-icd10-entity':
                    entity_score = None
                    entity_id = None

                    for entity in item['extension']:
                        ## store the entity id
                        if entity['url'] == 'http://healthlake.amazonaws.com/aws-cm/infer-icd10/aws-cm-icd10-entity-id':
                            entity_id = entity['valueInteger']
                            continue

                        ## store the entity score
                        if entity[
                            'url'] == 'http://healthlake.amazonaws.com/aws-cm/infer-icd10/aws-cm-icd10-entity-score':
                            entity_score = entity['valueDecimal']
                            continue

                        if entity[
                            'url'] == 'http://healthlake.amazonaws.com/aws-cm/infer-icd10/aws-cm-icd10-entity-ConceptList':
                            code_id = 0

                            ## capture each of detected codes and scores
                            for concept in entity['extension']:
                                code_value = None
                                code_score = None
                                code_description = None

                                for code in concept['extension']:
                                    if code[
                                        'url'] == 'http://healthlake.amazonaws.com/aws-cm/infer-icd10/aws-cm-icd10-entity-Concept-Code':
                                        code_value = code['valueString']
                                        continue
                                    if code[
                                        'url'] == 'http://healthlake.amazonaws.com/aws-cm/infer-icd10/aws-cm-icd10-entity-Concept-Score':
                                        code_score = code['valueDecimal']
                                        continue
                                    if code[
                                        'url'] == 'http://healthlake.amazonaws.com/aws-cm/infer-icd10/aws-cm-icd10-entity-Concept-Description':
                                        code_description = code['valueString']
                                        continue

                                code_value = code_value.replace('.', '')

                                ## entity_score needs to be assigned before the code scores/values are processed
                                output.append({
                                    'row_id': row_id,
                                    'record_id': record_id,
                                    'encounter_id': encounter_id,
                                    'patient_id': patient_id,
                                    'entity_id': entity_id,
                                    'entity_score': entity_score,
                                    'code_id': code_id,
                                    'code_score': code_score,
                                    'code_value': code_value,
                                    'code_description': code_description
                                })
                                code_id += 1
                                row_id += 1
    df = pd.DataFrame(output)
    file = file.split(".")[0]
    df.to_csv("/tmp/" + file + ".csv", index=False, sep='\t', quoting=csv.QUOTE_NONE, escapechar=' ')
    s3.Bucket(args['bucket']).upload_file('/tmp/' + file + '.csv', 'ParsedDocRef/' + file + ".csv")
