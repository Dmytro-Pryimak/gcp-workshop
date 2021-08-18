from google.cloud import bigquery


def create_schema(schema_text):
    """Create BQ schema based on raw schema definition"""
    schema = []
    for desc in schema_text.split(","):
        pieces = desc.split(":")
        schema.append(bigquery.SchemaField(name=pieces[0], field_type=pieces[1]))
    return schema
