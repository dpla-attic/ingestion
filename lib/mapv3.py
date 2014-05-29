def array_or_item(obj):
    """Generate a Python list for use in JSON Schema oneOf declarations"""
    return [obj, {"$ref": "#/definitions/arrayMinOne", "items": obj}]

MAPV3_SCHEMAS = {
    "collection": {
        "$schema": "http://json-schema.org/draft-04/schema#",
        "title": "DPLA Metadata Application Profile Collection v3 JSON Schema",
        # Do not include the ID until this can be dereferenced
        # "id": "http://api.dp.la/schemas/mapv3/collection#",
        "type": "object",
        "required": ["@id", "title"],
        "properties": {
            "@context": {"type": ["string", "object"]},
            "@id": {"type": "string", "format": "uri"},
            "_id": {"type": "string"},
            "_rev": {"type": "string"},
            "admin": {
               "type": "object",
               "additionalProperties": True
            },
            "description": {"$ref": "#/definitions/arrayOrString"},
            "ingestionSequence": {"type": "number"},
            # do not check ingestDate as the 'date-time' json-schema
            # format, since it requires RFC 3339 conformance and not
            # ISO 8601 conformance
            "ingestDate": {"type": "string"},
            "ingestType": {"type": "string", "enum": ["collection"]},
            "id": {"type": "string"},
            "title": {"$ref": "#/definitions/arrayOrString"}
        },
        "additionalProperties": False,
        "definitions": {
            "arrayMinOne": {
                "type": "array",
                "minItems": 1
            },
            "arrayOrString": {"oneOf": array_or_item({"type": "string"})},
        }
    },
    "item": {
        "$schema": "http://json-schema.org/draft-04/schema#",
        "title": "DPLA Metadata Application Profile Item v3 JSON Schema",
        # Do not include the ID until this can be dereferenced
        # "id": "http://api.dp.la/schemas/mapv3/item#",
        "type": "object",
        "required": [
            "@context",
            "@id",
            "dataProvider",
            "id",
            "ingestionSequence",
            "isShownAt",
            "object",
            "originalRecord",
            "provider",
            "sourceResource"
        ],
        "properties": {
            "@context": {"type": ["string", "object"]},
            "@id": {"type": "string", "format": "uri"},
            "@type": {"type": "string", "enum": ["ore:Aggregation"]},
            "_id": {"type": "string"},
            "_rev": {"type": "string"},
            "admin": {
               "type": "object",
               "additionalProperties": True
            },
            "aggregatedCHO": {"type": "string", "format": "uri"},
            "dataProvider": {"$ref": "#/definitions/arrayOrString"},
            "hasView": {
                "oneOf": array_or_item({"$ref": "#/definitions/hasView"})
            },
            "id": {"type": "string"},
            "ingestionSequence": {"type": "number"},
            # do not check ingestDate as the 'date-time' json-schema
            # format, since it requires RFC 3339 conformance and not
            # ISO 8601 conformance
            "ingestDate": {"type": "string"},
            "ingestType": {"type": "string", "enum": ["item"]},
            "isShownAt": {"type": "string", "format": "uri"},
            "object": {"type": "string", "format": "uri"},
            "originalRecord": {
                "type": "object",
                "additionalProperties": True
            },
            "provider": {
                "type": "object",
                "required": ["@id", "name"],
                "properties":{
                    "@id": {"type": "string", "format": "uri"},
                    "name": {"type": "string"}
                }
            },
            "sourceResource": {"$ref": "#/definitions/sourceResource"}
        },
        "additionalProperties": False,
        "definitions": {
            "arrayMinOne": {
                "type": "array",
                "minItems": 1
            },
            "arrayOrString": {"oneOf": array_or_item({"type": "string"})},
            "collection": {
                "type": "object",
                "required": ["@id", "title"],
                "properties": {
                    "@context": {"type": ["string", "object"]},
                    "@id": {"type": "string", "format": "uri"},
                    "_id": {"type": "string"},
                    "_rev": {"type": "string"},
                    "admin": {
                       "type": "object",
                       "additionalProperties": True
                    },
                    "description": {"$ref": "#/definitions/arrayOrString"},
                    "ingestionSequence": {"type": "number"},
                    # do not check ingestDate as the 'date-time' json-schema
                    # format, since it requires RFC 3339 conformance and not
                    # ISO 8601 conformance
                    "ingestDate": {"type": "string"},
                    "ingestType": {"type": "string", "enum": ["collection"]},
                    "id": {"type": "string"},
                    "title": {"$ref": "#/definitions/arrayOrString"}
                },
                "additionalProperties": False
            },
            "date": {
                "type": "object",
                "properties": {
                    "begin": { "type": ["string", "null"] },
                    "displayDate": { "type": ["string", "null"] },
                    "end": { "type": ["string", "null"] }
                },
                "additionalProperties": False
            },
            "hasView": {
                "type": "object",
                "required": ["@id", "format"],
                "properties": {
                    "@id": {"type": "string", "format": "uri"},
                    "format": {"type": ["string", "null"]},
                    "rights": {"$ref": "#/definitions/arrayOrString"}
                },
                "additionalProperties": False
            },
            "sourceResource": {
                "type": "object",
                "required": [
                    "rights", # Many records are still missing rights
                    "title"
                ],
                "additionalProperties": False,
                "properties": {
                    "@id": {"type": "string", "format": "uri"},
                    "collection": {
                        "oneOf": array_or_item({"$ref": "#/definitions/collection"})
                    },
                    "contributor": {"$ref": "#/definitions/arrayOrString"},
                    "creator": {"$ref": "#/definitions/arrayOrString"},
                    "date": {
                        "oneOf": array_or_item({"$ref": "#/definitions/date"})
                    },
                    "description": {"$ref": "#/definitions/arrayOrString"},
                    "extent": {"$ref": "#/definitions/arrayOrString"},
                    "format": {"$ref": "#/definitions/arrayOrString"},
                    "identifier": {"$ref": "#/definitions/arrayOrString"},
                    "language": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "additionalProperties": False,
                            "required": ["name"],
                            "properties": {
                                "name": { "type": "string" },
                                "iso639_3": {
                                    "type": "string",
                                    "minLength": 3,
                                    "maxLength": 3
                                }
                            }
                        }
                    },
                    "publisher": {"$ref": "#/definitions/arrayOrString"},
                    "relation": {"$ref": "#/definitions/arrayOrString"},
                    "rights": {"$ref": "#/definitions/arrayOrString"},
                    "spatial": {
                        "oneOf": array_or_item({"$ref": "#/definitions/spatial"})
                    },
                    "specType": {"$ref": "#/definitions/arrayOrString"},
                    "stateLocatedIn": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "name": {"type": "string"}
                            }
                        }
                    },
                    "subject": {
                        "$ref": "#/definitions/arrayMinOne",
                        "items": {
                            "type": "object",
                            "properties": {
                                "name": {"type": "string"}
                            },
                        }
                    },
                    "temporal": {"$ref": "#/definitions/arrayOrString"},
                    "title": {"$ref": "#/definitions/arrayOrString"},
                    "type": {
                        "oneOf": array_or_item({"$ref": "#/definitions/type"})
                    }
                }
            },
            "spatial": {
                "type": "object",
                "properties": {
                    "city": {"type": "string"},
                    "coordinates": {"type": "string"},
                    "country": {"type": "string"},
                    "county": {"type": "string"},
                    "name": {"type": "string"},
                    "region": {"type": "string"},
                    "state": {"type": "string"}
                }
            },
            "type": {
                "type": "string",
                "enum": [
                    "collection",
                    "dataset",
                    "image",
                    "interactive resource",
                    "moving image",
                    "service",
                    "sound",
                    "text"
                ]
            }
        }
    }
}