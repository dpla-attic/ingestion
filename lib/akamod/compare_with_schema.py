from amara.thirdparty import json
from akara.services import simple_service
from akara import response
from akara import logger
from dplaingestion.selector import getprop, delprop

@simple_service("POST", "http://purl.org/la/dp/compare_with_schema",
                "compare_with_schema", "application/json")
def comparewithschema(body, ctype):
    """
    Service that accepts a JSON document and removes any fields not listed
    as part of the schema.
    """

    # TODO: Send GET request to API once schema endpoint is created

    try :
        data = json.loads(body)
    except:
        response.code = 500
        response.add_header("content-type", "text/plain")
        return "Unable to parse body as JSON"

    if "_id" not in data or ("sourceResource" not in data and
                             data.get("ingestType") == "item"):
        return body

    type = data.get("ingestType")
    if type:
        props = ["dpla/collection/properties"] if type == "collection" else \
                ["dpla/item/properties",
                 "dpla/item/properties/sourceResource/properties"]
        for prop in props:
            schema_keys = getprop(SCHEMA, prop).keys()

            if "sourceResource" in prop:
                data_keys = data["sourceResource"].keys()
                field_prefix = "sourceResource/"
            else:
                data_keys = data.keys()
                data_keys.remove("_id")
                field_prefix = ""

            # Remove any keys in the document that are not found in the schema
            for field in [k for k in data_keys if k not in schema_keys]:
                field = field_prefix + field
                logger.error("Field %s for %s not found in schema; deleting" %
                             (field, data.get("_id")))
                delprop(data, field)
    else:
        logger.error("Unknown type %s for %s" % (type, data.get("_id")))

    return json.dumps(data)

SCHEMA = {
  "dpla" : {
    "item" : {
      "_meta" : {
        "created" : "2013-05-11 15:24:40 -0400"
      },
      "date_detection" : False,
      "properties" : {
        "@context" : {
          "type" : "object",
          "enabled" : False
        },
        "@id" : {
          "type" : "string",
          "index" : "not_analyzed",
          "omit_norms" : True,
          "index_options" : "docs"
        },
        "_rev" : {
          "type" : "object",
          "enabled" : False
        },
        "admin" : {
          "type" : "object",
          "enabled" : False
        },
        "dataProvider" : {
          "type" : "multi_field",
          "fields" : {
            "dataProvider" : {
              "type" : "string"
            },
            "not_analyzed" : {
              "type" : "string",
              "index" : "not_analyzed",
              "omit_norms" : True,
              "index_options" : "docs",
              "include_in_all" : False
            }
          }
        },
        "hasView" : {
          "properties" : {
            "@id" : {
              "type" : "string",
              "index" : "not_analyzed",
              "omit_norms" : True,
              "index_options" : "docs"
            },
            "format" : {
              "type" : "string",
              "index" : "not_analyzed",
              "omit_norms" : True,
              "index_options" : "docs"
            },
            "rights" : {
              "type" : "string",
              "index" : "not_analyzed",
              "omit_norms" : True,
              "index_options" : "docs"
            }
          }
        },
        "id" : {
          "type" : "string",
          "index" : "not_analyzed",
          "omit_norms" : True,
          "index_options" : "docs"
        },
        "ingestDate" : {
          "type" : "object",
          "enabled" : False
        },
        "ingestType" : {
          "type" : "object",
          "enabled" : False
        },
        "ingestionSequence" : {
          "type" : "long"
        },
        "isPartOf" : {
          "properties" : {
            "@id" : {
              "type" : "string",
              "index" : "not_analyzed",
              "omit_norms" : True,
              "index_options" : "docs"
            },
            "name" : {
              "type" : "multi_field",
              "fields" : {
                "name" : {
                  "type" : "string"
                },
                "not_analyzed" : {
                  "type" : "string",
                  "index" : "not_analyzed",
                  "omit_norms" : True,
                  "index_options" : "docs",
                  "include_in_all" : False
                }
              }
            }
          }
        },
        "isShownAt" : {
          "type" : "string",
          "index" : "not_analyzed",
          "omit_norms" : True,
          "index_options" : "docs"
        },
        "object" : {
          "type" : "string",
          "index" : "not_analyzed",
          "omit_norms" : True,
          "index_options" : "docs"
        },
        "originalRecord" : {
          "type" : "object",
          "enabled" : False
        },
        "provider" : {
          "properties" : {
            "@id" : {
              "type" : "string",
              "index" : "not_analyzed",
              "omit_norms" : True,
              "index_options" : "docs"
            },
            "name" : {
              "type" : "multi_field",
              "fields" : {
                "name" : {
                  "type" : "string"
                },
                "not_analyzed" : {
                  "type" : "string",
                  "index" : "not_analyzed",
                  "omit_norms" : True,
                  "index_options" : "docs",
                  "include_in_all" : False
                }
              }
            }
          }
        },
        "sourceResource" : {
          "properties" : {
            "collection" : {
              "properties" : {
                "@id" : {
                  "type" : "string",
                  "index" : "not_analyzed",
                  "omit_norms" : True,
                  "index_options" : "docs"
                },
                "description" : {
                  "type" : "string"
                },
                "id" : {
                  "type" : "string",
                  "index" : "not_analyzed",
                  "omit_norms" : True,
                  "index_options" : "docs"
                },
                "title" : {
                  "type" : "multi_field",
                  "fields" : {
                    "title" : {
                      "type" : "string"
                    },
                    "not_analyzed" : {
                      "type" : "string",
                      "index" : "not_analyzed",
                      "omit_norms" : True,
                      "index_options" : "docs",
                      "include_in_all" : False
                    }
                  }
                }
              }
            },
            "contributor" : {
              "type" : "string",
              "index" : "not_analyzed",
              "omit_norms" : True,
              "index_options" : "docs"
            },
            "creator" : {
              "type" : "string"
            },
            "date" : {
              "properties" : {
                "begin" : {
                  "type" : "date",
                  "format" : "dateOptionalTime",
                  "null_value" : "-9999"
                },
                "displayDate" : {
                  "type" : "string",
                  "index" : "not_analyzed",
                  "omit_norms" : True,
                  "index_options" : "docs"
                },
                "end" : {
                  "type" : "date",
                  "format" : "dateOptionalTime",
                  "null_value" : "9999"
                }
              }
            },
            "description" : {
              "type" : "string"
            },
            "extent" : {
              "type" : "string",
              "index" : "not_analyzed",
              "omit_norms" : True,
              "index_options" : "docs"
            },
            "format" : {
              "type" : "string",
              "index" : "not_analyzed",
              "omit_norms" : True,
              "index_options" : "docs"
            },
            "id" : {
              "type" : "string",
              "index" : "not_analyzed",
              "omit_norms" : True,
              "index_options" : "docs"
            },
            "identifier" : {
              "type" : "string"
            },
            "isPartOf" : {
              "type" : "object",
              "enabled" : False
            },
            "language" : {
              "properties" : {
                "iso639" : {
                  "type" : "string",
                  "index" : "not_analyzed",
                  "omit_norms" : True,
                  "index_options" : "docs"
                },
                "iso639_3" : {
                  "type" : "string"
                },
                "name" : {
                  "type" : "string",
                  "index" : "not_analyzed",
                  "omit_norms" : True,
                  "index_options" : "docs"
                }
              }
            },
            "publisher" : {
              "type" : "string"
            },
            "relation" : {
              "type" : "string"
            },
            "rights" : {
              "type" : "string"
            },
            "spatial" : {
              "properties" : {
                "city" : {
                  "type" : "multi_field",
                  "fields" : {
                    "city" : {
                      "type" : "string"
                    },
                    "not_analyzed" : {
                      "type" : "string",
                      "index" : "not_analyzed",
                      "omit_norms" : True,
                      "index_options" : "docs",
                      "include_in_all" : False
                    }
                  }
                },
                "coordinates" : {
                  "type" : "geo_point"
                },
                "country" : {
                  "type" : "multi_field",
                  "fields" : {
                    "country" : {
                      "type" : "string"
                    },
                    "not_analyzed" : {
                      "type" : "string",
                      "index" : "not_analyzed",
                      "omit_norms" : True,
                      "index_options" : "docs",
                      "include_in_all" : False
                    }
                  }
                },
                "county" : {
                  "type" : "multi_field",
                  "fields" : {
                    "county" : {
                      "type" : "string"
                    },
                    "not_analyzed" : {
                      "type" : "string",
                      "index" : "not_analyzed",
                      "omit_norms" : True,
                      "index_options" : "docs",
                      "include_in_all" : False
                    }
                  }
                },
                "iso3166-2" : {
                  "type" : "string",
                  "index" : "not_analyzed",
                  "omit_norms" : True,
                  "index_options" : "docs"
                },
                "name" : {
                  "type" : "multi_field",
                  "fields" : {
                    "name" : {
                      "type" : "string"
                    },
                    "not_analyzed" : {
                      "type" : "string",
                      "index" : "not_analyzed",
                      "omit_norms" : True,
                      "index_options" : "docs",
                      "include_in_all" : False
                    }
                  }
                },
                "region" : {
                  "type" : "multi_field",
                  "fields" : {
                    "region" : {
                      "type" : "string"
                    },
                    "not_analyzed" : {
                      "type" : "string",
                      "index" : "not_analyzed",
                      "omit_norms" : True,
                      "index_options" : "docs",
                      "include_in_all" : False
                    }
                  }
                },
                "state" : {
                  "type" : "multi_field",
                  "fields" : {
                    "state" : {
                      "type" : "string"
                    },
                    "not_analyzed" : {
                      "type" : "string",
                      "index" : "not_analyzed",
                      "omit_norms" : True,
                      "index_options" : "docs",
                      "include_in_all" : False
                    }
                  }
                }
              }
            },
            "specType" : {
              "type" : "string"
            },
            "stateLocatedIn" : {
              "properties" : {
                "iso3166-2" : {
                  "type" : "string",
                  "index" : "not_analyzed",
                  "omit_norms" : True,
                  "index_options" : "docs"
                },
                "name" : {
                  "type" : "string",
                  "index" : "not_analyzed",
                  "omit_norms" : True,
                  "index_options" : "docs"
                }
              }
            },
            "subject" : {
              "properties" : {
                "@id" : {
                  "type" : "string",
                  "index" : "not_analyzed",
                  "omit_norms" : True,
                  "index_options" : "docs"
                },
                "@type" : {
                  "type" : "string",
                  "index" : "not_analyzed",
                  "omit_norms" : True,
                  "index_options" : "docs"
                },
                "name" : {
                  "type" : "multi_field",
                  "fields" : {
                    "name" : {
                      "type" : "string"
                    },
                    "not_analyzed" : {
                      "type" : "string",
                      "index" : "not_analyzed",
                      "omit_norms" : True,
                      "index_options" : "docs",
                      "include_in_all" : False
                    }
                  }
                }
              }
            },
            "temporal" : {
              "properties" : {
                "begin" : {
                  "type" : "date",
                  "format" : "dateOptionalTime",
                  "null_value" : "-9999"
                },
                "displayDate" : {
                  "type" : "string"
                },
                "end" : {
                  "type" : "date",
                  "format" : "dateOptionalTime",
                  "null_value" : "9999"
                }
              }
            },
            "title" : {
              "type" : "string"
            },
            "type" : {
              "type" : "string",
              "index" : "not_analyzed",
              "omit_norms" : True,
              "index_options" : "docs"
            }
          }
        }
      }
    },
    "collection" : {
      "_meta" : {
        "created" : "2013-05-11 15:24:40 -0400"
      },
      "date_detection" : False,
      "properties" : {
        "@id" : {
          "type" : "string",
          "index" : "not_analyzed",
          "omit_norms" : True,
          "index_options" : "docs"
        },
        "_rev" : {
          "type" : "object",
          "enabled" : False
        },
        "description" : {
          "type" : "string"
        },
        "id" : {
          "type" : "string",
          "index" : "not_analyzed",
          "omit_norms" : True,
          "index_options" : "docs"
        },
        "ingestDate" : {
          "type" : "object",
          "enabled" : False
        },
        "ingestType" : {
          "type" : "object",
          "enabled" : False
        },
        "ingestionSequence" : {
          "type" : "long"
        },
        "title" : {
          "type" : "string"
        }
      }
    }
  }
}
