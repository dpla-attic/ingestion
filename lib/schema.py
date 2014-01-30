schema = {
  "collection": {
    "date_detection": False,
    "properties": {
      "@id": {
        "type": "string",
        "index": "not_analyzed",
        "sort": "field"
      },
      "id": {
        "type": "string",
        "index": "not_analyzed",
        "sort": "field"
      },
      "description": {
        "type": "string"
      },
      "title": {
        "type": "multi_field",
        "fields": {
          "title": {
            "type": "string",
            "sort": "multi_field"
          },
          "not_analyzed": {
            "type": "string",
            "index": "not_analyzed",
            "sort": "script",
            "facet": True
          }
        }
      },
      "ingestionSequence": {
        "enabled": False
      },
      "ingestType": {
        "enabled": False
      },
      "ingestDate": {
        "enabled": False
      },
      "_rev": {
        "enabled": False
      }
    }
  },
  "item": {
    "date_detection": False,
    "properties": {
      "@id": {
        "type": "string",
        "index": "not_analyzed",
        "sort": "field"
      },
      "admin": {
        "properties": {
          "sourceResource": {
            "properties": {
              "title": {
                "type": "string",
                "analyzer": "canonical_sort",
                "null_value": "zzzzzzzz"
              }
            }
          },
          "ingestType": {
            "enabled": False
          },
          "ingestDate": {
            "type": "date"
          }
        }
      },
      "id": {
        "type": "string",
        "index": "not_analyzed",
        "sort": "field"
      },
      "sourceResource": {
        "properties": {
          "identifier": {
            "type": "string",
            "index": "not_analyzed",
            "sort": "field"
          },
          "collection": {
            "properties": {
              "@id": {
                "type": "string",
                "index": "not_analyzed",
                "sort": "field",
                "facet": True
              },
              "id": {
                "type": "string",
                "index": "not_analyzed",
                "sort": "field",
                "facet": True
              },
              "description": {
                "type": "string"
              },
              "title": {
                "type": "multi_field",
                "fields": {
                  "title": {
                    "type": "string",
                    "sort": "multi_field"
                  },
                  "not_analyzed": {
                    "type": "string",
                    "index": "not_analyzed",
                    "sort": "script",
                    "facet": True
                  }
                }
              }
            }
          },
          "contributor": {
            "type": "string",
            "index": "not_analyzed",
            "sort": "field",
            "facet": True
          },
          "creator": {
            "type": "string"
          },
          "date": {
            "properties": {
              "displayDate": {
                "type": "string",
                "index": "not_analyzed"
              },
              "begin": {
                "type": "multi_field",
                "fields": {
                  "begin": {
                    "type": "date",
                    "sort": "multi_field",
                    "null_value": "-9999"
                  },
                  "not_analyzed": {
                    "type": "date",
                    "sort": "field",
                    "facet": True
                  }
                }
              },
              "end": {
                "type": "multi_field",
                "fields": {
                  "end": {
                    "type": "date",
                    "sort": "multi_field",
                    "null_value": "9999"
                  },
                  "not_analyzed": {
                    "type": "date",
                    "sort": "field",
                    "facet": True
                  }
                }
              }
            }
          },
          "description": {
            "type": "string"
          },
          "extent": {
            "type": "string",
            "index": "not_analyzed",
            "sort": "field"
          },
          "isPartOf": {
            "enabled": False
          },
          "language": {
            "properties": {
              "name": {
                "type": "string",
                "index": "not_analyzed",
                "sort": "field",
                "facet": True
              },
              "iso639": {
                "type": "string",
                "index": "not_analyzed",
                "sort": "field",
                "facet": True
              }
            }
          },
          "format": {
            "type": "string",
            "index": "not_analyzed",
            "sort": "field",
            "facet": True
          },
          "publisher": {
            "type": "multi_field",
            "fields": {
              "publisher": {
                "type": "string"
              },
              "not_analyzed": {
                "type": "string",
                "index": "not_analyzed",
                "facet": True
              }
            }
          },
          "rights": {
            "type": "string"
          },
          "relation": {
            "type": "string"
          },
          "spatial": {
            "properties": {
              "name": {
                "type": "multi_field",
                "fields": {
                  "name": {
                    "type": "string",
                    "sort": "multi_field"
                  },
                  "not_analyzed": {
                    "type": "string",
                    "index": "not_analyzed",
                    "sort": "script",
                    "facet": True
                  }
                }
              },
              "country": {
                "type": "multi_field",
                "fields": {
                  "country": {
                    "type": "string",
                    "sort": "multi_field"
                  },
                  "not_analyzed": {
                    "type": "string",
                    "index": "not_analyzed",
                    "sort": "script",
                    "facet": True
                  }
                }
              },
              "region": {
                "type": "multi_field",
                "fields": {
                  "region": {
                    "type": "string",
                    "sort": "multi_field"
                  },
                  "not_analyzed": {
                    "type": "string",
                    "index": "not_analyzed",
                    "sort": "script",
                    "facet": True
                  }
                }
              },
              "county": {
                "type": "multi_field",
                "fields": {
                  "county": {
                    "type": "string",
                    "sort": "multi_field"
                  },
                  "not_analyzed": {
                    "type": "string",
                    "index": "not_analyzed",
                    "sort": "script",
                    "facet": True
                  }
                }
              },
              "state": {
                "type": "multi_field",
                "fields": {
                  "state": {
                    "type": "string",
                    "sort": "multi_field"
                  },
                  "not_analyzed": {
                    "type": "string",
                    "index": "not_analyzed",
                    "sort": "script",
                    "facet": True
                  }
                }
              },
              "city": {
                "type": "multi_field",
                "fields": {
                  "city": {
                    "type": "string",
                    "sort": "multi_field"
                  },
                  "not_analyzed": {
                    "type": "string",
                    "index": "not_analyzed",
                    "sort": "script",
                    "facet": True
                  }
                }
              },
              "iso3166-2": {
                "type": "string",
                "index": "not_analyzed",
                "sort": "field",
                "facet": True
              },
              "coordinates": {
                "type": "geo_point",
                "index": "not_analyzed",
                "sort": "geo_distance",
                "facet": True
              }
            }
          },
          "specType": {
            "type": "string",
            "index": "not_analyzed",
            "sort": "field",
            "facet": True
          },
          "stateLocatedIn": {
            "properties": {
              "name": {
                "type": "string",
                "index": "not_analyzed",
                "sort": "field",
                "facet": True
              },
              "iso3166-2": {
                "type": "string",
                "index": "not_analyzed",
                "sort": "field",
                "facet": True
              }
            }
          },
          "subject": {
            "properties": {
              "@id": {
                "type": "string",
                "index": "not_analyzed",
                "sort": "field",
                "facet": True
              },
              "@type": {
                "type": "string",
                "index": "not_analyzed",
                "sort": "field"
              },
              "name": {
                "type": "multi_field",
                "fields": {
                  "name": {
                    "type": "string",
                    "sort": "multi_field"
                  },
                  "not_analyzed": {
                    "type": "string",
                    "index": "not_analyzed",
                    "sort": "script",
                    "facet": True
                  }
                }
              }
            }
          },
          "temporal": {
            "properties": {
              "begin": {
                "type": "multi_field",
                "fields": {
                  "begin": {
                    "type": "date",
                    "sort": "multi_field",
                    "null_value": "-9999"
                  },
                  "not_analyzed": {
                    "type": "date",
                    "sort": "field",
                    "facet": True
                  }
                }
              },
              "end": {
                "type": "multi_field",
                "fields": {
                  "end": {
                    "type": "date",
                    "sort": "multi_field",
                    "null_value": "9999"
                  },
                  "not_analyzed": {
                    "type": "date",
                    "sort": "field",
                    "facet": True
                  }
                }
              }
            }
          },
          "title": {
            "type": "string",
            "sort": "shadow"
          },
          "type": {
            "type": "string",
            "index": "not_analyzed",
            "sort": "field",
            "facet": True
          }
        }
      },
      "dataProvider": {
        "type": "multi_field",
        "fields": {
          "dataProvider": {
            "type": "string",
            "sort": "multi_field"
          },
          "not_analyzed": {
            "type": "string",
            "index": "not_analyzed",
            "sort": "script",
            "facet": True
          }
        }
      },
      "hasView": {
        "properties": {
          "@id": {
            "type": "string",
            "index": "not_analyzed",
            "sort": "field",
            "facet": True
          },
          "format": {
            "type": "string",
            "index": "not_analyzed",
            "sort": "script",
            "facet": True
          },
          "rights": {
            "type": "string",
            "index": "not_analyzed"
          }
        }
      },
      "isPartOf": {
        "properties": {
          "@id": {
            "type": "string",
            "index": "not_analyzed",
            "sort": "field",
            "facet": True
          },
          "name": {
            "type": "multi_field",
            "fields": {
              "name": {
                "type": "string",
                "sort": "multi_field"
              },
              "not_analyzed": {
                "type": "string",
                "index": "not_analyzed",
                "sort": "script",
                "facet": True
              }
            }
          }
        }
      },
      "isShownAt": {
        "type": "string",
        "index": "not_analyzed",
        "sort": "field"
      },
      "object": {
        "type": "string",
        "index": "not_analyzed",
        "sort": "field"
      },
      "provider": {
        "properties": {
          "@id": {
            "type": "string",
            "index": "not_analyzed",
            "sort": "field",
            "facet": True
          },
          "name": {
            "type": "multi_field",
            "fields": {
              "name": {
                "type": "string",
                "sort": "multi_field"
              },
              "not_analyzed": {
                "type": "string",
                "index": "not_analyzed",
                "sort": "script",
                "facet": True
              }
            }
          }
        }
      },
      "@context": {
        "type": "object",
        "enabled": False
      },
      "originalRecord": {
        "type": "object",
        "enabled": False
      },
      "ingestionSequence": {
        "enabled": False
      },
      "ingestType": {
        "enabled": False
      },
      "ingestDate": {
        "enabled": False
      },
      "_rev": {
        "enabled": False
      }
    }
  }
}
