from dplaingestion.mappers.edan_mapper import EDANMapper


def test_edan_strip_html_tags():
    provider_data = {
        "freetext": {
            "notes": [
              {
                "@label": "Description (Brief)",
                "#text": "<b>Marked</b> <i>up</i>"
              }
            ]
          },
          "descriptiveNonRepeating": {
              "title": {
                  "@label": "Title",
                  "#text": "Booke of &#254;is and &#xFE;at."
              }
          }
    }
    em = EDANMapper(provider_data)
    em.map_title()
    em.map_description()
    expected = {
        'sourceResource': {
            'description': [u'Marked up'],
            'title': u'Booke of \xfeis and \xfeat.'
            }
        }
    assert em.mapped_data == expected
