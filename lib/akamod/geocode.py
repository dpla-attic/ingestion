
# -*- coding: utf-8 -*-
from akara import module_config, response
from akara import logger
from akara.services import simple_service
from amara.thirdparty import json
from dplaingestion.selector import getprop, setprop, exists
from geopy import point
import re
import traceback
from urllib import urlencode
from urllib2 import urlopen, URLError
from dplaingestion.utilities import iterify


@simple_service('POST', 'http://purl.org/la/dp/geocode', 'geocode',
                'application/json')
def geocode(body, ctype, prop="sourceResource/spatial", newprop='coordinates'):
    '''
    Adds geocode data to the record coming as follows:

    1. If the coordinates property does not exist, attempt to extract it from
       name.
    2. Run GeoNames enrichment, reverse encoding coordinate values to identify,
       parent features, or (if none exist) searching for name values. Put
       parent features in appropriate state/country values.
    3. If we still haven't identified the place, use Bing to get lat/long
       values. If one is found, pass the coordinates through Geonames again
       to identify parent features.
    4. Add any non-existing features to the spatial dictionary.
    '''
    try:
        data = json.loads(body)
    except:
        response.code = 500
        response.add_header('content-type','text/plain')
        return "Unable to parse body as JSON"

    if (not exists(data, prop)):
        pass
    else:
        logger.debug("Geocoding %s" % data["_id"])
        value = getprop(data, prop)
        places = []

        for v in iterify(value):
            if not isinstance(v, dict):
                logger.error("Spatial value must be a dictionary; record %s" %
                             data["_id"])
                continue
            place = Place(v)
            place.enrich_geodata(TwofishesGeocoder())
            places.append(place)

        values = map(lambda x: x.to_map_json(), places)
        setprop(data, prop, values)

    return json.dumps(data)


class Place:
    MAP_FIELDS = ['name', 'city', 'state', 'county', 'country', 'region',
                  'coordinates']

    def __init__(self, spatial={}):
        for name in self.MAP_FIELDS:
            setattr(self,
                    name,
                    self._urlencode_safe_str(spatial.get(name)))
        if self.name and not self.coordinates:
            try:
                p = point.Point(self.name)
                # Latitude and longitude by List index
                coords = ', '.join([str(p[0]), str(p[1])])
            except ValueError:
                # If there are no legitimate coordinates in the Place name
                coords = ""
            if coords:
                self.coordinates = coords
                self.name = None
                self.set_name()

    def set_name(self):
        """
        Modify and return our name property, after cleaning it up. If none is
        set, initialize it to the smallest available geographic division label.
        """
        if self.name:
            # If *only* "United States or its variants" (re.search searches
            # from the beginning of the string), normalize it
            if re.search(ur' *(United States(?: of America)?|USA)$',
                         self.name):
                self.name = 'United States'
            self.name = self._urlencode_safe_str(self.name)
            return self.name

        prop_order = ["city", "county", "state", "country", "region"]
        for prop in prop_order:
            if getattr(self, prop):
                self.name = self._urlencode_safe_str(getattr(self, prop))
                return self.name
        return self.name

    def _urlencode_safe_str(self, s):
        if isinstance(s, unicode):
            return s.encode('utf-8', 'ignore')
        else:
            return s

    def to_map_json(self):
        """
        Serializes for inclusion in DPLA MAP JSON.
        """
        return dict((k,v) for k,v in self.__dict__.iteritems()
                    if v and (k in self.MAP_FIELDS))

    def enrich_geodata(self, geocoder):
        """Enhance my data with the given geocoder.

        Attempt to merge the geocoder's place with myself.
        """
        try:
            coded_place = geocoder.enrich_place(self)
            if coded_place == self:
                return
        except Exception, e:
            tb = traceback.format_exc(5)
            logger.error("%s failed on place '%s', error: %s\n%s" %
                (str(geocoder), self.name, e, tb))

            return

        # Add only non-existing properties
        for prop in coded_place.__dict__.keys():
            if not getattr(self, prop):
                setattr(self, prop, getattr(coded_place, prop))


class TwofishesGeocoder():
    """Geocoder that uses Twofishes"""

    # A subset of the WOE Types above that we want to try to apply to
    # sourceResource.spatial properties. Our "region" spatial property is
    # not precisely defined and does not correspond cleanly to any of the WOE
    # Types.
    PROP_FOR_WOE_TYPE = {7: 'city', 8: 'state', 9: 'county', 12: 'country'}

    def __init__(self):
        self.base_url = module_config().get('twofishes_base_url')
        self.params = {'lang': 'en',
                       'responseIncludes': 'PARENTS,DISPLAY_NAME',
                       'maxInterpretations': 1}

    def enrich_place(self, place):
        """Take a Place and return an enriched replacement, if possible.

        If no enhancement is possible, return the original place.

        Perform a reverse lookup if a coordinate property is set on Place,
        othewise do a forward lookup with the Place's contents.
        """
        if place.coordinates:
            try:
                lat, lng = place.coordinates.split(',')
            except:
                return place
            new_place = self._place_from_coordinates(lat, lng)
            if new_place:
                return new_place
        else:
            place.set_name()
            new_place = self._geocode_place(place)
            if new_place:
                return new_place
        return place

    def _place_from_coordinates(self, lat, lng):
        """Given coordinates, return a Place or None if there is no match

        See self.enrich_place()
        """
        interpretation = self._reverse_interpretation(lat, lng)
        if interpretation:
            return Place(self._place_features(interpretation))
        else:
            return None

    def _geocode_place(self, place):
        """Enhance a given Place with a forward Twofishes lookup"""
        interpretation = self._forward_interpretation(place)
        if interpretation:
            return Place(self._place_features(interpretation))
        else:
            return None

    def _place_features(self, interpretation):
        """Given a Twofishes interpretation, return a dictionary that expresses
        the Place's properties

        See self._place_from_coordinates()
        """
        rv = {'name': interpretation['feature']['displayName']}
        # Interpretation WOE Type will be e.g. 'city', 'county', etc...
        i_type = self.PROP_FOR_WOE_TYPE.get(
            interpretation['feature']['woeType'], None)
        if i_type:
            rv[i_type] = interpretation['feature']['name']
            if i_type != 'country':
                # Assign coordinates if it's not a country.
                place_center = interpretation['feature']['geometry']['center']
                coords = ', '.join([str(place_center['lat']),
                                    str(place_center['lng'])])
                rv['coordinates'] = coords
        rv.update(dict({(self.PROP_FOR_WOE_TYPE[p['woeType']],
                         p['name'])
                         for p in interpretation['parents']
                         if p['woeType'] in self.PROP_FOR_WOE_TYPE}))
        return rv

    def _reverse_interpretation(self, lat, lng):
        """Given coordinates, return the best Twofishes interpretation.

        The interpretation is a dictionary of the first object of the
        Twofishes 'interpretations' array in the response.

        An empty dictionary is returned if there's no good result.
        """
        coords = ','.join([lat, lng])
        our_params = {'ll': coords}
        return self._lookup_data(our_params)

    def _forward_interpretation(self, place):
        """Given a search term, return a Twofishes interpretation"""
        our_params = {'query': self._query_phrase(place)}
        return self._lookup_data(our_params)

    def _lookup_data(self, xtra_params):
        """Given query parameters, return a Twofishes interpretation"""
        url = self._url(xtra_params)
        try:
            return self._twofishes_data(url)['interpretations'][0]
        except KeyError as e:
            return {}

    def _url(self, params):
        params.update(self.params)
        return "%s?%s" % (self.base_url, urlencode(params))

    def _twofishes_data(self, url):
        """Return a dict of Twofishes data for the given URL.

        Rely on the response being Unicode-encoded JSON.
        """
        logger.debug("GET %s" % url)
        try:
            response = urlopen(url, None, 2)
            http_status = response.getcode()
            if http_status != 200:
                logger.error("Got status %d from %s" % (http_status, url))
                return {}
            return json.loads(response.read())
        except URLError as e:
            logger.error("Could not open %s (%s)" % (url, e))
            return {}
        except Exception as e:
            logger.error("Unexpected exception from %s: %e" % (url, e))
            return {}

    def _query_phrase(self, place):
        """Construct a good string to use for the 'query' search parameter

        See also https://github.com/foursquare/fsqio/blob/69c8e68a580b5bb4a94787735541a64ed5bf2676/src/jvm/io/fsq/twofishes/server/GeocodeImpl.scala#L35-L60
        """
        features = filter(None, [place.city, place.county,
                                 place.state])
        ok_features = [f for f in features if f not in place.name]
        return " ".join([place.name] + ok_features)
