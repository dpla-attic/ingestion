
# -*- coding: utf-8 -*-
from akara import module_config, response
from akara import logger
from akara.services import simple_service
from amara.thirdparty import json
from dplaingestion.selector import getprop, setprop, exists
from geopy import geocoders, util
import itertools
import math
import re
from urllib import urlencode
from urllib2 import URLError
from dplaingestion.utilities import iterify, urlopen_with_retries

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

            if place.name:
                coords = parse_coordinates_from_name(place.name)
                if coords:
                    place.coordinates = coords
                    place.name = None
                    place.set_name()

            # Run Geonames enrichment
            place.enrich_geodata(DplaGeonamesGeocoder())

            if not place.validate():
                if not place.set_name():
                    logger.error("Spatial dictionary must have a " +
                                 "'name' property. Could not enhance input " +
                                 "data to include a name property; " +
                                 "record %s" % data["_id"])

            places.append(place)

        values = map(lambda x: x.to_map_json(), Place.merge_related(places))
        setprop(data, prop, values)

    return json.dumps(data)

def haversine(origin, destination):
    '''
    Distance in km between two lat/lng coordinates.
    '''
    lat1, lon1 = map(float, origin)
    lat2, lon2 = map(float, destination)
    radius = 6371 # Radius of the earth in km

    dlat = math.radians(lat2-lat1)
    dlon = math.radians(lon2-lon1)
    a = math.sin(dlat/2) * math.sin(dlat/2) + math.cos(math.radians(lat1)) \
        * math.cos(math.radians(lat2)) * math.sin(dlon/2) * math.sin(dlon/2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    d = radius * c

    return d

def parse_coordinates_from_name(value):
    """
    Attempts to extract coordinate data from a string,
    will recognize coordinates out of range for latitude
    and longitude, and reorder them if needed.
    """
    if _match_for_coordinate(value):
        coords = map(float, value.split(','))
    elif _match_for_transposed_coordinate(value):
        lon, lat = value.split(',')
        coords = map(float, [lat, lon])
    else:
        return
    return floats_to_coordinates(coords)

def floats_to_coordinates(floats):
    """
    Accepts a pair of floating point values representing latitude
    and longitide and returns them in a string format separated by
    a comma.
    """
    return ", ".join(["%s" % str(n) for n in floats])

def _match_for_coordinate(value):
    return re.search(r"^[-+]?([1-8]?\d(\.\d+)?|90(\.0+)?),\s*[-+]?"
                     r"(180(\.0+)?|((1[0-7]\d)|([1-9]?\d))(\.\d+)?)$",
                     value)


def _match_for_transposed_coordinate(value):
    return re.search(r"^[-+]?(180(\.0+)?|((1[0-7]\d)|([1-9]?\d))"
                     r"(\.\d+)?),\s*[-+]?([1-8]?\d(\.\d+)?|90(\.0+)?)$",
                     value)


class DplaGeonamesGeocoder(object):
    resultCache = {}
    base_uri = 'http://ws.geonames.net/'

    def enrich_place(self, place):
        """
        Accepts a Place and attempts to return a geonames enhanced
        replacement. If no enhancement is possible, returns the original
        place. (Fulfills Dpla Geocoder interface.)

        Performs reverse geocoding if a coordinate property is set on Place,
        othewise calls geocode_place to search against name.
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
            new_place = self.geocode_place(place)
            if new_place:
                return new_place

        return place

    def geocode_place(self, place):
        """
        Accepts a Place and searches Geonames for a matching location.

        Privledges states (first-order administrative divisions) and countries,
        with exactly matching text, then privledges US places.

        Enhances Place with geonames hierarchies using geonameId values for
        identifiers of places in the hierarchy. If some hierarchical values
        are already present, will attempt to validate them before accepting
        the new place.
        """
        place.set_name()
        params = {}

        # Whether to register coordinates for a Place, which could be a region
        # that is too large to warrent pinpointing with falsely-precise
        # coordinates that would clutter a map interface that shows points for
        # objects.
        register_coordinates = True

        # TODO: considering filtering on specific feature codes
        if place.name in [place.country, place.region]:
            params['featureClass'] = 'A'
            register_coordinates = False
        elif place.name == place.state:
            params['featureClass'] = 'A'
            params['countryBias'] = 'US'
        elif place.name == place.city:
            params['featureClass'] = 'P'
            params['countryBias'] = 'US'
        else:
            params['countryBias'] = 'US'

        interpretations = self._name_search(place.name, params)

        # If we just got a name, and no fields identifying it specifically as
        # a city, state, or country, do some more lookups to see if we can
        # identify it more accurately
        #
        # FIXME: This seems unnecessary. A name search returns multiple
        # interpretations that can be iterated over.
        #
        if not 'featureClass' in params.keys():

            # Gather interpretations that consider the place name as that of
            # a state  (REMOTE API CALL)
            st_interps = self._name_search(place.name, {'featureCode': 'ADM1'})

            # Gather interpretations that consider the place name as that of
            # a country  (REMOTE API CALL)
            co_interps = self._name_search(place.name, {'featureCode': 'PCLI'})

            # Combine all of these, including the unrestricted interpretations
            # of the name, into one list
            interpretations = st_interps + co_interps + interpretations

        if not interpretations:
            return place

        candidates = []

        for interp in interpretations:

            candidate_place = Place({
                'name': interp.get('name'),
                'uri': interp.get('geonameId'),
                'country': interp.get('countryName'),
                'feature_type': interp.get('fcode')
            })

            if register_coordinates:
                candidate_place.coordinates = "%s, %s" % (interp['lat'],
                                                          interp['lng'])

            parent_features = self.build_hierarchy(interp['geonameId'],
                                                   ["PCLI", # Country
                                                    "ADM1", # State
                                                    "ADM2", # County
                                                    "PPLA"]) # City

            for feature in parent_features:
                if feature['fcode'] == 'PCLI':    # country
                    candidate_place.country = feature["name"]
                    candidate_place.country_uri = feature["geonameId"]
                elif feature['fcode'] == 'ADM1':  # state
                    candidate_place.state = feature["name"]
                    candidate_place.state_uri = feature["geonameId"]
                elif feature['fcode'] == 'ADM2':  # county
                    candidate_place.county = feature["name"]
                    candidate_place.county_uri = feature["geonameId"]

            # Move on to the next interpretation if its name and the one we
            # presented for lookup do not have anything in common.
            if (candidate_place.name.lower() not in place.name.lower()) and \
               (place.name.lower() not in candidate_place.name.lower()):
                continue

            # If the place we presented for lookup is labeled with a country,
            # move on to the next candidate if the candidate's country is not
            # the same as our place's country.
            if place.country and (place.country.lower() != \
                                  candidate_place.country.lower()):
                continue

            # If the place we presented for lookup is labeled with a state,
            # move on the the next candidate if the candidate's state is not
            # the same as our place's state.
            if place.state and (place.state.lower() != \
                                candidate_place.state.lower()):
                continue

            # As above, move on if know our place's county and the candidate's
            # county does not match.
            if place.county and (place.county.lower() != \
                                 candidate_place.county.lower()):
                continue

            # If the interpretation's name is exactly the same as the name
            # we started out with, count it as as the right one and return
            # the candidate place, since we've weeded out the possibility that
            # it's in the wrong county, state, or country.
            if interp['name'].lower() == place.name.lower():
                return candidate_place

            # Otherwise, we're still not sure, so add it to the list of
            # candidates.
            candidates.append(candidate_place)

        # Pick the best candidate of possibly many not-ideal choices:
        #
        # Return the one we have if that's all we have
        if len(candidates) == 1:
            return candidates[0]
        elif len(candidates) > 1:
            for candidate in candidates:
                # Return the one that looks like some kind of "political
                # entity," i.e. country or section thereof -- theoretically the
                # more general one -- if we can.
                if candidate.feature_type and 'PCL' in candidate.feature_type:
                    return candidate

            # Oh, well, just return the first one.
            return candidates[0]
        else:
            # If there were no candidates, we're just returning the place that
            # we started with.
            return place

    def reverse_geocode(self, lat, lng):
        """
        Accepts latitude and longitude values and returns a geonames place
        that matches their value.
        """
        params = { "lat": lat,
                   "lng": lng,
                   "username": module_config().get("geonames_username"),
                   "token": module_config().get("geonames_token") }
        url = DplaGeonamesGeocoder.base_uri + \
              "findNearbyJSON?%s" % urlencode(params)
        if (url not in DplaGeonamesGeocoder.resultCache):
            result = DplaGeonamesGeocoder._get_result(url)
            if ("geonames" in result \
                and len(result["geonames"]) > 0):
                DplaGeonamesGeocoder.resultCache[url] = result["geonames"][0]
            else:
                logger.error("Could not reverse geocode (%s, %s)" %
                             (lat, lng,))
                return None

        return DplaGeonamesGeocoder.resultCache[url]

    def build_hierarchy(self, geonames_id, fcodes=None):
        """
        Accepts a geonames id and fetches a hierarchy of features from
        the API, returning them as a list of geoname items.
        """
        hierarchy = []
        params = { "geonameId": str(geonames_id),
                   "username": module_config().get("geonames_username"),
                   "token": module_config().get("geonames_token") }
        url = DplaGeonamesGeocoder.base_uri + \
              "hierarchyJSON?%s" % urlencode(params)
        if (url not in DplaGeonamesGeocoder.resultCache):
            result = DplaGeonamesGeocoder._get_result(url)  # REMOTE API CALL
            if result.get('geonames'):
                DplaGeonamesGeocoder.resultCache[url] = result["geonames"]
            else:
                return hierarchy
        # Return only the requested fcodes
        for feature in DplaGeonamesGeocoder.resultCache.get(url):
            if not fcodes or feature["fcode"] in fcodes:
                hierarchy.append(feature)

        return hierarchy

    def _name_search(self, name, params={}):
        """Search GeoNames for a given name and return a dict of the result"""
        defaults = { "q": name.encode("utf8"),
                     "maxRows": 15,
                     "username": module_config().get("geonames_username"),
                     "token": module_config().get("geonames_token") }
        params = dict(defaults.items() + params.items())

        url = DplaGeonamesGeocoder.base_uri + "searchJSON?%s" % \
              urlencode(params)
        if (url not in DplaGeonamesGeocoder.resultCache):
            result = DplaGeonamesGeocoder._get_result(url)
            if result.get('geonames'):
                DplaGeonamesGeocoder.resultCache[url] = result["geonames"]
            else:
                return []
        return DplaGeonamesGeocoder.resultCache[url]

    def _place_from_coordinates(self, lat, lng):
        """
        Reverse-geocode this location to find country, state, and
        county parent places.
        """

        place = Place()
        geonames_item = self.reverse_geocode(lat, lng)
        if not geonames_item:
            return
        hierarchy = self.build_hierarchy(geonames_item['geonameId'],
                                         ["PCLI", # Country
                                          "ADM1", # State
                                          "ADM2", # County
                                          "PPLA"])# City

        for feature in hierarchy:
            if feature.get("fcode"):
                if ("PCLI" == feature["fcode"]):
                    place.country = feature["name"]
                    place.uri = feature["geonameId"]
                elif ("ADM1" == feature["fcode"]):
                    place.state = feature["name"]
                    place.uri = feature["geonameId"]
                elif ("ADM2" == feature["fcode"]):
                    place.county = feature["name"]
                    place.uri = feature["geonameId"]
                elif ("ADM2" == feature["fcode"]):
                    place.city = feature["name"]
                    place.uri = feature["geonameId"]

            # Deterine how close we are to the original coordinates, to
            # see if this is the place that was geocoded and we should
            # stop adding more specificity (e.g. if the record
            # specified "South Carolina", we don't want to include the
            # county that is located at the coordinates of South
            # Carolina. We use increasing tolerance levels to account
            # for differences between input and Geonames coordinates.
            d = haversine((lat, lng), (feature["lat"], feature["lng"]))

            # Country tolerance (input/Geonames 49.9km off)
            country_tolerance_met = ("PCLI" == feature.get("fcode") and
                                             d < 50)
            # State tolerance
            state_tolerance_met = ("ADM1" == feature.get("fcode") and d < 15)

            if (country_tolerance_met or state_tolerance_met):
                return place

        return place

    @staticmethod
    def _get_result(url):
        try:
            logger.debug("hitting %s" % url)
            result = json.loads(util.decode_page(urlopen_with_retries(url)))
            return result
        except URLError, e:
            logger.error("GeoNames error, could not open URL: %s, error: %s" %
                         (url, e))
            return {}

class Place:
    def __init__(self, spatial={}):
        self.name = spatial.get("name")
        self.city = spatial.get("city")
        self.state = spatial.get("state")
        self.state_uri = spatial.get("state_uri")
        self.county = spatial.get("county")
        self.county_uri = spatial.get("county_uri")
        self.country = spatial.get("country")
        self.country_uri = spatial.get("country_uri")
        self.region = spatial.get("region")
        self.coordinates = spatial.get("coordinates")
        self.feature_type = spatial.get("feature_type")
        self.uri = spatial.get("uri")

    def map_fields(self):
        """
        Returns a list of the fields to be included in DPLA MAP's
        serializations of Place.
        """
        fields = ['name', 'city', 'state', 'country', 'region', 'county']
        if (not self.feature_type) or (not 'PCL' in self.feature_type):
            fields.append('coordinates')
        return fields

    def set_name(self):
        """
        Returns the name property. If none is set, sets it to the smallest
        available geographic division label.
        """
        if self.name:
            return self.name

        prop_order = ["city", "county", "state", "country", "region"]
        for prop in prop_order:
            if getattr(self, prop):
                self.name = getattr(self, prop)
                return self.name
        return self.name

    def to_map_json(self):
        """
        Serializes for inclusion in DPLA MAP JSON.
        """
        return dict((k,v) for k,v in self.__dict__.iteritems() if v and (k in self.map_fields()))

    def validate(self):
        return self.name

    def refine_coordinates(self):
        """
        Validates coordinate values through parse_coordinates_from_name().
        Sets and returns new coordinate values if needed.
        """
        coords = parse_coordinates_from_name(self.coordinates)
        self.coordinates = coords if coords else none
        return self.coordinates

    def enrich_geodata(self, geocoder):
        """
        Accepts a geocoder with an `enrich_place()` method.

        Calls the enrichment method for the geocoder passed and
        attempts to merge the place returned into self.
        """
        try:
            coded_place = geocoder.enrich_place(self)
        except Exception, e:
            logger.error("%s failed on place '%s', error: %s" %
                (str(geocoder), self.name, e))
            return

        # Add only non-existing properties
        # TODO: get more sophisticated about update
        for prop in coded_place.__dict__.keys():
            if not getattr(self, prop):
                setattr(self, prop, getattr(coded_place, prop))
        if not self.uri:
            self.uri = coded_place.uri
            self.country_uri = coded_place.country_uri
            self.state_uri = coded_place.state_uri
            self.county_uri = coded_place.county_uri

    @staticmethod
    def merge_related(places):
        """
        Removes places that are included redundantly.
        Accepts a list of Place objects and uses their URIs to remove
        places that are redundant in light of a smaller geographic division.

        e.g. given 'Los Angeles' and 'Los Angeles County', the latter will be
        removed "merging" it into Los Angeles.
        """
        for place in places[:]:
            if not place in places:
                continue
            for compare_place in places[:]:
                if compare_place == place:
                    continue
                if Place._is_parent_of(place, compare_place):
                    try:
                        places.remove(place)
                    except ValueError:
                        pass
        return places

    @staticmethod
    def _is_parent_of(p1, p2):
        """
        Accepts two Place objects and checks whether the first is
        a parent in the location hierarchy of the second.

        Returns a boolean.
        """
        if not ((not p1.uri) or (not p2.uri)):
            parents = [p2.country_uri, p2.state_uri, p2.county_uri]
            if p1.uri in parents:
                return True
        return False
