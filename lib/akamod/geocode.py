
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
            bing_geocode = True
            if not isinstance(v, dict):
                logger.error("Spatial value must be a dictionary; record %s" %
                             data["_id"])
                continue

            place = Place(v)

            if place.name:
                coords = get_coordinates(place.name)
                if coords:
                    place.coordinates = coords
                    place.name = None
                    place.set_name()

            # Run Geonames enrichment to do initial search
            place.enrich_geodata(DplaGeonamesGeocoder())

            # Don't enrich with geodata if place is 'United States'
            pattern = ur" *(United States(?!-)|États-Unis|USA)"
            if (place.name and re.search(pattern, place.name)):
                bing_geocode = False

            if bing_geocode:
                # Attempt to find this item's lat/lng coordinates
                if not place.coordinates:
                    api_key = module_config().get("bing_api_key")
                    place.enrich_geodata(DplaBingGeocoder(api_key=api_key))
                    # rerun geonames enrichment with new coordinates
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

def get_coordinates(value):
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


class Address:
    def __init__(self, spatial):
        self.name = spatial.get("name")
        self.city = spatial.get("city")
        self.county = spatial.get("county")
        self.region = spatial.get("region")
        self.state = spatial.get("state")
        self.country = spatial.get("country")

    def get_candidates(self):
        if (self.name):
            # TODO: Add state if it does not exist already
            yield self._clean(self.name)

        if (self.city):
            yield self._clean(self._get_candidate(self.city))
        if (self.county):
            yield self._clean(self._get_candidate(self.county))
        if (self.region):
            yield self._clean(self._get_candidate(self.region))
        if (self.state):
            yield self._clean(self._get_candidate())
        if (self.country):
            yield self._clean(self.country)

    def _clean(self, value):
        # Remove characters that confuse Bing
        if (re.match("[0-9]+\.?[0-9]*[NS].+[0-9]+\.?[0-9]*[EW]", value)):
            # This looks like a lat/lng, keep the decimal point
            return value

        return value.translate(dict.fromkeys(map(ord, ".()")))

    def _get_candidate(self, value = ""):
        searchTokens = []

        if (value):
            searchTokens.append(value)

        if (self.state):
            searchTokens.append(self.state)
        if (self.country):
            country = self.country

            # Convert country to 2-character codes
            # TODO: Move to enrich-location
            if ("united states" == country.lower()):
                country = "US"

            searchTokens.append(country)

        return ", ".join(searchTokens)


class DplaBingGeocoder(geocoders.Bing):
    countryBBoxCache = {}
    resultCache = {}

    def __init__(self, **kwargs):
        super(DplaBingGeocoder, self).__init__(**kwargs)

    def enrich_place(self, place):
        """
        Accepts a place and returns a new one with coordinates
        added as identified by Bing.

        In a better implementation of this geocoder
        this function would try to return a fully updated
        version of the place.

        As it stands, it simply provides wrapper around the
        geocode_spatial function to provide a common interface
        between geocoders and places.
        """

        coords = self.geocode_spatial(place.to_map_json())
        if coords:
            place.coordinates = floats_to_coordinates(coords)

        return place

    def geocode_spatial(self, spatial):
        '''
        Accepts a dictionary and attempts to return a set
        of coordinates in format [latitude, longitude] that
        match the place.
        '''
        if (not self.api_key):
            logger.warn("No API key set for Bing " +
                        "(use bing_api_key configuration key)")
            return None

        address = Address(spatial)
        for candidate in address.get_candidates():
            # See if this address candidate exists in our cache
            if (candidate not in DplaBingGeocoder.resultCache):
                results = self._fetch_results(candidate)
                DplaBingGeocoder.resultCache[candidate] = list(results)

            # Require that a single match, or closely grouped matches be
            # returned to avoid bad geocoding results
            candidates = len(DplaBingGeocoder.resultCache[candidate])
            closely_grouped_results = self._are_closely_grouped_results(
                                        DplaBingGeocoder.resultCache[candidate]
                                        )
            if (candidates == 1 or closely_grouped_results):
                result = DplaBingGeocoder.resultCache[candidate][0]
                coordinates = (result["geocodePoints"][0]["coordinates"][0],
                               result["geocodePoints"][0]["coordinates"][1])
                valid_result = True

                # If we have a specified country, perform a sanity check that
                # the returned coordinates is within the country's bounding box
                if (address.country and "countryRegion" in result["address"]):
                    bbox_result = self._is_in_country(coordinates,
                                                      address.country)

                    # If we can't get a country's bbox, assume that we have a
                    # good result
                    if (bbox_result is not None):
                        valid_result = bbox_result
                        if (not valid_result):
                            msg = "Geocode result [%s] " % result["name"] + \
                                  "not in the correct country " + \
                                  "[%s], ignoring" % address.country
                            logger.debug(msg)

                if (valid_result):
                    if ("name" in spatial):
                        logger.debug("Geocode result: %s => %s (%s)" %
                                     (spatial["name"], result["name"],
                                      result["point"]["coordinates"],))
                    else:
                        logger.debug("Geocode result: %s => %s (%s)" %
                                     (spatial, result["name"],
                                      result["point"]["coordinates"],))
                    return coordinates

        return None

    def _are_closely_grouped_results(self, results):
        """
        Check to see if all results are within 10km of each other.
        """
        if (0 == len(results)):
            return False

        TOLERANCE_KM = 10
        gpoints = "geocodePoints"
        coords = "coordinates"
        coordinates = [(x[gpoints][0][coords][0], x[gpoints][0][coords][1]) for
                       x in results]
        for combination in itertools.combinations(coordinates, 2):
            if (TOLERANCE_KM < haversine(combination[0], combination[1])):
                return False

        return True

    def _fetch_results(self, q):
        params = {'q': q.encode("utf8"),
                  'key': self.api_key }

        # geopy changed the instance variables on the bing geocoder in
        # version 0.96 - this handles the differences
        if hasattr(self, 'url'):
            url = self.url % urlencode(params)
        else:
            url = "%s?%s" % (self.api, urlencode(params))

        try:
            page = urlopen_with_retries(url)
        except Exception, e:
            logger.error("Geocode error, could not open URL: %s, error: %s" %
                         (url, e))
            return []

        if (not isinstance(page, basestring)):
            page = util.decode_page(page)

        doc = json.loads(page)
        return doc["resourceSets"][0]["resources"]

    def _get_country_bbox(self, country):
        if (country not in DplaBingGeocoder.countryBBoxCache):
            results = list(self._fetch_results(country))
            DplaBingGeocoder.countryBBoxCache[country] = results

        if (1 == len(DplaBingGeocoder.countryBBoxCache[country])):
            bbox = DplaBingGeocoder.countryBBoxCache[country][0]["bbox"]
            return (bbox[0], bbox[1]), (bbox[2], bbox[3])

        return None

    def _is_in_country(self, coordinates, country):
        bbox = self._get_country_bbox(country)
        if (bbox):
            c1, c2 = bbox
            return ((c1[0] <= coordinates[0] and coordinates[0] <= c2[0]) and
                    (c1[1] <= coordinates[1] and coordinates[1] <= c2[1]))

        # We can't locate a bounding box for this country
        return None


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
        # TODO: considering filtering on specific feature codes
        if place.name == place.country:
            params['featureClass'] = 'A'
        elif place.name == place.region:
            params['featureClass'] = 'A'
        elif place.name == place.state:
            params['featureClass'] = 'A'
            params['countryBias'] = 'US'
        elif place.name == place.city:
            params['featureClass'] = 'P'
            params['countryBias'] = 'US'
        else:
            params['countryBias'] = 'US'

        geonames_json = self._name_search(place.name, params)

        if not 'featureClass' in params.keys():
            states_json = self._name_search(place.name, {'featureCode': 'ADM1'})
            countries_json = self._name_search(place.name,
                                               {'featureCode': 'PCLI'})
            geonames_json = states_json + countries_json + geonames_json

        if not geonames_json:
            return place

        candidates = []

        for geoname in geonames_json:
            candidate_place = Place({
                'name': geoname.get('name'),
                'uri': geoname.get('geonameId'),
                'country': geoname.get('countryName'),
                'feature_type': geoname.get('fcode'),
                'coordinates': geoname['lat'] + ', ' + geoname['lng']
            })
            parent_features = self.build_hierarchy(geoname['geonameId'],
                                                   ["PCLI", # Country
                                                    "ADM1", # State
                                                    "ADM2", # County
                                                    "PPLA"]) # City
            for feature in parent_features:
                if ("PCLI" == feature.get("fcode")):
                    candidate_place.country = feature["name"]
                    candidate_place.country_uri = feature["geonameId"]
                elif ("ADM1" == feature.get("fcode")):
                    candidate_place.state = feature["name"]
                    candidate_place.state_uri = feature["geonameId"]
                elif ("ADM2" == feature.get("fcode")):
                    candidate_place.county = feature["name"]
                    candidate_place.county_uri = feature["geonameId"]

            if (candidate_place.name.lower() not in place.name.lower()) and \
               (place.name.lower() not in candidate_place.name.lower()):
                continue
            if place.country and (place.country.lower() != \
                                  candidate_place.country.lower()):
                continue
            if place.state and (place.state.lower() != \
                                candidate_place.state.lower()):
                continue
            if place.county and (place.county.lower() != \
                                 candidate_place.county.lower()):
                continue

            if geoname['name'].lower() == place.name.lower():
                return candidate_place
            candidates.append(candidate_place)

        if len(candidates) == 1:
            return candidates[0]
        elif len(candidates) > 1:
            for candidate in candidates:
                if candidate.feature_type and \
                   ("PCL" in candidate.feature_type):
                    return candidate

            return candidates[0]

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
            result = DplaGeonamesGeocoder._get_result(url)
            if result.get('geonames'):
                DplaGeonamesGeocoder.resultCache[url] = result["geonames"]
            else:
                return hierarchy
        # Return only the requested fcodes
        for feature in DplaGeonamesGeocoder.resultCache.get(url):
            if (("fcode" in feature and feature["fcode"] in fcodes) or
                fcodes is None):
                hierarchy.append(feature)

        return hierarchy

    def _name_search(self, name, params={}):
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
        Validates coordinate values through get_coordinates.
        Sets and returns new coordinate values if needed.
        """
        coords = get_coordinates(self.coordinates)
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
        for place in places:
            if not place in places:
                next
            for compare_place in places:
                if compare_place == place:
                    continue
                if Place._is_parent_of(place, compare_place):
                    places.remove(place)
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
