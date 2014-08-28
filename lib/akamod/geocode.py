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
from urllib2 import urlopen
from dplaingestion.utilities import iterify


@simple_service('POST', 'http://purl.org/la/dp/geocode', 'geocode',
                'application/json')
def geocode(body, ctype, prop="sourceResource/spatial", newprop='coordinates'):
    '''
    Adds geocode data to the record coming (in only if a "state" value is not
    already included) as follows:

    1. If a coordinate property does not exist, or if the name property is not
       a coordinate, attempt to get a lat/lng coordinate from the name property
    2. If the coordinate property does exist, or if the name property is a
       coordinate, use this lat/lng coordinate
    3. For the given lat/lng coordinate, attempt to determine its parent
       features (county, state, country). We use Geonames to reverse geocode
       the lat/lng point and retrieve the location hierarchy
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
        for v in iterify(value):
            if not isinstance(v, dict):
                logger.error("Spatial value must be a dictionary; record %s" %
                             data["_id"])
                continue

            set_name(v)
            if "coordinates" not in v and "name" not in v:
                logger.error("Spatial dictionary must have 'coordinates' " +
                             "or 'name' property; record %s" % data["_id"])

            if v.get("state") is not None:
                # Do not geocode if dictionary has a "state" value
                continue

            # Likewise, skip generic "United States"
            pattern = ur" *(United States(?!-)|États-Unis)"
            if re.search(pattern, v.get("name", "")) or \
               re.search(pattern, v.get("country", "")):
                    continue

            if "coordinates" in v:
                if get_coordinates(v["coordinates"]):
                    result = get_coordinates(v["coordinates"])
                else:
                    logger.error("Value %s " % v["coordinates"] +
                                 "for property \"coordinates\" is not a " +
                                 "valid coordinate value; record %s" %
                                 data["_id"])
                    continue
            elif get_coordinates(v["name"]):
                result = get_coordinates(v["name"])
            else:
                # Attempt to find this item's lat/lng coordinates
                api_key = module_config().get("bing_api_key")
                result = DplaBingGeocoder(api_key=api_key).geocode_spatial(v)

            if (result):
                lat, lng = result
                if newprop not in v:
                    v[newprop] = "%s, %s" % (lat, lng,)

                # Reverse-geocode this location to find country, state, and
                # county parent places
                hierarchy = DplaGeonamesGeocoder().reverse_geocode_hierarchy(
                                                            lat,
                                                            lng,
                                                            ["PCLI", # Country
                                                             "ADM1", # State
                                                             "ADM2"] # County
                                                             )
                spatial = {}
                for place in hierarchy:
                    fcode = place["fcode"]
                    if ("PCLI" == place["fcode"]):
                        spatial["country"] = place["toponymName"]
                    elif ("ADM1" == place["fcode"]):
                        spatial["state"] = place["toponymName"]
                    elif ("ADM2" == place["fcode"]):
                        spatial["county"] = place["toponymName"]

                    # Deterine how close we are to the original coordinates, to
                    # see if this is the place that was geocoded and we should
                    # stop adding more specificity (e.g. if the record
                    # specified "South Carolina", we don't want to include the
                    # county that is located at the coordinates of South
                    # Carolina. We use increasing tolerance levels to account
                    # for differences between Bing and Geonames coordinates.
                    d = haversine((lat, lng), (place["lat"], place["lng"]))

                    # Country tolerance (Bing/Geonames 49.9km off)
                    country_tolerance_met = ("PCLI" == place["fcode"] and
                                             d < 50)
                    # State tolerance
                    state_tolerance_met = ("ADM1" == place["fcode"] and d < 15)

                    if (country_tolerance_met or state_tolerance_met):
                        break

                # Add only non-existing properties to v
                for k in spatial.keys():
                    if k not in v:
                        v[k] = spatial[k]
            else:
                logger.debug("No geocode result found for %s" % v)

        setprop(data, prop, value)

    return json.dumps(data)

def set_name(spatial_dict):
    if "name" not in spatial_dict:
        key_order = ["coordinates", "city", "county", "state", "country",
                     "region"]
        for key in key_order:
            if key in spatial_dict:
                spatial_dict["name"] = spatial_dict[key]
                return

def get_coordinates(value):
    try:
        coordinates = map(float, value.split(","))
    except:
        return

    if len(coordinates) == 2:
        return (coordinates[0], coordinates[1])
    else:
        return

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

class Address:
    def __init__(self, spatial):
        self.name = spatial["name"] if ("name" in spatial) else ""
        self.city = spatial["city"] if ("city" in spatial) else ""
        self.county = spatial["county"] if ("county" in spatial) else ""
        self.region = spatial["region"] if ("region" in spatial) else ""
        self.state = spatial["state"] if ("state" in spatial) else ""
        self.country = spatial["country"] if ("country" in spatial) else ""

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

    def geocode_spatial(self, spatial):
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
            page = urlopen(url)
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

    def reverse_geocode(self, lat, lng):
        params = { "lat": lat,
                   "lng": lng,
                   "username": module_config().get("geonames_username"),
                   "token": module_config().get("geonames_token") }
        url = "http://ws.geonames.net/findNearbyJSON?%s" % urlencode(params)
        if (url not in DplaGeonamesGeocoder.resultCache):
            result = json.loads(util.decode_page(urlopen(url)))
            if ("geonames" in result \
                and len(result["geonames"]) > 0):
                DplaGeonamesGeocoder.resultCache[url] = result["geonames"][0]
            else: 
                logger.error("Could not reverse geocode (%s, %s)" %
                             (lat, lng,)) 
                return None

        return DplaGeonamesGeocoder.resultCache[url]


    def reverse_geocode_hierarchy(self, lat, lng, fcodes=None):
        hierarchy = []

        geonames_item = self.reverse_geocode(lat, lng)
        if (geonames_item):
            params = { "geonameId": geonames_item["geonameId"],
                       "username": module_config().get("geonames_username"),
                       "token": module_config().get("geonames_token") }
            url = "http://ws.geonames.net/hierarchyJSON?%s" % urlencode(params)
            if (url not in DplaGeonamesGeocoder.resultCache):
                result = json.loads(util.decode_page(urlopen(url)))
                DplaGeonamesGeocoder.resultCache[url] = result["geonames"]
                
            # Return only the requested fcodes
            for place in DplaGeonamesGeocoder.resultCache[url]:
                if (("fcode" in place and place["fcode"] in fcodes) or
                    fcodes is None):
                    hierarchy.append(place)
                    
        return hierarchy 
