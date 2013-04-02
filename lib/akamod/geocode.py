from akara import module_config, logger, response
from akara.services import simple_service
from amara.thirdparty import json
from dplaingestion.selector import getprop, setprop, exists
from geopy import geocoders, util
import math
from urllib import urlencode
from urllib2 import urlopen


@simple_service('POST', 'http://purl.org/la/dp/geocode', 'geocode', 'application/json')
def geocode(body, ctype, prop="sourceResource/spatial", newprop='coordinates'):
    '''
    Adds geocode data to the record coming in as follows: 
        1. Attempt to get a lat/lng coordinate from the property. We use Bing to 
           lookup lat/lng from a string as it is much better than Geonames. 
        2. For the given lat/lng coordinate, attempt to determine its parent 
           features (county, state, country). We use Geonames to reverse geocode 
           the lat/lng point and retrieve the location hierarchy. 
    '''
    # logger.debug(body)

    try:
        data = json.loads(body)
    except:
        response.code = 500
        response.add_header('content-type','text/plain')
        return "Unable to parse body as JSON"

    if (not exists(data, prop)): 
        # logger.warn("geocode: COULD NOT FIND %s" % prop)
        pass 
    else:
        value = getprop(data, prop)
        for v in iterify(value):
            # Attempt to find this item's lat/lng coordinates
            result = DplaBingGeocoder(api_key=module_config().get("bing_api_key")).geocode_spatial(v)
            if (result): 
                lat, lng = result
                v["coordinates"] = "%s, %s" % (lat, lng,)

                # Reverse-geocode this location to find country, state, and county parent places 
                hierarchy = DplaGeonamesGeocoder().reverse_geocode_hierarchy(lat, lng, ["PCLI",   # Country
                                                                                        "ADM1",   # State
                                                                                        "ADM2"])  # County
                for place in hierarchy: 
                    fcode = place["fcode"]
                    if ("PCLI" == place["fcode"]): 
                        str = place["toponymName"]
                        v["country"] = place["toponymName"]
                    elif ("ADM1" == place["fcode"]):
                        str += ", %s" % place["toponymName"]
                        v["state"] = place["toponymName"]
                    elif ("ADM2" == place["fcode"]):
                        str += ", %s" % place["toponymName"]
                        v["county"] = place["toponymName"]

                    # Deterine how close we are to the original coordinates, to see if this is the 
                    #  place that was geocoded and we should stop adding more specificity (e.g. if 
                    #  the record specified "South Carolina", we don't want to include the county 
                    #  that is located at the coordinates of South Carolina. We use increasing 
                    #  tolerance levels to account for differences between Bing and Geonames 
                    #  coordinates.
                    d = haversine((lat, lng), (place["lat"], place["lng"]))
                    if (("PCLI" == place["fcode"] and d < 50)       # Country tolerance (Bing/Geonames 49.9km off) \
                        or ("ADM1" == place["fcode"] and d < 15)):  # State tolerance 
                        break
            else:
                logger.debug("geocode: No result found for %s" % v)

    return json.dumps(data)



def haversine(origin, destination):
    ''' 
    Distance in km between two lat/lng coordinates.
    '''
    lat1, lon1 = origin
    lat2, lon2 = destination
    radius = 6371 # Radius of the earth in km

    dlat = math.radians(lat2-lat1)
    dlon = math.radians(lon2-lon1)
    a = math.sin(dlat/2) * math.sin(dlat/2) + math.cos(math.radians(lat1)) \
        * math.cos(math.radians(lat2)) * math.sin(dlon/2) * math.sin(dlon/2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    d = radius * c

    return d



def iterify(iterable): 
    ''' 
    Treat iterating over a single item or an interator seamlessly.
    '''
    if (isinstance(iterable, basestring)):
        iterable = [iterable]
    try:
        iter(iterable)
    except TypeError:
        iterable = [iterable]
    return iterable



class DplaBingGeocoder(geocoders.Bing): 
    resultCache = {}

    def __init__(self, **kwargs): 
        super(DplaBingGeocoder, self).__init__(**kwargs)

    def geocode_spatial(self, spatial): 
        if (not self.api_key): 
            logger.warn("No API key set for Bing (use bing_api_key configuration key")
            return None

        for address in self._get_address_candidates(spatial):
            # See if address exists in our cache 
            if (address not in DplaBingGeocoder.resultCache): 
                logger.debug("geocode: No result for [%s] in cache, retrieving from Bing" % address)
                params = {'q': address,
                          'key': self.api_key }
                url = self.url % urlencode(params)
                result = self.geocode_url(url=url, exactly_one=False)
                DplaBingGeocoder.resultCache[address] = list(result)
                logger.info("geocode: Result: %s => %s" % (spatial, DplaBingGeocoder.resultCache[address],))

            for place, (lat, lng) in DplaBingGeocoder.resultCache[address]:
                if (lat is not None 
                    and lng is not None):
                    return (lat, lng)

        return None

    def _get_address_candidates(self, spatial): 
        tokens = []
        if ("name" in spatial):
            # TODO: Add state if it does not exist already
            yield self._clean(spatial["name"])

        if ("city" in spatial):
            yield self._clean(self._get_address_candidate(spatial["city"], spatial))
        if ("county" in spatial): 
            yield self._clean(self._get_address_candidate(spatial["county"], spatial))
        if ("region" in spatial):
            yield self._clean(self._get_address_candidate(spatial["region"], spatial))
        if ("state" in spatial): 
            yield self._clean(self._get_address_candidate("", spatial))


    def _get_address_candidate(self, value, spatial_dict): 
        '''
        Return a dictionary with a string to search Geonames with, and a string to 
        match the results with. 
        '''
        searchTokens = []

        if (value): 
            searchTokens.append(value)

        if ("state" in spatial_dict):
            searchTokens.append(spatial_dict.get("state"))
        elif ("iso3166-2" in spatial_dict):
            searchTokens.append(spatial_dict.get("iso3166-2")[3:])

        if ("country" in spatial_dict):
            country = spatial_dict.get("country")

            # Convert country to 2-character codes
            # TODO: Move to enrich-location 
            if ("united states" == country.lower()): 
                country = "US"

            searchTokens.append(country)

        elif ("iso3166-2" in spatial_dict 
              or "state" in spatial_dict): 
            searchTokens.append("US")

        return ", ".join(searchTokens)


    def _clean(self, value): 
        '''
        Remove characters that confuse Bing 
        '''
        return value.translate(dict.fromkeys(map(ord, ".()")))




class DplaGeonamesGeocoder(object): 
    resultCache = {}

    def reverse_geocode(self, lat, lng): 
        params = { "lat": lat, 
                   "lng": lng,
                   "username": module_config().get("geonames_username") }
        url = "http://api.geonames.org/findNearbyJSON?%s" % urlencode(params)
        if (url not in DplaGeonamesGeocoder.resultCache):
            result = json.loads(util.decode_page(urlopen(url)))
            DplaGeonamesGeocoder.resultCache[url] = result["geonames"][0]

        return DplaGeonamesGeocoder.resultCache[url]


    def reverse_geocode_hierarchy(self, lat, lng, fcodes=None): 
        # First, reverse geocode these coordinates 
        geonames_item = self.reverse_geocode(lat, lng)

        params = { "geonameId": geonames_item["geonameId"],
                   "username": module_config().get("geonames_username") }
        url = "http://api.geonames.org/hierarchyJSON?%s" % urlencode(params)
        if (url not in DplaGeonamesGeocoder.resultCache):
            result = json.loads(util.decode_page(urlopen(url)))
            DplaGeonamesGeocoder.resultCache[url] = result["geonames"]

        # Return only the requested fcodes 
        hierarchy = []
        for place in DplaGeonamesGeocoder.resultCache[url]: 
            if (place["fcode"] in fcodes \
                or fcodes is None):
                hierarchy.append(place)

        return hierarchy 
