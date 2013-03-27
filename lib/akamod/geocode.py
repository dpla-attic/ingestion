from urllib import urlencode
from akara import logger
from akara import response
from akara.services import simple_service
from amara.thirdparty import json
from dplaingestion.selector import getprop, setprop, exists
from geopy import geocoders


@simple_service('POST', 'http://purl.org/la/dp/geocode', 'geocode', 'application/json')
def geocode(body, ctype, prop="sourceResource/spatial", newprop='coordinates'):
    # logger.debug(body)

    try:
        data = json.loads(body)
    except:
        response.code = 500
        response.add_header('content-type','text/plain')
        return "Unable to parse body as JSON"

    if (not exists(data, prop)): 
        logger.warn("geocode: COULD NOT FIND %s" % prop)
    else:
        value = getprop(data, prop)
        for v in iterify(value):
            result = DplaBingGeocoder().geocode(v)
            if (result): 
                lat, lng = result
                v["coordinates"] = "%s, %s" % (lat, lng,)
                break; 
            else:
                logger.debug("geocode: No result found for %s" % v)

    return json.dumps(data)



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



class DplaBingGeocoder(object): 
    gn = geocoders.Bing(api_key="AkXkEKeY7toR3-guQvN4Wq9BEHjm0AtTCw60ir5P8MSCwipe_IRjpP3BLy3Uttph")
    resultCache = {}

    def geocode(self, spatial): 
        for address in self._get_address_candidates(spatial):
            # See if address exists in our cache 
            if (address not in DplaBingGeocoder.resultCache): 
                logger.debug("geocode: No result for [%s] in cache, retrieving from Bing" % address)
                params = {'q': address,
                          'o': DplaBingGeocoder.gn.output_format,
                          'key': DplaBingGeocoder.gn.api_key
                          }
                url = DplaBingGeocoder.gn.url % urlencode(params)
                result = DplaBingGeocoder.gn.geocode_url(url=url, exactly_one=False)
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
        """
        Remove characters that confuse Bing 
        """
        return value.translate(dict.fromkeys(map(ord, ".()")))




class DplaGeonamesGeocoder(object): 
    gn = geocoders.GeoNames(user="dpla")
    resultCache = {}

    def geocode(self, spatial): 
        for address in self._get_address_candidates(spatial):
            logger.debug("geocode: Testing candidate %s" % address)
            # See if address exists in our cache 
            if (address["search"] not in DplaGeonamesGeocoder.resultCache): 
                # Does not exist, lookup from Geonames
                logger.debug("geocode: No result for %s in cache, retrieving from Geonames" % address["search"])
                result = DplaGeonamesGeocoder.gn.geocode(string=address["search"], exactly_one=False)
                DplaGeonamesGeocoder.resultCache[address["search"]] = list(result)

            for place, (lat, lng) in DplaGeonamesGeocoder.resultCache[address["search"]]:
                # Loop through each results and check for an exact match 
                if (place == address["match"]):
                    logger.debug("geocode: Geocode result: %s => (%s, %s)" % (address["search"], lat, lng,))
                    return (lat, lng)
                else: 
                    logger.debug("geocode: %s != %s" % (address["match"], place,))
                    None

            logger.debug("geocode: No result found for %s" % address)

        return None


    def _get_address_candidates(self, spatial): 
        ''' 
        Generate an array of addresses to lookup with the following specificities
        1. City, State, Country
        2. County, State, Country
        3. Region, State, Country
        4. State, Country 
        ''' 
        logger.debug("geocode: Received spatial: %s" % spatial)
        if (isinstance(spatial, basestring)):
            yield spatial
        elif (isinstance(spatial, dict)):
            if ("city" in spatial):
                yield self._get_address_candidate(spatial.get("city"), spatial)
            if ("county" in spatial):
                yield self._get_address_candidate(spatial.get("county") + " County", spatial)
            if ("region" in spatial):
                yield self._get_address_candidate(spatial.get("region"), spatial) 
            if ("state" in spatial):
                yield self._get_address_candidate('', spatial)

                        
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

        # Geonames returns the place as Feature Name, Country Code, so when we're matching on results, 
        #  use the first and last search tokens 
        matchTokens = [searchTokens[0], searchTokens[-1]] if (len(searchTokens) > 2) else searchTokens

        return { 
            "search": ", ".join(searchTokens),
            "match": ", ".join(matchTokens)
            }


