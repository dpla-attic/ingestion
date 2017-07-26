import traceback
import internetarchive
from akara import logger
from dplaingestion.fetchers.fetcher import Fetcher


class IAFetcher(Fetcher):

    def fetch_all_data(self, sets=None):
        """A generator for all of the provider's item and collection records

        The interface for Fetcher.fetch_all_data() (Not as defined in Fetcher,
        but in practice, the way it's invoked by fetch_records.py) has a
        `set' argument. IAFetcher does not use this, and gets its collections
        from its provider profile (ia.pjs). IAFetcher will raise an Exception
        if `sets' is set.
        """
        if sets:
            raise Exception("IAFetcher does not take a `sets' argument, but "
                            "'%s' was provided." % sets)
        self.create_collection_records()
        self.reset_response()
        for token in self.collections.iterkeys():
            self.response['records'].append(self.collections[token])
            i = 1
            for item in internetarchive \
                        .search_items("collection:%s" % token,
                                request_kwargs=dict(timeout=60)) \
                        .iter_as_items():
                try:
                    md = item.item_metadata
                    md['_id'] = "ia--%s" % md['metadata']['identifier']
                    md['collection'] = self.source_resource_collection(token)
                    self.response['records'].append(md)
                    i += 1
                    if i >= self.batch_size:
                        # ^^^ Should probably use `==' but being paranoid about
                        # possible but unlikely batch_size of 1.
                        yield self.response
                        self.reset_response()
                        i = 1
                except Exception as e:
                    tb = traceback.format_exc(5)
                    msg = "In IA collection %s: %s" % (token, e)
                    self.response['errors'].append(msg)
                    logger.error("%s\n%s" % (msg, tb))
            if len(self.response['records']):
                # last yield of the collection
                yield self.response
                self.reset_response()

    def source_resource_collection(self, token):
        """Return one collection dict suitable for sourceResource.collection

        Args:
            token: The collection token / identifier that's a key of the
                profile's 'collections' object

        Returns dict, which will be empty if there is no such collection.

        FIXME: Refactor to move this function into Fetcher and clean up code in
        other modules that have their own independent implementations of the
        same logic.
            - AbsoluteURLFetcher.fetch_all_data()
              and AbsoluteURLFetcher.add_collection_to_item_records()
            - OAIVerbsFetcher.fetch_all_data()
              and OAIVerbsFetcher.add_collection_to_item_records()
            - MDLAPIFetcher.add_collection_to_item_records()
            - NARAFetcher.add_collection_to_item_records()
            - PrimoFetcher.add_collection_to_item_records()
            - EDANFetcher.add_collection_to_item_record()
        """
        exclusions = ('_id', 'ingestType')
        return {k:v for k, v in self.collections.get(token, {}).items()
                if k not in exclusions}
