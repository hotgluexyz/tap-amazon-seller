from sp_api.api import (
    CatalogItems
)
from sp_api.base import sp_endpoint, ApiResponse, fill_query_params

DEFAULT_CATALOG_ITEMS_VERSION = "2022-04-01"


class CatalogItems_v2(CatalogItems):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.version = DEFAULT_CATALOG_ITEMS_VERSION

    @sp_endpoint('/catalog/<version>/items/{}', method='GET')
    def get_catalog_item(self, asin=None, **kwargs) -> ApiResponse:
        """
        get_catalog_item(self, asin, **kwargs) -> ApiResponse

        Retrieves details for an item in the Amazon catalog.


        ======================================  ==============
        Rate (requests per second)               Burst
        ======================================  ==============
        5                                       5
        ======================================  ==============

        The x-amzn-RateLimit-Limit response header returns the usage plan rate limits that were applied to the requested operation. Rate limits for some selling partners will vary from the default rate and burst shown in the table above. For more information, see "Usage Plans and Rate Limits" in the Selling Partner API documentation.

        Args:
            asin:string | * REQUIRED The Amazon Standard Identification Number (ASIN) of the item.
            key marketplaceIds:array | * REQUIRED A comma-delimited list of Amazon marketplace identifiers. Data sets in the response contain data only for the specified marketplaces.
            key includedData:array |  A comma-delimited string or list of data sets to include in the response. Default: summaries.
            key locale:string |  Locale for retrieving localized summaries. Defaults to the primary locale of the marketplace.

        Returns:
            ApiResponse:
        """

        includedData = kwargs.get('includedData', [])
        if includedData and isinstance(includedData, list):
            kwargs['includedData'] = ','.join(includedData)
        if asin:
            return self._request(fill_query_params(kwargs.pop('path'), asin), params=kwargs)
        else:
            # remove default path
            kwargs.pop("path")
            # change the version, earlier versions don't support attributes as includedData using the search endpoint
            version = kwargs.pop("version")
            return self._request(path=f"/catalog/{version}/items", params=kwargs)