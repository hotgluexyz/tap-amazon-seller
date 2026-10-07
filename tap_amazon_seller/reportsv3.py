from sp_api.api import (
    ReportsV2,
)
import zlib
from typing import Optional
import requests

from sp_api.base import sp_endpoint, fill_query_params, ApiResponse
from sp_api.util.report_document import handle_file


class ReportsV3(ReportsV2):
    @sp_endpoint("/reports/2021-06-30/documents/{}", method="GET")
    def get_report_document(
        self,
        reportDocumentId,
        download: bool = False,
        file=None,
        character_code: Optional[str] = None,
        **kwargs
    ) -> ApiResponse:
        """
        get_report_document(self, document_id, decrypt: bool = False, file=None, character_code: Optional[str] = None, **kwargs) -> ApiResponse
        Returns the information required for retrieving a report document's contents. This includes a presigned URL for the report document as well as the information required to decrypt the document's contents.

        If decrypt = True the report will automatically be loaded and decrypted/unpacked
        If file is set to a file (or file like object), the report's contents are written to the file


        **Usage Plan:**


        ======================================  ==============
        Rate (requests per second)               Burst
        ======================================  ==============
        0.0167                                  15
        ======================================  ==============



        For more information, see "Usage Plans and Rate Limits" in the Selling Partner API documentation.

        Examples:
            literal blocks::

                Reports().get_report_document('0356cf79-b8b0-4226-b4b9-0ee058ea5760', download=True, file=file)

        Args:
            reportDocumentId: str | the document to load
            download: bool | flag to automatically download a report
            file: If passed, will save the document to the file specified.
                  Only valid if decrypt=True
            character_code: If passed, will be a file with the specified character code.
                            The default is the Content-Encoding in the response while
                            obtaining the document from the document URL.
                            It fallbacks to 'iso-8859-1' if no encoding was found.
                            Only valid if decrypt=True.

        Returns:
             ApiResponse
        """  # noqa: E501
        res = self._request(
            fill_query_params(kwargs.pop("path"), reportDocumentId),
            add_marketplace=False,
        )
        if download or file or ("decrypt" in kwargs and kwargs["decrypt"]):
            document_response = requests.get(
                res.payload.get("url"),
                proxies=self.proxies,
                verify=self.verify,
            )
            document = document_response.content
            if not character_code:
                character_code = (
                    document_response.encoding
                    if document_response and document_response.encoding
                    else "iso-8859-1"
                )
                if character_code.lower() == "windows-31j":
                    character_code = "cp932"
            # Turkish region reports are marked as GZIP'd but actually uncompressed this block fails for no reason then
            if "compressionAlgorithm" in res.payload:
                try:
                    document = zlib.decompress(bytearray(document), 15 + 32)
                except:
                    pass
            decoded_document = document.decode(character_code)
            if download:
                res.payload.update(
                    {
                        "document": decoded_document,
                    }
                )
            if file:
                handle_file(file, decoded_document, character_code)
        return res
