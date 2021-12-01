import argparse
import json
import logging
from typing import Optional, Dict, List, AnyStr, Generator, Any

from pandas import DataFrame, read_excel

"""
Skeleton for Squirro Delivery Hiring Coding Challenge
August 2021
"""

logger = logging.getLogger(__name__)


class NoArgumentsException(Exception):
    pass


class Excel:
    """
    An auxiliary object for handling xlsx files, using Pandas DataFrame.
    """

    def __init__(self, path: AnyStr, sheet_name: AnyStr, index_col: Optional[int], header: int, column_type: Dict):
        self._path = path
        self._sheet_name = sheet_name
        self._index_col = index_col
        self._header = header
        self._column_type = column_type
        self._excel_df: Optional[DataFrame] = None

    def _load_sheet(self):
        if self._excel_df is not None:
            return
        converters = {column: (lambda x: x.strip() if isinstance(x, str) else x) for column in self.columns}
        self._excel_df = read_excel(
            self._path,
            sheet_name=self._sheet_name,
            index_col=self._index_col,
            header=self._header,
            converters=converters,
        )
        self._excel_df.reset_index(drop=True, inplace=True)

    @property
    def data_frame(self) -> DataFrame:
        self._load_sheet()

        return self._excel_df

    @property
    def columns(self) -> List[AnyStr]:
        return list(self._column_type.keys())


class NYTimesSource(object):
    """
    A data loader plugin for the NY Times Data.
    """

    def __init__(self):
        self.args: Optional[argparse.Namespace] = None
        self._response_docs = ["response", "docs"]
        self._docs: Optional[Generator] = None
        self._schema: Optional[List[AnyStr]] = None

    def connect(self, inc_column=None, max_inc_value=None):
        # Ignore this method
        logger.debug("Incremental Column: %r", inc_column)
        logger.debug("Incremental Last Value: %r", max_inc_value)

    def disconnect(self):
        """Disconnect from the source."""
        # Nothing to do
        pass

    def getDataBatch(self, batch_size: int) -> Dict:
        """
        Generator - Get data from source on batches.

        :returns One list for each batch. Each of those is a list of
                 dictionaries with the defined rows.
        """
        self._check_args()
        self._load_data()
        while True:
            results = []
            try:
                for _ in range(batch_size):
                    doc = next(self._docs)
                    results.append(doc)
            except StopIteration:
                if results:
                    yield results
                break
            yield results

    def getSchema(self) -> List[AnyStr]:
        """
        Return the schema of the dataset
        :returns a List containing the names of the columns retrieved from the
        source
        """
        self._check_args()
        self._load_schema()

        return self._schema

    def _check_args(self):
        if self.args is None:
            raise NoArgumentsException(f"Object: '{self.__class__.__name__}' configuration arguments are not set.")

    def _load_schema(self):
        if self._schema is not None:
            return
        self._load_data()

    def _load_data(self):
        if self._docs is not None:
            return
        with open(self.args.api_response_file, encoding='utf-8') as json_file:
            data = json.load(json_file)
        docs = self._get_nested_data(data, self._response_docs)
        review_status = Excel(
            path=self.args.reference_data_file,
            sheet_name="review_status",
            index_col=0,
            header=2,
            column_type={"Row": str, "Article Id": str, "Reference Id": str, "Status": str},
        )
        date_completed = Excel(
            path=self.args.reference_data_file,
            sheet_name="date_completed",
            index_col=None,
            header=0,
            column_type={"Reference Id": str, "Date Completed": str, "Reviewer": str},
        )
        excel_data = review_status.data_frame.merge(date_completed.data_frame, on="Reference Id", how="outer")
        self._schema = list(self._match_by_review_status(self._flatten_dict(docs[0]), excel_data).keys())
        self._docs = self._docs_generator(docs, excel_data)

    def _get_nested_data(self, data: Dict, path: List[AnyStr]) -> Any:
        _tmp = data
        for key in path:
            _tmp = _tmp[key]
        return _tmp

    def _flatten_dict(self, doc: Dict) -> Dict:
        flatten_doc = {}
        keys = doc.keys()
        go_deep = True
        while go_deep:
            go_deep = False
            nested_keys = []
            for key in keys:
                item = self._get_nested_data(doc, key.split("."))
                if not isinstance(item, dict):
                    flatten_doc[key] = item
                    continue
                nested_keys.extend([f"{key}.{ele}" for ele in item.keys()])
                go_deep = True
            keys = nested_keys

        return flatten_doc

    def _match_by_review_status(self, doc: Dict, excel_data: DataFrame) -> Dict:
        article_id = doc.get("_id")
        filtered_data = excel_data[excel_data["Article Id"] == article_id]
        series = excel_data.iloc[filtered_data["Row"].idxmax()]

        doc.update({key.lower().replace(" ", "_"): value for key, value in series.to_dict().items()})

        return doc

    def _docs_generator(self, docs: List[Dict], excel_data: DataFrame) -> Dict:
        for doc in docs:
            doc = self._flatten_dict(doc)
            doc = self._match_by_review_status(doc, excel_data)
            yield doc


if __name__ == "__main__":
    config = {
        "api_response_file": "api_response.json",
        "reference_data_file": "reference_data.xlsx"
    }
    source = NYTimesSource()

    source.args = argparse.Namespace(**config)

    print(source.getSchema())
    for idx, batch in enumerate(source.getDataBatch(2)):
        print(f"{idx} Batch of {len(batch)} items")
        for item in batch:
            print(f"{item['_id']} - {item['headline.main']}")
            print(f" --> {item['status']} - {item.get('date_completed')}")
