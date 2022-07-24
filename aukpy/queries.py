from aukpy import db
import pandas as pd
import sqlite3
from dataclasses import dataclass, replace
from typing import List, Optional, Sequence, Union, Tuple, Any


def check_simple_type(value):
    return isinstance(value, str) or isinstance(value, float) or isinstance(value, int) or isinstance(value, bool)


class Filter:

    def __and__(self, other):
        return AndFilter(self, other)

    def __or__(self, other):
        return OrFilter(self, other)

    def query(self) -> Tuple[str, Tuple[Any, ...]]:
        raise NotImplementedError


@dataclass
class AndFilter(Filter):
    filter_1: Filter
    filter_2: Filter

    def query(self) -> Tuple[str, Tuple[Any, ...]]:
        f1, v1 = self.filter_1.query()
        f2, v2 = self.filter_2.query()
        vals = v1 + v2
        return f'{f1} AND {f2}', vals


@dataclass
class OrFilter(Filter):
    filter_1: Filter
    filter_2: Filter

    def query(self) -> Tuple[str, Tuple[Any, ...]]:
        f1, v1 = self.filter_1.query()
        f2, v2 = self.filter_2.query()
        vals = v1 + v2
        return f'{f1} OR {f2}', vals


@dataclass
class ColumnFilter(Filter):
    column: str

    def query(self) -> Tuple[str, Tuple[Any, ...]]:
        raise NotImplementedError


@dataclass
class IsIn(ColumnFilter):
    values: Tuple[Any, ...]

    def query(self) -> Tuple[str, Tuple[Any, ...]]:
        return f'{self.column} in ?', (self.values, )


@dataclass
class Is(ColumnFilter):
    value: Any

    def query(self) -> Tuple[str, Tuple[Any, ...]]:
        return f'{self.column} = ?', (self.value, )


@dataclass
class EqualsOrIn(ColumnFilter):
    value: Any

    def query(self) -> Tuple[str, Tuple[Any, ...]]:
        if isinstance(self.value, tuple):
            return f'{self.column} is in ?', (self.value, )
        else:
            # The only types that are contained in the database are these simple types
            assert check_simple_type(self.value)
            return f'{self.column} = ?', (self.value, )


@dataclass
class LT(ColumnFilter):
    value: Union[float, int]

    def query(self) -> Tuple[str, Tuple[Any, ...]]:
        return f'{self.column} < ?', (self.value, )


@dataclass
class GT(ColumnFilter):
    value: Union[float, int]

    def query(self) -> Tuple[str, Tuple[Any, ...]]:
        return f'{self.column} > ?', (self.value, )


@dataclass
class IsTrue(ColumnFilter):

    def query(self) -> Tuple[str, Tuple[Any, ...]]:
        return f'{self.column} = 1', ()


@dataclass
class IsFalse(ColumnFilter):

    def query(self) -> Tuple[str, Tuple[Any, ...]]:
        return f'{self.column} = 0', ()


@dataclass
class Query:
    """A wrapper around a set of filters for each table"""

    row_filter: Optional[Filter]=None

    def _update_filter(self, new_filt: Filter):
        if self.row_filter is None:
            return replace(self, row_filter=new_filt)
        else:
            return replace(self, row_filter=self.row_filter & new_filt)

    def species(self, names: Union[str, Sequence[str]]) -> 'Query':
        """Filter by species name
        Can be any of scientific name, common name, subspecies scientific name, or subspecies common name.
        """
        scientific_filt = EqualsOrIn('species.scientific_name', names)
        common_filt = EqualsOrIn('species.common_name', names)
        sub_science = EqualsOrIn('species.subspecies_scientific_name', names)
        sub_common = EqualsOrIn('species.subspecies_common_name', names)
        new_filt = scientific_filt | common_filt | sub_science | sub_common
        return self._update_filter(new_filt)

    def country(self, names: Union[str, Sequence[str]]) -> 'Query':
        """Filter by country name or country code."""
        new_filt = EqualsOrIn('location_data.country_name', names) | EqualsOrIn('location_data.country_code', names)
        return self._update_filter(new_filt)

    def state(self, names: Union[str, Sequence[str]]) -> 'Query':
        """Filter by state name or state code."""
        new_filt = EqualsOrIn('location_data.state_name', names) | EqualsOrIn('location_data.state_code', names)
        return self._update_filter(new_filt)

    def bcr(self, code: Union[str, Sequence[str]]) -> 'Query':
        """Filter by BCR code"""
        return self._update_filter(EqualsOrIn('bcrcode.bcr_code', code))

    def bbox(self, min_long: float=-180.0, min_lat: float=-90.0, max_long: float=180.0, max_lat: float=90.0) -> 'Query':
        """Filter for observations within a bounding box."""
        new_filt = GT('longitude', min_long) & LT('longitude', max_long) & GT('latitude', min_lat) & LT('latitude', max_lat)
        return self._update_filter(new_filt)

    def date(self, date: Union[str, Sequence[str]]) -> 'Query':
        """Filter for a specific set of dates"""
        return self._update_filter(EqualsOrIn('observation_date', date))

    def before(self, date: Union[str, Sequence[str]]) -> 'Query':
        """Filter for observations before the given date"""
        raise NotImplementedError

    def after(self, date: Union[str, Sequence[str]]) -> 'Query':
        """Filter for observations after the given date"""
        raise NotImplementedError

    def last_edited(self, ) -> 'Query':
        raise NotImplementedError

    def protocol(self, value: Union[str, Sequence[str]]) -> 'Query':
        return self._update_filter(EqualsOrIn('protocol.protocol_ code', value))

    def project(self, value: Union[str, Sequence[str]]) -> 'Query':
        return self._update_filter(EqualsOrIn('protocol.project_code', value))

    def time(self, after: str, before: str) -> 'Query':
        raise NotImplementedError

    def duration(self, minimum: float=0, maximum: Optional[float] = None) -> 'Query':
        new_filt = GT('duration', minimum)
        if maximum is not None:
            new_filt = new_filt & LT('duration', maximum)
        return self._update_filter(new_filt)

    def distance(self, ) -> 'Query':
        raise NotImplementedError

    def breeding(self, ) -> 'Query':
        raise NotImplementedError

    def complete(self) -> 'Query':
        return self._update_filter(IsTrue('complete'))

    def get_query(self) -> Tuple[str, Tuple[Any, ...]]:
        if self.row_filter is not None:
            q_filter, vals = self.row_filter.query()
        else:
            q_filter = ''
            vals = ()
        query = f"""SELECT {db.DF_COLUMNS} FROM
        observation
        INNER JOIN species          ON species_id = species.id
        INNER JOIN location_data    ON location_data_id = location_data.id
        INNER JOIN bcrcode          ON bcrcode_id = bcrcode.id
        INNER JOIN ibacode          ON ibacode_id = ibacode.id
        INNER JOIN breeding         ON breeding_id = breeding.id
        INNER JOIN protocol         ON protocol_id = protocol.id
        WHERE {q_filter}"""
        return query, vals

    def run(self, db_conn: sqlite3.Connection) -> List[Tuple[Any, ...]]:
        """Execute the query, returning the raw data"""
        query, vals = self.get_query()
        cursor = db_conn.execute(query, vals)
        return cursor.fetchall()

    def run_pandas(self, db_conn: sqlite3.Connection) -> pd.DataFrame:
        """Execute the query, returning the results as a dataframe"""
        query, vals = self.get_query()
        return pd.read_sql_query(query, db_conn, params=vals)


def implicit_query(f):
    name = f.__name__
    def wrapper(*args, **kwargs):
        new_query = Query(None)
        method = getattr(new_query, name)
        return method(*args, **kwargs)
    return wrapper


@implicit_query
def species(names: Union[str, Sequence[str]]):
    pass


@implicit_query
def country(names: Union[str, Sequence[str]]) -> Query:
    pass


@implicit_query
def state(names: Union[str, Sequence[str]]) -> Query:
    pass


@implicit_query
def bcr(code: Union[str, Sequence[str]]) -> Query:
    pass


@implicit_query
def bbox(min_long: float=-180.0, min_lat: float=-90.0, max_long: float=180.0, max_lat: float=90.0) -> Query:
    pass


@implicit_query
def date() -> Query:
    raise NotImplementedError


@implicit_query
def last_edited() -> Query:
    raise NotImplementedError


@implicit_query
def protocol() -> Query:
    raise NotImplementedError


@implicit_query
def project() -> Query:
    raise NotImplementedError


@implicit_query
def time() -> Query:
    raise NotImplementedError


@implicit_query
def duration() -> Query:
    raise NotImplementedError


@implicit_query
def distance() -> Query:
    raise NotImplementedError


@implicit_query
def breeding() -> Query:
    raise NotImplementedError


@implicit_query
def complete() -> Query:
    raise NotImplementedError
