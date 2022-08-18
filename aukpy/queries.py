import datetime
from functools import reduce
from aukpy import db
import pandas as pd
import sqlite3
from dataclasses import dataclass, field, replace as dc_replace
from typing import (
    Iterable,
    List,
    Optional,
    Union,
    Tuple,
    Any,
    Literal,
)


Distance = Literal["km", "miles"]


def check_simple_type(value) -> bool:
    return (
        isinstance(value, str)
        or isinstance(value, float)
        or isinstance(value, int)
        or isinstance(value, bool)
    )


class Filter:
    def __and__(self, other: "Filter") -> "Filter":
        if isinstance(self, Empty):
            return other
        elif isinstance(other, Empty):
            return self
        else:
            return AndFilter(self, other)

    def __or__(self, other: "Filter") -> "Filter":
        if isinstance(self, Empty):
            return other
        elif isinstance(other, Empty):
            return self
        else:
            return OrFilter(self, other)

    def query(self) -> Tuple[str, Tuple[Any, ...]]:
        raise NotImplementedError


class Empty(Filter):
    def query(self) -> Tuple[str, Tuple[Any, ...]]:
        return "", ()

    def __or__(self, other: Filter) -> Filter:
        return other


@dataclass
class AndFilter(Filter):
    filter_1: Filter
    filter_2: Filter

    def query(self) -> Tuple[str, Tuple[Any, ...]]:
        f1, v1 = self.filter_1.query()
        f2, v2 = self.filter_2.query()
        vals = v1 + v2
        return f"{f1} AND {f2}", vals


@dataclass
class OrFilter(Filter):
    filter_1: Filter
    filter_2: Filter

    def query(self) -> Tuple[str, Tuple[Any, ...]]:
        f1, v1 = self.filter_1.query()
        f2, v2 = self.filter_2.query()
        vals = v1 + v2
        return f"{f1} OR {f2}", vals


@dataclass
class ColumnFilter(Filter):
    column: str

    def query(self) -> Tuple[str, Tuple[Any, ...]]:
        raise NotImplementedError


@dataclass
class IsIn(ColumnFilter):
    values: Tuple[Any, ...]

    def query(self) -> Tuple[str, Tuple[Any, ...]]:
        quotes = ",".join(("?" for _ in self.values))
        return f"{self.column} in ({quotes})", self.values


@dataclass
class Is(ColumnFilter):
    value: Any

    def query(self) -> Tuple[str, Tuple[Any, ...]]:
        return f"{self.column} = ?", (self.value,)


@dataclass
class EqualsOrIn(ColumnFilter):
    value: Union[str, int, float, bool, Iterable[Any]]

    def query(self) -> Tuple[str, Tuple[Any, ...]]:
        if check_simple_type(self.value):
            return f"{self.column} = ?", (self.value,)
        else:
            return IsIn(self.column, tuple(self.value)).query()  # type: ignore


@dataclass
class LT(ColumnFilter):
    value: Union[float, int]

    def query(self) -> Tuple[str, Tuple[Any, ...]]:
        return f"{self.column} < ?", (self.value,)


@dataclass
class GT(ColumnFilter):
    value: Union[float, int]

    def query(self) -> Tuple[str, Tuple[Any, ...]]:
        return f"{self.column} > ?", (self.value,)


@dataclass
class LE(ColumnFilter):
    value: Union[float, int]

    def query(self) -> Tuple[str, Tuple[Any, ...]]:
        return f"{self.column} <= ?", (self.value,)


@dataclass
class GE(ColumnFilter):
    value: Union[float, int]

    def query(self) -> Tuple[str, Tuple[Any, ...]]:
        return f"{self.column} >= ?", (self.value,)


@dataclass
class Between(ColumnFilter):
    lower: Union[float, int]
    upper: Union[float, int]

    def query(self) -> Tuple[str, Tuple[Any, ...]]:
        return f"{self.column} BETWEEN ? AND ?", (self.lower, self.upper)


@dataclass
class IsTrue(ColumnFilter):
    def query(self) -> Tuple[str, Tuple[Any, ...]]:
        return f"{self.column} = 1", ()


@dataclass
class Wrapped(Filter):
    inner: Filter

    def query(self) -> Tuple[str, Tuple[Any, ...]]:
        sub_q, sub_v = self.inner.query()
        return f"({sub_q})", sub_v


@dataclass
class Query:
    """A wrapper around a set of filters for each table"""

    row_filters: List[Filter] = field(default_factory=list)
    row_filter: Optional[Filter] = None

    def _update_filter(self, new_filt: Filter):
        new_filters = self.row_filters + [new_filt]
        return dc_replace(self, row_filters=new_filters)

    def species(self, names: Union[str, Iterable[str]]) -> "Query":
        """Filter by species name.
        Can be any of scientific name, common name, subspecies scientific name, or subspecies common name.

        Args:
            names: A name or names.
        """
        if isinstance(names, str):
            names_param: Tuple[str, ...] = (names,)
        else:
            names_param = tuple(names)
        scientific_filt = EqualsOrIn("species.scientific_name", names_param)
        common_filt = EqualsOrIn("species.common_name", names_param)
        sub_science = EqualsOrIn("species.subspecies_scientific_name", names_param)
        sub_common = EqualsOrIn("species.subspecies_common_name", names_param)
        new_filt = scientific_filt | common_filt | sub_science | sub_common
        return self._update_filter(new_filt)

    def country(
        self, names: Union[str, Iterable[str]], replace: bool = True
    ) -> "Query":
        """Filter by country name or country code."""
        if isinstance(names, str):
            names_param: Tuple[str, ...] = (names,)
        else:
            names_param = tuple(names)
        new_filt = EqualsOrIn("location_data.country", names_param) | EqualsOrIn(
            "location_data.country_code", names
        )

        # Get rid of old country filters if replace is True
        if replace is True:
            new_filters = [
                x for x in self.row_filters if not getattr(x, "column", False)
            ]
            new_obj = dc_replace(self, row_filters=new_filters)
            return new_obj._update_filter(new_filt)
        else:
            return self._update_filter(new_filt)

    def state(self, names: Union[str, Iterable[str]]) -> "Query":
        """Filter by state name or state code."""
        new_filt = EqualsOrIn("location_data.state_name", names) | EqualsOrIn(
            "location_data.state_code", names
        )
        return self._update_filter(new_filt)

    def bcr(self, code: Union[str, Iterable[str]]) -> "Query":
        """Filter by BCR code"""
        return self._update_filter(EqualsOrIn("bcrcode.bcr_code", code))

    def bbox(
        self,
        min_long: float = -180.0,
        min_lat: float = -90.0,
        max_long: float = 180.0,
        max_lat: float = 90.0,
    ) -> "Query":
        """Filter for observations within a bounding box."""
        new_filt = (
            GT("longitude", min_long)
            & LT("longitude", max_long)
            & GT("latitude", min_lat)
            & LT("latitude", max_lat)
        )
        return self._update_filter(new_filt)

    def _wildcard_date(self, after: Optional[str], before: Optional[str]) -> "Query":
        if after is None:
            after = "*-01-01"
        if before is None:
            before = "*-12-31"
        current = datetime.datetime.now().year
        after = after.replace("*", str(current))
        before = before.replace("*", str(current))

        after_dt = pd.to_datetime(after)
        before_dt = pd.to_datetime(before) + pd.DateOffset(days=1)
        offset = pd.DateOffset(years=1)

        f: Filter = Empty()

        # 1800 is the start of observations
        while after_dt.year >= 1800:
            new_f = GE("observation_date", int(after_dt.timestamp())) & LT(
                "observation_date", int(before_dt.timestamp())
            )

            f = f | new_f
            after_dt -= offset
            before_dt -= offset

        return self._update_filter(f)

    def date(
        self, after: Optional[str] = None, before: Optional[str] = None
    ) -> "Query":
        """Filter for a specific set of dates
        At least one of `after` or `before` must not be None
        Args:
            after: The start of the date range. Should be in year-month-day format. Defaults to None (no minimum date).
            before: The end of the date range. Should be in year-month-day format. Defaults to None (no maximum date).
        """
        assert not (after is None and before is None)
        # Convert to integers
        if after is not None:

            if after[0] == "*":
                return self._wildcard_date(after, before)
            after_seconds = int(pd.to_datetime(after, format="%Y-%m-%d").timestamp())
            after_filter: Filter = GE("observation_date", after_seconds)
        else:
            after_filter = Empty()
        if before is not None:
            if before[0] == "*":
                return self._wildcard_date(after, before)
            before_seconds = int(pd.to_datetime(before, format="%Y-%m-%d").timestamp())
            before_filter: Filter = LE("observation_date", before_seconds)
        else:
            before_filter = Empty()

        return self._update_filter(after_filter & before_filter)

    def last_edited(
        self, after: Optional[str] = None, before: Optional[str] = None
    ) -> "Query":
        """Filter for edits between a range of dates.
        At least one of `after` or `before` must not be None
        Args:
            after: The start of the date range. Should be in year-month-day format. Defaults to None (no minimum date).
            before: The end of the date range. Should be in year-month-day format. Defaults to None (no maximum date).
        """
        assert not (after is None and before is None)
        # Convert to integers
        if after is not None:
            after_seconds = int(pd.to_datetime(after, format="%Y-%m-%d").timestamp())
            after_filter: Filter = GT("last_edited_date", after_seconds)
        else:
            after_filter = Empty()
        if before is not None:
            before_seconds = int(pd.to_datetime(before, format="%Y-%m-%d").timestamp())
            before_filter: Filter = LT("last_edited_date", before_seconds)
        else:
            before_filter = Empty()

        return self._update_filter(after_filter & before_filter)

    def protocol(self, protocol: Union[str, Iterable[str]]) -> "Query":
        """Filter for observations obtained using a particular protocol.
        The most common protocols are stationary, traveling, and casual.

        Args:
            protocol: The protocol or protocols to filter for
        """
        return self._update_filter(EqualsOrIn("protocol_type", protocol))

    def project(self, project: Union[str, Iterable[str]]) -> "Query":
        """Filter for observations that are part of a particular project.

        Args:
            project: The name of the project
        """
        return self._update_filter(EqualsOrIn("project_code", project))

    def time(self, after: str = "00:00", before: str = "23:59") -> "Query":
        """Select observations started between the given hours.

        Args:
            after: The minimum start time of the observation event. Should be a string in 24 hour format.
            before: The maximum start time of the observation event. Should be a string in 24 hour format.

        Returns:
            Query: A query object that will select the relevant observations.
        """
        after_ts = pd.to_datetime(after)
        before_ts = pd.to_datetime(before)
        after_delta = (
            datetime.datetime.combine(datetime.date.min, after_ts.time())
            - datetime.datetime.min
        )
        before_delta = (
            datetime.datetime.combine(datetime.date.min, before_ts.time())
            - datetime.datetime.min
        )

        after_s = int(after_delta.total_seconds())
        before_s = int(before_delta.total_seconds())

        f = Between("time_observations_started", after_s, before_s)
        return self._update_filter(f)

    def duration(self, minimum: float = 0, maximum: Optional[float] = None) -> "Query":
        """Filter on the duration of the observation period, in minutes.
        Args:
            minimum: The minimum duration time.
            maximum: The maximum duration time. Defaults to no upper bound.
        """
        new_filt: Filter = GT("duration_minutes", minimum)
        new_obj = self._update_filter(new_filt)
        if maximum is not None:
            return new_obj._update_filter(LT("duration_minutes", maximum))
        else:
            return self._update_filter(new_filt)

    def distance(
        self, minimum: float = 0.0, maximum: float = 1e9, unit: Distance = "km"
    ) -> "Query":
        """Filter on the distance traveled during the observation period.

        Args:
            minimum: The minimum distance to accept. If 0, 'stationary' observations are also accepted.
            maximum: The maximum distance to accept.
            unit: The unit of distance, either 'km' or 'miles'. Defaults to 'km'
        """
        MILES_TO_KM = 1.60934
        if unit == "miles":
            minimum *= MILES_TO_KM
            maximum *= MILES_TO_KM

        if minimum == 0.0:
            filt = LT("effort_distance_km", maximum) | Is("protocol_code", "stationary")
            return self._update_filter(filt)
        else:
            filt = Between("effort_distance_km", minimum, maximum)
            return self._update_filter(filt)

    def breeding(
        self,
        breeding_code: Union[str, Iterable[str]],
    ) -> "Query":
        """Filter observations on breeding code.

        Args:
            breeding_code: A breeding code or codes to filter on
        """
        if isinstance(breeding_code, str):
            return self._update_filter(Is("breeding_code", breeding_code))
        else:
            return self._update_filter(IsIn("breeding_code", tuple(breeding_code)))

    def complete(self) -> "Query":
        return self._update_filter(IsTrue("complete"))

    def get_query(self) -> Tuple[str, Tuple[Any, ...]]:
        if len(self.row_filters) > 0:
            single_filter = reduce(lambda a, b: a & b, self.row_filters)
            q_filter, vals = single_filter.query()
            where = f"WHERE {q_filter}"
        else:
            where = ""
            vals = ()
        query = f"""SELECT {', '.join(db.DF_COLUMNS)} FROM
        observation
        LEFT JOIN sampling_event   ON sampling_event_id = sampling_event.id
        LEFT JOIN species          ON species_id = species.id
        LEFT JOIN location_data    ON location_data_id = location_data.id
        LEFT JOIN breeding         ON breeding_id = breeding.id
        LEFT JOIN protocol         ON protocol_id = protocol.id
        {where}"""
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
        new_query = Query()
        method = getattr(new_query, name)
        return method(*args, **kwargs)

    return wrapper


def no_filter():
    q = Query()
    return q


@implicit_query
def species(names: Union[str, Iterable[str]]):
    pass


@implicit_query
def country(names: Union[str, Iterable[str]]) -> Query:
    pass


@implicit_query
def state(names: Union[str, Iterable[str]]) -> Query:
    pass


@implicit_query
def bcr(code: Union[str, Iterable[str]]) -> Query:
    pass


@implicit_query
def bbox(
    min_long: float = -180.0,
    min_lat: float = -90.0,
    max_long: float = 180.0,
    max_lat: float = 90.0,
) -> Query:
    pass


@implicit_query
def date(after: Optional[str] = None, before: Optional[str] = None) -> Query:
    pass


@implicit_query
def last_edited() -> Query:
    pass


@implicit_query
def protocol(protocol: Union[str, Iterable[str]]) -> Query:
    pass


@implicit_query
def project(project: Union[str, Iterable[str]]) -> Query:
    pass


@implicit_query
def time(after: str = "00:00", before: str = "23:59") -> Query:
    pass


@implicit_query
def duration(minimum: float = 0, maximum: Optional[float] = None) -> Query:
    pass


@implicit_query
def distance(
    minimum: float = 0.0, maximum: float = 1e9, unit: Distance = "km"
) -> Query:
    pass


@implicit_query
def breeding(breeding_code: Union[str, Iterable[str]]) -> Query:
    pass


@implicit_query
def complete() -> Query:
    raise NotImplementedError
