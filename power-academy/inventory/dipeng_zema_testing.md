# dipeng/ZEMA testing

- id: `dipeng_zema_testing` · class: practice · type: folder
- topic: Programmatic access to the ZEMA energy data platform via SOAP API using Python · level: foundation · market: mixed · year: None
- worked examples: True · code: True

## Concepts
- **ZEMA API authentication** — Token-based authentication mechanism required to establish a session with the ZEMA data server.
- **SOAP web service** — Protocol used by the ZEMA API to expose energy data services and return structured responses.
- **Data source catalogue** — Enumeration of named market data providers available within the ZEMA database, such as EEX, National Grid, and Amprion.
- **Data report** — A named dataset within a data source representing a specific published series, such as power available capacity or generation.
- **Observations retrieval** — Fetching the measurable series identifiers (e.g. available capacity) associated with a specific data report.
- **Attribute values** — Metadata dimensions attached to a data report, including country, fuel source type, and creation timestamp, used to filter or slice time series.
- **Holiday calendar** — Named sets of market-specific non-trading days used to adjust or filter time series data for exchanges and jurisdictions globally.
- **Unit conversion** — API-supported mapping from a source energy unit (MWh) to a wide range of target units for normalising power and energy data.
- **Day-type filter** — Identifiers allowing time series to be restricted to specific day categories such as weekday, weekend, or named holiday sets.
- **Client profile** — User-level configuration objects retrievable from the ZEMA server that define saved queries or access settings.
- **Time-series date enumeration** — Sequential daily date labels used to index or select energy market data records within a data management platform.
- **Data platform UI parameter selection** — Structured label-value pair schema for defining selectable date parameters in an energy data retrieval interface.
- **SOAP web service response parsing** — Extraction of structured energy market data from XML SOAP envelope responses returned by a data platform API.
- **Analytic profile management** — Organisation of named user-defined analytic configurations covering power, gas, and weather data within a multi-user platform.
- **Data access control** — Classification of analytic profiles by access type (Public) and user ownership within a shared energy data environment.
- **Generation by fuel type reporting** — Retrieval and presentation of electricity generation data disaggregated by fuel source across multiple European markets.
- **Renewable energy forecast vs actual comparison** — Profile-level configuration for comparing forecast and observed solar and wind generation data at regional granularity.
- **Gas infrastructure operational data** — Profiles referencing pipeline and LNG terminal flow and operational metrics from gas transmission system operators.
- **ZEMA API authentication token** — Credential token obtained from the ZEMA server to authorise subsequent API calls.
- **Data source enumeration** — Retrieval of the full list of available energy data providers accessible through the ZEMA platform.
- **Data report retrieval** — Fetching the catalogue of data reports associated with a specific energy data source.
- **Observation retrieval** — Accessing individual observable fields or series available within a named data report.
- **Attribute value retrieval** — Querying the dimension values (e.g. country, fuel type) that filter records within a data report.
- **Holiday group catalogue** — A reference list of named calendar holiday schedules used to exclude non-trading days from data profiles.
- **Unit conversion lookup** — Retrieval of target unit options available when converting energy quantities from a specified source unit.
- **Analytic profile** — A saved user configuration object in ZEMA specifying data selection, analytic type, and calendar exclusion rules.
- **Day-of-week and holiday filter** — Predefined filter identifiers used to include or exclude specific weekdays or holiday calendars from time series.
- **SOAP web service interface** — The SOAPpy-based remote procedure call mechanism used to invoke ZEMA platform services from Python.

## Methods
- SOAP API calls via Python SOAPpy library
- Token-based session authentication
- XML response parsing
- Date-range enumeration
- XML/structured markup for UI parameter binding
- SOAP API invocation
- XML parsing
- Profile versioning
- Date attribute enumeration
- SOAP API calls via SOAPpy
- Token-based authentication
- User and profile administration queries
- Unit conversion enumeration

## Implied prerequisites
- Basic Python programming
- Understanding of web services and HTTP
- Familiarity with energy market data concepts
- Basic understanding of energy market data systems
- Familiarity with time-series data structures
- Basic understanding of XML and SOAP protocols
- Familiarity with energy market data sources (BMRS, EEX, RTE, Snam, Gassco, National Grid)
- Knowledge of power and gas market data types
- Python 2.7 programming
- SOAP/WSDL web services concepts
- Basic energy market data structures
- Understanding of calendar conventions in energy markets
