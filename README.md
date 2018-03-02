# Architecture

The Geographic Storage, Transformation and Retrieval Engine (GSToRE) has been developed as a flexible, scalable data management, discovery and delivery platform that supports a combination of Open and Community standards for client access. It is built upon the principle of a services oriented architecture that provides a layer of abstraction between data and metadata management technologies and the client applications that consume the services published by the platform.

The platform can support both spatial and non-spatial data. Spatial services include OGC web services using MapServer, data retrieval and documentation. The documentation platform is designed to handle a variety of XML-based standards, with out-of-the-box support for CSDGM (FGDC) and ISO-19115/19139.

Framework: Pyramid and SQLAlchemy (Python)

Databases: PostgreSQL/PostGIS (canonical data store, spatial data processing), MongoDB (vector and tabular attribute storage), ElasticSearch (search engine)
