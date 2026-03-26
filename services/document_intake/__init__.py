# services/document_intake/__init__.py
"""
Settlement and compensation document intake service.

This module provides the foundation for ingesting raw settlement PDFs,
compensation files, and similar non-tabular documents into the platform.

Pattern:
- Raw originals stored in S3 landing zone
- Metadata registered in Postgres (raw_data.file_registry)
- Parsed content tracked in raw_data.file_manifest
- Business-level view in core.document_registry

Future parsing modules will be added under parsers/ subdirectory.
"""
