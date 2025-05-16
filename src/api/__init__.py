# File: src/api/__init__.py

# -*- coding: utf-8 -*-

"""
API module for the Comtrade Data Pipeline.
Contains classes and functions for interacting with the UN Comtrade API.
"""

from .client import ComtradeAPIClient

__all__ = ['ComtradeAPIClient']