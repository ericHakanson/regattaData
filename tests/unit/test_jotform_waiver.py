
from __future__ import annotations

from datetime import date
import pytest

from regatta_etl.normalize import parse_date, split_signed_document_urls, normalize_email, normalize_phone, normalize_name

def test_parse_date():
    assert parse_date("Jul 23, 2025") == date(2025, 7, 23)
    assert parse_date("  Aug 1, 2024  ") == date(2024, 8, 1)
    assert parse_date("invalid date") is None
    assert parse_date(None) is None

def test_split_signed_document_urls():
    doc = """
    https://www.jotform.com/uploads/username/12345/signed/1.pdf
    https://www.jotform.com/uploads/username/12345/signed/2.pdf
    """
    expected = [
        "https://www.jotform.com/uploads/username/12345/signed/1.pdf",
        "https://www.jotform.com/uploads/username/12345/signed/2.pdf",
    ]
    assert split_signed_document_urls(doc) == expected
    assert split_signed_document_urls("  ") == []
    assert split_signed_document_urls(None) == []

def test_normalize_email():
    assert normalize_email("  TEST@EXAMPLE.COM  ") == "test@example.com"
    assert normalize_email(None) is None

def test_normalize_phone():
    assert normalize_phone("123-456-7890") == "+11234567890"
    assert normalize_phone("  (123) 456-7890  ") == "+11234567890"
    assert normalize_phone("11234567890") == "+11234567890"
    assert normalize_phone(None) is None
    assert normalize_phone("123") is None

def test_normalize_name():
    assert normalize_name("  John Doe  ") == "john doe"
    assert normalize_name("O'Malley, John") == "omalley john"
    assert normalize_name(None) is None
