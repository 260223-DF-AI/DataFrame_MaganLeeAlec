## pytesting for validations.py
import pytest
from CafeSalesAnalysis import validations

# 1. test required fields
def test_requiredfields_valid_value():
    assert validations.requiredfields("valid_value") == True

def test_requiredfields_invalid_value():
    assert validations.requiredfields("") == False
    assert validations.requiredfields(None) == False
    assert validations.requiredfields("   ") == False

# 2. test numeric validation/range validation/decimals
def test_price_valid():
    assert validations.price(25.5) == True
    assert validations.price(0) == True
    assert validations.price(100) == True

def test_price_invalid():
    assert validations.price(-1) == False
    assert validations.price(-1.01) == False
    assert validations.price("twenty") == False

# 4. test date validation
# year, month, day
def test_date_valid():
    assert validations.date("2023-01-01") == True
    assert validations.date("2023-12-31") == True

def test_date_invalid():
    assert validations.date("2023/13/01") == False
    assert validations.date("2023-00-01") == False
    assert validations.date("2023-01-32") == False
    assert validations.date("2023-02-30") == False
    assert validations.date("not-a-date") == False
    assert validations.date("") == False

# 5. test categorical values
def test_paymenttype_valid():
    assert validations.paymenttype("Cash") == True
    assert validations.paymenttype("Credit Card") == True
    assert validations.paymenttype("Digital Wallet") == True

def test_paymenttype_invalid():
    assert validations.paymenttype("Check") == False
    assert validations.paymenttype("Barter") == False
    assert validations.paymenttype("") == False

# 8. test full record values, if a set of a record is valid
def test_record_valid():
    record = {
        "date": "2023-01-01",
        "item": "Coffee",
        "price": 3.5,
        "payment_type": "Cash"
    }
    assert validations.record(record) == True

def test_record_invalid():
    record = {
        "date": "2023-01-01",
        "item": "",
        "price": 3.5,
        "payment_type": "Cash"
    }
    assert validations.record(record) == False

    record = {
        "date": "2023-01-01",
        "item": "Coffee",
        "price": -1,
        "payment_type": "Cash"
    }
    assert validations.record(record) == False

    record = {
        "date": "2023-01-01",
        "item": "Coffee",
        "price": 3.5,
        "payment_type": "Check"
    }
    assert validations.record(record) == False

# 9. test error messages for bad input types
def test_errors():
    record = {
        "item": "",
        "price": -1,
        "payment_type": "Check"
    }
    is_valid, errors = validations.record(record)
    assert is_valid == False
    assert "item cannot be blank" in errors
    assert "price must be a non-negative number" in errors
    assert "payment type must be cash, creedit card, or digital wallet" in errors   
    
# 10. test unknown fields
def test_unknown_fields():
    record = {
        "date": "2023-01-01",
        "item": "UNKNOWN",
        "price": 3.5,
        "payment_type": "Cash",
    }
    assert validations.record(record) == True