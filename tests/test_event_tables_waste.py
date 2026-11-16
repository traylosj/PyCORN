"""
Waste lines in the fraction table (`pycorn.event_tables.is_waste`): a waste line is a row, with its
flow, but not a fraction.
"""
import pytest

from pycorn.event_tables import is_waste


@pytest.mark.parametrize("name", ["Waste", "Waste(Frac)", "Out-Waste", "waste", "WASTE LINE 2"])
def test_default_rule_flags_names_that_contain_waste(name):
    assert is_waste(name)


@pytest.mark.parametrize("name", ["Frac", "3.A.1", "Out 1", "", None])
def test_default_rule_leaves_other_tubes(name):
    assert not is_waste(name)


def test_waste_and_waste_frac_are_two_tubes_and_both_waste_lines():
    # the flag does not merge them: they stay two names
    names = ["Waste", "Waste(Frac)"]
    assert [is_waste(n) for n in names] == [True, True] and names[0] != names[1]


def test_caller_names_replace_the_rule():
    # a skid that calls its waste line 'Drain': exact names only, any case, the default rule is off
    assert is_waste("drain ", ["Drain"])
    assert not is_waste("Waste", ["Drain"])
    assert not is_waste("Waste", [])
