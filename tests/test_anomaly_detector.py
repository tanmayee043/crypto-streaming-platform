import pytest

from anomaly_detector import classify_anomaly, z_score


def test_z_score_of_constant_window_is_zero():
    assert z_score(5.0, [5.0, 5.0, 5.0]) == 0.0


def test_z_score_matches_hand_calculation():
    # mean 3, population stdev sqrt(2)
    assert z_score(5.0, [1.0, 2.0, 3.0, 4.0, 5.0]) == pytest.approx(2 / 2**0.5)


def test_z_score_is_negative_below_the_mean():
    assert z_score(1.0, [1.0, 2.0, 3.0]) < 0


@pytest.mark.parametrize(
    ("price_z", "vol_z", "expected"),
    [
        (0.0, 0.0, None),
        (3.0, 0.0, None),  # the threshold itself is not an anomaly
        (3.1, 0.0, "price"),
        (-3.1, 0.0, "price"),
        (0.0, 4.0, "volume"),
        (-5.0, 5.0, "both"),
    ],
)
def test_classify_anomaly(price_z, vol_z, expected):
    assert classify_anomaly(price_z, vol_z) == expected
