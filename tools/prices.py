"""Object storage's list prices, kept in one place: what `kafka_bench.py --steady` and `soak.py`
price a lake's requests and bytes at."""

LABEL = ("list prices on 2026-10-13: S3 Standard in us-east-1 (aws.amazon.com/s3/pricing), "
         "R2 Standard (developers.cloudflare.com/r2/pricing)")

# Dollars per million requests of each class a bucket bills (DELETE is free on both), and per GB-month kept.
PRICES = {"s3": {"write": 5.00, "list": 5.00, "read": 0.40, "delete": 0, "gb_month": 0.023},
          "r2": {"write": 4.50, "list": 4.50, "read": 0.36, "delete": 0, "gb_month": 0.015}}
OPS, MONTH = ("write", "list", "read", "delete"), 30 * 86400


def dollars_a_month(rates_per_s, gb, where):
    """What `rates_per_s` requests a second of each class, and `gb` GB kept, cost a month at `where`'s prices."""
    p = PRICES[where]
    return sum(rates_per_s.get(op, 0) * p[op] for op in OPS) * MONTH / 1e6 + gb * p["gb_month"]
